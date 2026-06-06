use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    io,
    marker::Send,
    os::fd::OwnedFd,
    path::{Path, PathBuf},
    sync::Arc,
};

use kanal::{AsyncSender as Sender, bounded_async as bounded};
use nohash::BuildNoHashHasher;
use rustix::{
    fs::{Dir, Mode, OFlags, open},
    path::Arg,
};

use crate::{
    actor::{Actor, Runnable as _, Sink},
    fs_util::{DevId, File_, get_dev},
    global::{config, get_err},
    spawn,
};

const MAX_LOCAL_LEN: usize = 4096 / size_of::<Box<Path>>();

pub struct JobChunk {
    dev: DevId,
    wq: SubvolWQ,
}

impl JobChunk {
    fn from_path(path: impl Into<Box<Path>>) -> Result<Self, io::Error> {
        let path: Box<Path> = path.into();
        let dev = get_dev(&path);
        let fd = open(
            path.as_ref(),
            OFlags::DIRECTORY | OFlags::NOFOLLOW,
            Mode::RUSR,
        )?;
        Ok(Self {
            dev,
            wq: SubvolWQ {
                fd: Arc::new(fd),
                paths: vec![path],
            },
        })
    }
}

struct SubvolWQ {
    paths: Vec<Box<Path>>,
    fd: Arc<OwnedFd>,
}
impl SubvolWQ {
    fn split(&mut self, n: usize) -> Self {
        let paths = self.paths.drain(0..n).collect();
        let fd = self.fd.clone();
        Self { paths, fd }
    }
}
struct JobMgr {
    jobs: HashMap<DevId, SubvolWQ, BuildNoHashHasher<u64>>,
}

impl JobMgr {
    fn new() -> Self {
        Self {
            jobs: HashMap::with_hasher(BuildNoHashHasher::default()),
        }
    }
    fn push(&mut self, mut job_chunk: JobChunk) {
        match self.jobs.entry(job_chunk.dev) {
            Entry::Occupied(mut o) => o.get_mut().paths.append(&mut job_chunk.wq.paths),
            Entry::Vacant(v) => {
                v.insert(job_chunk.wq);
            }
        }
    }
    // will return at most n jobs, maybe fewer
    fn get_n_jobs(&mut self, n: usize) -> Option<JobChunk> {
        let dev = *self.jobs.keys().next()?;
        let Entry::Occupied(mut entry) = self.jobs.entry(dev) else {
            unreachable!("key is from keys() which is non-empty")
        };
        let wq = if entry.get().paths.len() <= n {
            entry.remove()
        } else {
            entry.get_mut().split(n)
        };
        Some(JobChunk { dev, wq })
    }

    fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    fn clear(&mut self) {
        self.jobs.clear()
    }
}

type WalkerId = u8;
pub struct WalkDir {
    walkers: Box<[Sender<JobChunk>]>,
    pending_walkers: Vec<WalkerId>,
    global_joblist: JobMgr,
}

impl WalkDir {
    pub fn spawn<F, FC>(
        mut file_consumer: F,
        paths: impl IntoIterator<Item = impl Into<PathBuf>>,
        nwalker: u8,
    ) where
        F: FnMut() -> FC + Send + 'static,
        FC: Sink<Item = File_> + Send + 'static,
    {
        assert_ne!(nwalker, 0);
        let mut files = vec![];
        let chunks = paths
            .into_iter()
            .map(|p| p.into().into_boxed_path())
            .filter_map(|p| {
                if p.is_dir() {
                    Some(p)
                } else {
                    files.push(p);
                    None
                }
            })
            .filter_map(|p| JobChunk::from_path(p).ok());
        let mut global_joblist = JobMgr::new();
        for chunk in chunks {
            global_joblist.push(chunk);
        }
        let mut cb = file_consumer();
        spawn(async move {
            for p in files {
                let Ok(f) = File_::from_path(p) else {
                    continue;
                };
                cb.consume(f).await;
            }
        });
        if global_joblist.is_empty() {
            return;
        }
        let (sender, rx) = bounded(64);
        let walkers = (0..nwalker)
            .map(|i| {
                let walker = Walker::new(i, sender.clone(), file_consumer());
                let (s, r) = bounded(0); // the walker must be waiting for jobs
                spawn(walker.run(r));
                s
            })
            .collect();
        drop(sender);
        let mut self_ = Self {
            pending_walkers: (0..nwalker).collect(),
            global_joblist,
            walkers,
        };
        spawn(async {
            self_.job_balance().await;
            self_.run(rx).await;
        });
    }

    fn cleanup(&mut self) {
        self.global_joblist.clear();
        self.pending_walkers = Default::default();
        self.walkers = Default::default();
    }

    async fn job_balance(&mut self) {
        // no global job pending, no need to balance
        if self.global_joblist.is_empty() {
            // no job pending and all walkers free
            // we have our jobs done
            // cleanup and exit
            if self.pending_walkers.len() == self.walkers.len() {
                self.cleanup();
            }
            return;
        }
        while !self.pending_walkers.is_empty() && !self.global_joblist.is_empty() {
            let chunk = self.global_joblist.get_n_jobs(MAX_LOCAL_LEN / 2).unwrap();
            let id = self.pending_walkers.pop().unwrap();
            let walker = &self.walkers[id as usize];
            walker.send(chunk).await.ok();
        }
    }
}

pub enum WalkDirMsg {
    PushJobs(JobChunk),
    RequireJobs(WalkerId),
}

impl Actor for WalkDir {
    type Message = WalkDirMsg;

    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        if get_err().is_ok() {
            match msg {
                WalkDirMsg::PushJobs(chunk) => {
                    self.global_joblist.push(chunk);
                }
                WalkDirMsg::RequireJobs(id) => {
                    self.pending_walkers.push(id);
                }
            }
            self.job_balance().await;
        } else {
            self.cleanup();
        }
        Ok(())
    }
}

struct Walker<F> {
    id: WalkerId,
    master: Sender<WalkDirMsg>,
    file_handler: F,
}
impl<F> Walker<F> {
    fn new(id: WalkerId, master: Sender<WalkDirMsg>, file_handler: F) -> Self {
        Self {
            id,
            master,
            file_handler,
        }
    }
}

impl<F> Actor for Walker<F>
where
    F: Sink<Item = File_> + Send,
{
    type Message = JobChunk;

    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        let JobChunk {
            dev,
            wq: SubvolWQ { paths, fd },
        } = msg;
        let mut dirs = VecDeque::from(paths);
        let mut newfs_dirs = Vec::new();
        while let Some(dir_path) = dirs.pop_back() {
            if get_err().is_err() {
                break;
            }
            let read_dir = match open(
                dir_path.as_ref(),
                OFlags::DIRECTORY | OFlags::NOFOLLOW,
                Mode::RUSR,
            )
            .and_then(Dir::new)
            {
                Ok(rd) => rd,
                Err(e) => {
                    eprintln!("{}: {}", dir_path.display(), e);
                    continue;
                }
            };

            for entry in read_dir {
                let entry = match entry {
                    Ok(e) => e,
                    Err(e) => {
                        eprintln!("{}: {}", dir_path.display(), e);
                        continue;
                    }
                };
                if entry.file_name() == c"." || entry.file_name() == c".." {
                    continue;
                }

                let file_type = entry.file_type();
                let path = dir_path
                    .join(entry.file_name().as_str().unwrap())
                    .into_boxed_path();

                if file_type.is_dir() {
                    let dir_dev = get_dev(&path);
                    if dir_dev == dev {
                        dirs.push_back(path);
                    } else if !config().one_fs {
                        let Ok(fd) = open(
                            path.as_ref(),
                            OFlags::DIRECTORY | OFlags::NOFOLLOW,
                            Mode::RUSR,
                        ) else {
                            continue;
                        };
                        newfs_dirs.push(JobChunk {
                            dev: dir_dev,
                            wq: SubvolWQ {
                                fd: Arc::new(fd),
                                paths: vec![path],
                            },
                        });
                    }
                } else if file_type.is_file() {
                    self.file_handler
                        .consume(File_::new(fd.clone(), path, entry.ino()))
                        .await;
                }
            }
            for chunk in newfs_dirs.drain(..) {
                self.master
                    .send(WalkDirMsg::PushJobs(chunk))
                    .await
                    .map_err(|_| ())?;
            }
            if dirs.len() > MAX_LOCAL_LEN {
                let r = dirs.len() - MAX_LOCAL_LEN / 2;
                let v: Vec<_> = dirs.drain(0..r).collect();
                self.master
                    .send(WalkDirMsg::PushJobs(JobChunk {
                        dev,
                        wq: SubvolWQ {
                            fd: fd.clone(),
                            paths: v,
                        },
                    }))
                    .await
                    .map_err(|_| ())?;
            }
        }
        self.master
            .send(WalkDirMsg::RequireJobs(self.id))
            .await
            .map_err(|_| ())?;
        Ok(())
    }
}
