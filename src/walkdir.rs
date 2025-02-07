use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    future::Future,
    io,
    marker::Send,
    os::linux::fs::MetadataExt as _,
    path::{Path, PathBuf},
};

use async_channel::{bounded, Sender};
use futures_lite::future::block_on;
use nohash::BuildNoHashHasher;

use crate::{actor::Runnable as _, global::get_one_fs, spawn, Actor};

const MAX_LOCAL_LEN: usize = 4096 / size_of::<Box<Path>>();

pub trait FileConsumer {
    fn consume(&mut self, path: Box<Path>) -> impl Future<Output = ()> + Send;
}

pub struct JobChunk {
    dev: u64,
    dirs: Vec<Box<Path>>,
}

impl JobChunk {
    fn from_path(path: impl Into<Box<Path>>) -> Result<Self, io::Error> {
        let path: Box<Path> = path.into();
        let dev = path.symlink_metadata()?.st_dev();
        Ok(Self {
            dev,
            dirs: vec![path.into()],
        })
    }
}

struct JobMgr {
    jobs: HashMap<u64, Vec<Box<Path>>, BuildNoHashHasher<u64>>,
}
impl JobMgr {
    fn new() -> Self {
        Self {
            jobs: HashMap::with_hasher(BuildNoHashHasher::default()),
        }
    }
    fn push(&mut self, mut job_chunk: JobChunk) {
        match self.jobs.entry(job_chunk.dev) {
            Entry::Occupied(mut o) => o.get_mut().append(&mut job_chunk.dirs),
            Entry::Vacant(v) => {
                v.insert(job_chunk.dirs);
            }
        }
    }
    // will return at most n jobs, maybe fewer
    fn get_n_jobs(&mut self, n: usize) -> Option<JobChunk> {
        let chunk = self.jobs.iter_mut().next()?;
        let key = *chunk.0;
        let jobs = if chunk.1.len() <= n {
            self.jobs.remove(&key).unwrap()
        } else {
            chunk.1.drain(0..n).collect()
        };
        Some(JobChunk {
            dev: key,
            dirs: jobs,
        })
    }

    fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}

pub struct WalkDir<F> {
    nwalker: usize,
    file_consumer: F,
    pending_walkers: Vec<Sender<WalkerMsg>>,
    global_joblist: JobMgr,
}

impl<F, FC> WalkDir<F>
where
    F: FnMut() -> FC + Send + 'static,
    FC: FileConsumer + Send + 'static,
{
    pub fn new(
        mut file_consumer: F,
        path: impl IntoIterator<Item = impl Into<PathBuf>>,
        nwalker: usize,
    ) -> Result<Self, io::Error> {
        assert_ne!(nwalker, 0);
        let mut files = vec![];
        let chunks = path
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
                cb.consume(p).await;
            }
        });

        Ok(Self {
            nwalker,
            file_consumer,
            pending_walkers: vec![],
            global_joblist,
        })
    }

    pub fn spawn_walkers(&mut self, sender: &Sender<WalkDirMsg>) {
        for _ in 0..self.nwalker {
            let walker = Walker::new(sender.clone(), (self.file_consumer)());
            let (s, r) = bounded(1); // only 1 msg in channel is possible
            spawn(walker.run(r));
            self.pending_walkers.push(s);
        }
        block_on(self.job_balance());
    }

    async fn job_balance(&mut self) {
        while !self.global_joblist.is_empty() {
            if let Some(addr) = self.pending_walkers.pop() {
                let chunk = self.global_joblist.get_n_jobs(MAX_LOCAL_LEN / 2).unwrap();
                let addr2 = addr.clone();
                addr2.send(WalkerMsg { chunk, addr }).await.ok();
            } else {
                break;
            }
        }
        if self.pending_walkers.len() == self.nwalker {
            self.pending_walkers.clear();
        }
    }
}

pub enum WalkDirMsg {
    PushJobs(JobChunk),
    RequireJobs(Sender<WalkerMsg>),
}

impl<F, FC> Actor for WalkDir<F>
where
    F: FnMut() -> FC + Send + 'static,
    FC: FileConsumer + Send + 'static,
{
    type Message = WalkDirMsg;

    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        match msg {
            WalkDirMsg::PushJobs(chunk) => {
                self.global_joblist.push(chunk);
            }
            WalkDirMsg::RequireJobs(addr) => {
                self.pending_walkers.push(addr);
            }
        }
        self.job_balance().await;
        Ok(())
    }
}

struct Walker<F> {
    master: Sender<WalkDirMsg>,
    file_handler: F,
}
impl<F> Walker<F> {
    fn new(addr: Sender<WalkDirMsg>, file_handler: F) -> Self {
        Self {
            master: addr,
            file_handler,
        }
    }
}

pub struct WalkerMsg {
    chunk: JobChunk,
    addr: Sender<Self>,
}

impl<F> Actor for Walker<F>
where
    F: FileConsumer + Send,
{
    type Message = WalkerMsg;

    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        let WalkerMsg {
            chunk: JobChunk { dev, dirs },
            addr,
        } = msg;
        let mut dirs = VecDeque::from(dirs);
        while let Some(dir) = dirs.pop_back() {
            for entry in dir.read_dir().unwrap() {
                let entry = entry.unwrap();
                if get_one_fs() && entry.metadata().unwrap().st_dev() != dev {
                    continue;
                }
                let fty = entry.file_type().unwrap();
                let path = entry.path().into_boxed_path();
                if fty.is_dir() {
                    dirs.push_back(path);
                } else if fty.is_file() {
                    self.file_handler.consume(path).await;
                }
            }
            if dirs.len() > MAX_LOCAL_LEN {
                let r = dirs.len() - MAX_LOCAL_LEN / 2;
                let v: Vec<_> = dirs.drain(0..r).collect();
                self.master
                    .send(WalkDirMsg::PushJobs(JobChunk { dev, dirs: v }))
                    .await
                    .map_err(|_| ())?;
            }
        }
        self.master
            .send(WalkDirMsg::RequireJobs(addr))
            .await
            .map_err(|_| ())?;
        Ok(())
    }
}
