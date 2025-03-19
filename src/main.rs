use std::{
    cmp::max,
    fs::OpenOptions,
    future::Future,
    io::stdout,
    os::unix::fs::OpenOptionsExt as _,
    path::{Path, PathBuf},
    process::exit,
};

use async_channel::{bounded, Sender};
use executor::block_on;
use fs_util::get_ino;
use mimalloc::MiMalloc;
use rustix::fs::OFlags;

mod actor;
mod btrfs;
mod executor;
mod fs_util;
mod global;
mod scale;
mod taskpak;
mod walkdir;

use actor::{Actor, Runnable as _};
use btrfs::{ExtentInfo, Sv2Args};
use global::{config, get_err, set_err};
use scale::{CompsizeStat, ExtentMap};
use taskpak::TaskPak;
use walkdir::{FileConsumer, WalkDir};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) {
    executor::spawn(future).detach();
}

fn main() {
    let collector = Collector::new();
    let (s, r) = bounded(32);
    collector.start(&s, &config().args);
    drop(s);
    block_on(collector.run(r));
    if get_err().is_err() {
        exit(1)
    }
}

struct Worker {
    collector: TaskPak<ExtentInfo, CollectorMsg>,
    nfile: u64,
    sv2_args: Sv2Args,
}
impl Worker {
    fn new(collector: Sender<CollectorMsg>) -> Self {
        Self {
            collector: TaskPak::new(collector),
            nfile: 0,
            sv2_args: Sv2Args::new(),
        }
    }
}
impl Actor for Worker {
    type Message = Box<[Box<Path>]>;
    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        fn inner<'s>(
            sv2_args: &'s mut Sv2Args,
            path: &Path,
            self_nfile: &mut u64,
        ) -> Result<btrfs::Sv2ItemIter<'s>, ()> {
            let file = match OpenOptions::new()
                .read(true)
                .write(false)
                .custom_flags((OFlags::NOFOLLOW | OFlags::NOCTTY | OFlags::NONBLOCK).bits() as _)
                .open(path)
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("{}: {}", path.display(), e);
                    return Err(());
                }
            };
            let ino = get_ino(&file);
            *self_nfile += 1;
            Ok(sv2_args.search_file(file.into(), ino))
        }

        for path in msg {
            get_err()?;
            let Ok(iter) = inner(&mut self.sv2_args, &path, &mut self.nfile) else {
                continue;
            };
            for extent in iter {
                let extent = match extent {
                    Ok(extent) => extent,
                    Err(e) => {
                        set_err()?;
                        if e.raw_os_error() == 25 {
                            eprintln!("{}: Not btrfs (or SEARCH_V2 unsupported)", path.display());
                        } else {
                            eprintln!("{}: SEARCH_V2: {}", path.display(), e);
                        }
                        break;
                    }
                };
                match extent.parse() {
                    Ok(Some(extent)) => {
                        self.collector.push(extent).await;
                    }
                    Err(e) => {
                        set_err()?;
                        eprintln!("{}", e);
                        break;
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let sender = self.collector.sender().clone();
        let nfile = self.nfile;
        spawn(async move {
            sender.send(nfile.into()).await.ok();
        });
    }
}

struct Collector {
    extent_map: ExtentMap,
    stat: CompsizeStat,
}

impl Collector {
    fn new() -> Self {
        Self {
            extent_map: ExtentMap::default(),
            stat: CompsizeStat::default(),
        }
    }
    fn start(
        &self,
        sender: &Sender<CollectorMsg>,
        paths: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) {
        let nworkers = max(config().jobs - 1, 1);
        let (worker_tx, worker_rx) = bounded(nworkers as _);
        for _ in 0..nworkers {
            let worker = Worker::new(sender.clone());
            spawn(worker.run(worker_rx.clone()));
        }
        struct F(TaskPak<Box<Path>, <Worker as Actor>::Message>);
        impl FileConsumer for F {
            fn consume(
                &mut self,
                path: Box<Path>,
            ) -> impl std::future::Future<Output = ()> + std::marker::Send {
                async {
                    self.0.push(path).await;
                }
            }
        }
        let fcb = move || F(TaskPak::new(worker_tx.clone()));
        let mut walkdir = WalkDir::new(fcb, paths, nworkers).unwrap();
        let (walkdir_tx, walkdir_rx) = bounded(nworkers as _);
        walkdir.spawn_walkers(&walkdir_tx);
        spawn(walkdir.run(walkdir_rx));
    }
}

impl Actor for Collector {
    type Message = CollectorMsg;

    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        match msg {
            CollectorMsg::Extent(extents) => {
                for extent in extents {
                    self.stat.insert(&mut self.extent_map, extent);
                }
            }
            CollectorMsg::NFile(n) => *self.stat.nfile_mut() += n,
        }
        Ok(())
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        if get_err().is_ok() {
            self.stat.fmt(stdout(), config().scale()).unwrap();
        }
    }
}

enum CollectorMsg {
    Extent(Box<[ExtentInfo]>),
    NFile(u64),
}

impl From<u64> for CollectorMsg {
    fn from(value: u64) -> Self {
        Self::NFile(value)
    }
}
impl From<Box<[ExtentInfo]>> for CollectorMsg {
    fn from(value: Box<[ExtentInfo]>) -> Self {
        Self::Extent(value)
    }
}
