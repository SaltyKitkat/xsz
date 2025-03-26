use std::{
    cmp::max,
    future::Future,
    io::stdout,
    path::{Path, PathBuf},
    process::exit,
};

use executor::block_on;
use kanal::{bounded_async as bounded, AsyncSender as Sender};
use mimalloc::MiMalloc;
use rustix::fs::{open, Mode, OFlags};

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
    type Message = Box<[(Box<Path>, u64)]>;
    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        fn inner<'s>(
            sv2_args: &'s mut Sv2Args,
            path: &Path,
            ino: u64,
            self_nfile: &mut u64,
        ) -> Result<btrfs::Sv2ItemIter<'s>, ()> {
            let file = match open(
                path,
                OFlags::NOFOLLOW | OFlags::NOCTTY | OFlags::NONBLOCK,
                Mode::RUSR,
            ) {
                Ok(fd) => fd,
                Err(errno) => {
                    eprintln!("{}: {}", path.display(), errno);
                    return Err(());
                }
            };
            *self_nfile += 1;
            Ok(sv2_args.search_file(file.into(), ino))
        }

        for (path, ino) in msg {
            get_err()?;
            let Ok(iter) = inner(&mut self.sv2_args, &path, ino, &mut self.nfile) else {
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
        let nworkers = max(config().jobs, 1);
        let (worker_tx, worker_rx) = bounded(nworkers as _);
        for _ in 0..nworkers {
            let worker = Worker::new(sender.clone());
            spawn(worker.run(worker_rx.clone()));
        }
        struct F(TaskPak<(Box<Path>, u64), <Worker as Actor>::Message>);
        impl FileConsumer for F {
            fn consume(
                &mut self,
                path: Box<Path>,
                ino: u64,
            ) -> impl std::future::Future<Output = ()> + std::marker::Send {
                async move {
                    self.0.push((path, ino)).await;
                }
            }
        }
        let fcb = move || F(TaskPak::new(worker_tx.clone()));
        WalkDir::spawn(fcb, paths, nworkers);
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
