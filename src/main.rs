use std::{
    fs::OpenOptions,
    future::Future,
    io::stdout,
    os::unix::fs::OpenOptionsExt as _,
    path::{Path, PathBuf},
    process::exit,
};

use async_channel::{bounded, Sender};
use fs_util::get_ino;
use futures_lite::future::block_on;
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
use scale::{CompsizeStat, ExtentMap, Scale};
use taskpak::TaskPak;
use walkdir::{FileConsumer, WalkDir};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) {
    executor::spawn(future).detach();
}

fn main() {
    let cfg = config();
    let scale = if cfg.bytes {
        Scale::Bytes
    } else {
        Scale::Human
    };
    let collector = Collector::new(scale);
    let (s, r) = bounded(32);
    collector.start(&s, &cfg.args);
    drop(s);
    block_on(collector.run(r));
    if get_err().is_err() {
        exit(1)
    }
}

struct Worker {
    sv2_args: Sv2Args,
    collector: TaskPak<ExtentInfo, CollectorMsg>,
    nfile: u64,
}
impl Worker {
    fn new(collector: Sender<CollectorMsg>) -> Self {
        Self {
            sv2_args: Sv2Args::new(),
            collector: TaskPak::new(collector),
            nfile: 0,
        }
    }
}
impl Actor for Worker {
    type Message = Box<[Box<Path>]>;
    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        fn inner<'s>(
            sv2_args: &'s mut Sv2Args,
            path: Box<Path>,
            self_nfile: &mut u64,
        ) -> Result<btrfs::Sv2ItemIter<'s>, ()> {
            let file = match OpenOptions::new()
                .read(true)
                .write(false)
                .custom_flags((OFlags::NOFOLLOW | OFlags::NOCTTY | OFlags::NONBLOCK).bits() as _)
                .open(&path)
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("{}: {}", path.display(), e);
                    return Err(());
                }
            };
            let ino = get_ino(&file);
            match sv2_args.search_file(file.into(), ino) {
                Ok(iter) => {
                    *self_nfile += 1;
                    Ok(iter)
                }
                Err(e) => {
                    set_err()?;
                    if e.raw_os_error() == 25 {
                        eprintln!("{}: Not btrfs (or SEARCH_V2 unsupported)", path.display());
                    } else {
                        eprintln!("{}: SEARCH_V2: {}", path.display(), e);
                    }
                    Err(())
                }
            }
        }

        for path in msg {
            get_err()?;
            let Ok(iter) = inner(&mut self.sv2_args, path, &mut self.nfile) else {
                continue;
            };
            for extent in iter {
                if let Ok(Some(extent)) = extent.parse() {
                    self.collector.push(extent).await;
                }
            }
        }
        Ok(())
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.collector
            .sender()
            .send_blocking(self.nfile.into())
            .ok();
    }
}

struct Collector {
    extent_map: ExtentMap,
    stat: CompsizeStat,
    scale: Scale,
}

impl Collector {
    fn new(scale: Scale) -> Self {
        Self {
            extent_map: ExtentMap::default(),
            stat: CompsizeStat::default(),
            scale,
        }
    }
    fn start(
        &self,
        sender: &Sender<CollectorMsg>,
        paths: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) {
        let nthreads = config().jobs;
        let (worker, r_worker) = bounded(nthreads * 2);
        for _ in 0..nthreads {
            let worker = Worker::new(sender.clone());
            spawn(worker.run(r_worker.clone()));
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
        let fcb = move || F(TaskPak::new(worker.clone()));
        let mut walkdir = WalkDir::new(fcb, paths, nthreads).unwrap();
        let (s_walkdir, r_walkdir) = bounded(nthreads);
        walkdir.spawn_walkers(&s_walkdir);
        spawn(walkdir.run(r_walkdir));
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
            self.stat.fmt(stdout(), self.scale).unwrap();
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
