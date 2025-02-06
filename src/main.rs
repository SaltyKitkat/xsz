use std::{
    env::args, fs::OpenOptions, future::Future, io::stdout, os::unix::fs::OpenOptionsExt as _,
    path::Path, process::exit, sync::LazyLock, thread::available_parallelism,
};

use async_channel::{bounded, Sender};
use futures_lite::future::block_on;
use mimalloc::MiMalloc;
use rustix::fs::{statx, AtFlags, OFlags, StatxFlags};

mod actor;
mod btrfs;
mod executor;
mod global_err;
mod scale;
mod taskpak;
mod walkdir;

use actor::{Actor, Runnable as _};
use btrfs::{ExtentInfo, Sv2Args};
use scale::{CompsizeStat, ExtentMap, Scale};
use taskpak::TaskPak;
use walkdir::{FileConsumer, WalkDir};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn nthreads() -> usize {
    static NTHREADS: LazyLock<usize> = LazyLock::new(|| {
        let nthreads = match available_parallelism() {
            Ok(n) => n.into(),
            Err(_) => 4,
        };
        nthreads
    });
    *NTHREADS
}

pub fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) {
    executor::spawn(future).detach();
}

fn main() {
    let collector = Collector::new();
    let (s, r) = bounded(32);
    collector.start(&s);
    drop(s);
    block_on(collector.run(r));
    if global_err::get().is_err() {
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
            let file = OpenOptions::new()
                .read(true)
                .write(false)
                .custom_flags((OFlags::NOFOLLOW | OFlags::NOCTTY | OFlags::NONBLOCK).bits() as _)
                .open(&path)
                .unwrap();
            let ino = statx(&file, "", AtFlags::EMPTY_PATH, StatxFlags::INO)
                .unwrap()
                .stx_ino;
            match sv2_args.search_file(file.into(), ino) {
                Ok(iter) => {
                    *self_nfile += 1;
                    return Ok(iter);
                }
                Err(e) => {
                    global_err::set()?;
                    if e.raw_os_error() == 25 {
                        eprintln!("{}: Not btrfs (or SEARCH_V2 unsupported)", path.display());
                    } else {
                        eprintln!("{}: SEARCH_V2: {}", path.display(), e);
                    }
                    return Err(());
                }
            }
        }
        for path in msg {
            global_err::get()?;
            let iter = inner(&mut self.sv2_args, path, &mut self.nfile)?;
            for extent in iter.filter_map(|it| it.parse().unwrap()) {
                self.collector.push(extent).await;
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
}

impl Collector {
    fn new() -> Self {
        Self {
            extent_map: ExtentMap::default(),
            stat: CompsizeStat::default(),
        }
    }
    fn start(&self, sender: &Sender<CollectorMsg>) {
        let nthreads = nthreads();
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
        let args: Vec<_> = args().skip(1).collect();
        let fcb = move || F(TaskPak::new(worker.clone()));
        let mut walkdir = WalkDir::new(fcb, args, nthreads).unwrap();
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
        self.stat.fmt(stdout(), Scale::Human).unwrap();
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
