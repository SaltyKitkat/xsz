use std::{
    env::args,
    fs::OpenOptions,
    future::{pending, Future},
    io::stdout,
    os::unix::fs::OpenOptionsExt as _,
    panic::catch_unwind,
    path::Path,
    sync::LazyLock,
    thread::{self, available_parallelism},
};

use async_channel::{bounded, Sender};
use btrfs::{ExtentInfo, Sv2Args};
use rustix::fs::{statx, AtFlags, OFlags, StatxFlags};
use scale::{CompsizeStat, ExtentMap, Scale};

mod actor;
mod btrfs;
mod scale;
mod taskpak;
mod walkdir;

use actor::Actor;
use mimalloc::MiMalloc;
use smol::{block_on, Executor};
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

fn global() -> &'static LazyLock<Executor<'static>> {
    static GLOBAL: LazyLock<Executor<'_>> = LazyLock::new(|| {
        let num_threads = nthreads();

        for n in 0..num_threads + num_threads {
            thread::Builder::new()
                .name(format!("xsz-worker{}", n))
                .spawn(|| loop {
                    catch_unwind(|| smol::block_on(GLOBAL.run(pending::<()>()))).ok();
                })
                .expect("cannot spawn executor thread");
        }

        let ex = Executor::new();
        ex
    });
    &GLOBAL
}

pub fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) {
    global().spawn(future).detach();
}

fn main() {
    let collector = Collector::new();
    let (s, r) = bounded(32);
    collector.start(&s);
    drop(s);
    block_on(collector.run(r));
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
    async fn handle(&mut self, msg: Self::Message) {
        for path in msg {
            let file = OpenOptions::new()
                .read(true)
                .write(false)
                .custom_flags((OFlags::NOFOLLOW | OFlags::NOCTTY | OFlags::NONBLOCK).bits() as _)
                .open(&path)
                .unwrap();
            let ino = statx(&file, "", AtFlags::EMPTY_PATH, StatxFlags::INO)
                .unwrap()
                .stx_ino;
            match self.sv2_args.search_file(file.into(), ino) {
                Ok(iter) => {
                    self.nfile += 1;
                    for extent in iter.filter_map(|it| it.parse().unwrap()) {
                        self.collector.push(extent).await;
                    }
                }
                Err(e) => {
                    if e.raw_os_error() == 25 {
                        eprintln!("{}: Not btrfs (or SEARCH_V2 unsupported)", path.display());
                    } else {
                        eprintln!("{}: SEARCH_V2: {}", path.display(), e);
                    }
                }
            }
        }
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

    async fn handle(&mut self, msg: Self::Message) {
        match msg {
            CollectorMsg::Extent(extents) => {
                for extent in extents {
                    self.stat.insert(&mut self.extent_map, extent);
                }
            }
            CollectorMsg::NFile(n) => *self.stat.nfile_mut() += n,
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.stat.fmt(stdout(), Scale::Human).unwrap();
    }
}

#[derive(Debug)]
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
