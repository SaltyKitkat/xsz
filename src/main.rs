use std::{
    env::args,
    fs::OpenOptions,
    future::Future,
    io::stdout,
    os::{linux::fs::MetadataExt, unix::fs::OpenOptionsExt as _},
    path::{Path, PathBuf},
    process::exit,
    sync::LazyLock,
    thread::available_parallelism,
};

use async_channel::{bounded, Sender};
use futures_lite::future::block_on;
use just_getopt::{OptFlags, OptSpecs, OptValueType};
use mimalloc::MiMalloc;
use rustix::fs::OFlags;

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

fn print_help() {
    const HELP_MSG: &str = include_str!("./helpmsg.txt");
    eprint!("{}", HELP_MSG);
}

fn main() {
    let opt_spec = OptSpecs::new()
        .flag(OptFlags::OptionsEverywhere)
        .option("b", "b", OptValueType::None)
        .option("b", "bytes", OptValueType::None)
        .option("x", "x", OptValueType::None)
        .option("x", "one-file-system", OptValueType::None)
        .option("h", "h", OptValueType::None)
        .option("h", "help", OptValueType::None);
    let opt = opt_spec.getopt(args().skip(1));
    if let Some(unknown_arg) = opt.unknown.first() {
        eprintln!("xsz: unrecognized option '--{}'", unknown_arg);
        exit(1);
    }
    let mut scale = Scale::Human;
    for opt in opt.options {
        match opt.id.as_str() {
            "b" => scale = Scale::Bytes,
            "x" => todo!(),
            "h" => {
                print_help();
                exit(0)
            }
            _ => unreachable!(),
        }
    }
    let collector = Collector::new(scale);
    let (s, r) = bounded(32);
    collector.start(&s, opt.other);
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
            let stat = file.metadata().unwrap();
            let dev = stat.st_dev();
            let ino = stat.st_ino();
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
            let Ok(iter) = inner(&mut self.sv2_args, path, &mut self.nfile) else {
                continue;
            };
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
        self.stat.fmt(stdout(), self.scale).unwrap();
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
