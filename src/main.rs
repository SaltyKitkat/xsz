use std::{
    env::args,
    fs::OpenOptions,
    mem::{replace, take},
    os::unix::fs::OpenOptionsExt as _,
    path::Path,
    thread::{available_parallelism, spawn},
};

use actor::{block_on, spawn_actor, Actor, Addr, Context};
use btrfs::{ExtentInfo, Sv2Args};
use jwalk::{Parallelism, WalkDir};
use rustix::fs::{statx, AtFlags, OFlags, StatxFlags};
use scale::{CompsizeStat, ExtentMap, Scale};

mod actor;
mod btrfs;
mod scale;
// mod walkdir;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    block_on(Collector::new());
    // let worker = run_n(12, move |_| Worker::new(collector.clone()));
    // let mut pak = TaskPak::new(move |paths| {
    //     worker.send(paths).expect("send should not fail");
    // });
    // for arg in args {
    //     let path = Path::new(&arg);
    //     if !path.is_dir() {
    //         pak.push(path.into());
    //         continue;
    //     }
    //     for entry in WalkDir::new(arg)
    //         .sort(false)
    //         .skip_hidden(false)
    //         .follow_links(false)
    //         .parallelism(Parallelism::RayonNewPool(4))
    //         .into_iter()
    //         .filter_map(|e| {
    //             let e = e.ok()?;
    //             if !e.path_is_symlink() && e.file_type().is_file() {
    //                 Some(e)
    //             } else {
    //                 None
    //             }
    //         })
    //     {
    //         pak.push(entry.path().into());
    //     }
    // }
    // drop(pak);
}

struct Worker {
    sv2_args: Sv2Args,
    collector: TaskPak<ExtentInfo>,
    nfile: u64,
}

impl Worker {
    fn new(collector: Addr<Collector>) -> Self {
        Self {
            sv2_args: Sv2Args::new(),
            collector: TaskPak::new(move |file_extents| {
                collector
                    .send(file_extents.into())
                    .expect("send should not fail");
            }),
            nfile: 0,
        }
    }
}

impl Actor for Worker {
    type Message = Box<[Box<Path>]>;
    type Ret = u64;

    fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Message) {
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
            match self.sv2_args.search_file(file, ino) {
                Ok(iter) => {
                    self.nfile += 1;
                    for extent in iter.filter_map(|it| it.parse().unwrap()) {
                        self.collector.push(extent);
                    }
                }
                Err(e) => {
                    // if e.raw_os_error() == 25 {
                    //     eprintln!("{}: Not btrfs (or SEARCH_V2 unsupported)", path.display());
                    // } else {
                    //     eprintln!("{}: SEARCH_V2: {}", path.display(), e);
                    // }
                    todo!()
                }
            }
        }
    }

    fn on_exit(&mut self, ctx: &mut Context<Self>) -> Self::Ret {
        self.nfile
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

impl Actor for Collector {
    type Message = CollectorMsg;
    type Ret = ();
    fn on_start(&mut self, ctx: &mut Context<Self>) {
        let nthreads = match available_parallelism() {
            Ok(n) => n.into(),
            Err(_) => 4,
        };
        let addr = ctx.addr().unwrap();
        let worker = ctx.spawn_n(nthreads, move |_| Worker::new(addr.clone()));
        spawn(move || {
            let args: Vec<_> = args().skip(1).collect();
            let mut pak = TaskPak::new(move |paths| {
                worker.send(paths).expect("send should not fail");
            });
            for arg in args {
                let path = Path::new(&arg);
                if !path.is_dir() {
                    pak.push(path.into());
                    continue;
                }
                for entry in WalkDir::new(arg)
                    .sort(false)
                    .skip_hidden(false)
                    .follow_links(false)
                    .parallelism(Parallelism::RayonNewPool(4))
                    .into_iter()
                    .filter_map(|e| {
                        let e = e.ok()?;
                        if !e.path_is_symlink() && e.file_type().is_file() {
                            Some(e)
                        } else {
                            None
                        }
                    })
                {
                    pak.push(entry.path().into());
                }
            }
            drop(pak);
        });
    }
    fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Message) {
        match msg {
            CollectorMsg::Extent(extents) => {
                for extent in extents {
                    self.stat.insert(&mut self.extent_map, extent);
                }
            }
            CollectorMsg::NFile(n) => *self.stat.nfile_mut() += n,
        }
    }

    fn on_exit(&mut self, ctx: &mut Context<Self>) {
        println!("{}", self.stat.display(Scale::Human));
    }
}

struct TaskPak<T> {
    inner: Vec<T>,
    handler: Box<dyn FnMut(Box<[T]>) + Send + 'static>,
}

impl<T> TaskPak<T> {
    const SIZE: usize = 4096 / size_of::<T>();
    pub fn new(handler: impl FnMut(Box<[T]>) + Send + 'static) -> Self {
        Self {
            inner: Vec::with_capacity(Self::SIZE),
            handler: Box::new(handler),
        }
    }
    pub fn push(&mut self, item: T) {
        self.inner.push(item);
        if self.is_full() {
            (self.handler)(replace(&mut self.inner, Vec::with_capacity(Self::SIZE)).into());
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    fn is_full(&self) -> bool {
        self.inner.len() >= Self::SIZE
    }
}

impl<T> Drop for TaskPak<T> {
    fn drop(&mut self) {
        if !self.is_empty() {
            (self.handler)(take(&mut self.inner).into())
        }
    }
}
