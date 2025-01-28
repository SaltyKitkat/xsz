use std::{
    env::args,
    fs::OpenOptions,
    future::{pending, Future},
    io::stdout,
    mem::{replace, take},
    os::unix::fs::OpenOptionsExt as _,
    panic::catch_unwind,
    path::Path,
    sync::LazyLock,
    thread::{self, available_parallelism},
};

use actor::{spawn_actor, Actor, Addr, Context};
use btrfs::{ExtentInfo, Sv2Args};
use rustix::fs::{statx, AtFlags, OFlags, StatxFlags};
use scale::{CompsizeStat, ExtentMap, Scale};

mod actor;
mod async_oneshot;
mod btrfs;
mod scale;
mod walkdir;

use mimalloc::MiMalloc;
use smol::Executor;
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

        for n in 0..num_threads {
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
    smol::block_on(actor::block_on(Collector::new()));
}

struct Worker {
    sv2_args: Sv2Args,
    collector: TaskPak<ExtentInfo, CollectorMsg>,
    nfile: u64,
}

impl Worker {
    fn new(collector: Addr<CollectorMsg>) -> Self {
        Self {
            sv2_args: Sv2Args::new(),
            collector: TaskPak::new(collector),
            nfile: 0,
        }
    }
}

type WorkerMsg = Box<[Box<Path>]>;

impl Actor for Worker {
    type Message = WorkerMsg;
    type Ret = u64;

    fn handle(
        &mut self,
        _ctx: &mut Context<Self::Message>,
        msg: Self::Message,
    ) -> impl Future<Output = ()> + Send {
        async {
            for path in msg {
                let file = OpenOptions::new()
                    .read(true)
                    .write(false)
                    .custom_flags(
                        (OFlags::NOFOLLOW | OFlags::NOCTTY | OFlags::NONBLOCK).bits() as _,
                    )
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

    fn on_exit(&mut self, _ctx: &mut Context<Self::Message>) -> impl Future<Output = Self::Ret> {
        async { self.nfile }
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
    async fn on_start(&mut self, ctx: &mut Context<Self::Message>) {
        let nthreads = nthreads();
        let addr = ctx.addr().unwrap().clone();
        let worker = ctx.spawn_n(nthreads, move |_| Worker::new(addr.clone()));
        spawn(async move {
            let args: Vec<_> = args().skip(1).collect();
            struct F(TaskPak<Box<Path>, WorkerMsg>);
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
            spawn_actor(WalkDir::new(fcb, args, nthreads).unwrap());
        });
        ctx.take_addr().unwrap();
    }
    async fn handle(&mut self, _ctx: &mut Context<Self::Message>, msg: Self::Message) {
        match msg {
            CollectorMsg::Extent(extents) => {
                for extent in extents {
                    self.stat.insert(&mut self.extent_map, extent);
                }
            }
            CollectorMsg::NFile(n) => *self.stat.nfile_mut() += n,
        }
    }

    async fn on_exit(&mut self, _ctx: &mut Context<Self::Message>) {
        // println!("{}", self.stat.display(Scale::Human));
        self.stat.fmt(stdout(), Scale::Human).unwrap();
    }
}

struct TaskPak<T, M>
where
    M: Send + 'static,
    Box<[T]>: Into<M>,
{
    inner: Vec<T>,
    handler: Addr<M>,
}

impl<T, M> TaskPak<T, M>
where
    M: Send + 'static,
    Box<[T]>: Into<M>,
{
    const SIZE: usize = 1024 * 8 / size_of::<T>();
    pub fn new(handler: Addr<M>) -> Self {
        Self {
            inner: Vec::with_capacity(Self::SIZE),
            handler,
        }
    }
    pub async fn push(&mut self, item: T) {
        self.inner.push(item);
        if self.is_full() {
            self.handler
                .send(
                    replace(&mut self.inner, Vec::with_capacity(Self::SIZE))
                        .into_boxed_slice()
                        .into(),
                )
                .await
                .ok();
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    fn is_full(&self) -> bool {
        self.inner.len() >= Self::SIZE
    }
}

impl<T, M> Drop for TaskPak<T, M>
where
    M: Send + 'static,
    Box<[T]>: Into<M>,
{
    fn drop(&mut self) {
        if !self.is_empty() {
            let handler = self.handler.clone();
            let item = take(&mut self.inner).into_boxed_slice().into();
            spawn(async move {
                handler.send(item).await.ok();
            });
        }
    }
}
