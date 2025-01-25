use std::{
    env::{self, args},
    fs::OpenOptions,
    future::{pending, Future},
    mem::{replace, take},
    num::NonZero,
    os::unix::fs::OpenOptionsExt as _,
    panic::catch_unwind,
    path::Path,
    sync::OnceLock,
    thread::{self, available_parallelism},
};

use actor::{spawn_actor, Actor, Addr, Context};
use btrfs::{ExtentInfo, Sv2Args};
use rustix::fs::{statx, AtFlags, OFlags, StatxFlags};
use scale::{CompsizeStat, ExtentMap, Scale};

mod actor;
mod btrfs;
mod scale;
mod walkdir;

use mimalloc::MiMalloc;
use smol::Executor;
use walkdir::WalkDir;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let nthreads = match available_parallelism() {
        Ok(n) => n.into(),
        Err(_) => 4,
    };
    // env::set_var("SMOL_THREADS", format!("{}", nthreads));
    smol::block_on(actor::block_on(Collector::new()));
}

pub fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) {
    static GLOBAL: OnceLock<Executor<'_>> = OnceLock::new();

    fn global() -> &'static Executor<'static> {
        GLOBAL.get_or_init(|| {
            let num_threads = match available_parallelism() {
                Ok(n) => n.into(),
                Err(_) => 4,
            };

            for n in 0..num_threads {
                thread::Builder::new()
                    .name(format!("xsz-{}", n))
                    .spawn(|| loop {
                        catch_unwind(|| smol::block_on(global().run(pending::<()>()))).ok();
                    })
                    .expect("cannot spawn executor thread");
            }

            let ex = Executor::new();
            ex
        })
    }

    global().spawn(future).detach();
}

struct Worker {
    sv2_args: Sv2Args,
    collector: TaskPak<ExtentInfo, Collector>,
    nfile: u64,
}

impl Worker {
    fn new(collector: Addr<Collector>) -> Self {
        Self {
            sv2_args: Sv2Args::new(),
            collector: TaskPak::new(collector),
            nfile: 0,
        }
    }
}

impl Actor for Worker {
    type Message = Box<[Box<Path>]>;
    type Ret = u64;

    fn handle(
        &mut self,
        ctx: &mut Context<Self>,
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
    }

    fn on_exit(&mut self, ctx: &mut Context<Self>) -> impl Future<Output = Self::Ret> {
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
    async fn on_start(&mut self, ctx: &mut Context<Self>) {
        let nthreads = match available_parallelism() {
            Ok(n) => n.into(),
            Err(_) => 4,
        };
        let addr = ctx.addr().unwrap();
        let worker = ctx.spawn_n(nthreads, move |_| Worker::new(addr.clone()));
        spawn(async move {
            let args: Vec<_> = args().skip(1).collect();
            for arg in args {
                spawn_actor(WalkDir::new(worker.clone(), arg, NonZero::new(12).unwrap()).unwrap());
            }
        });
    }
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Message) {
        match msg {
            CollectorMsg::Extent(extents) => {
                for extent in extents {
                    self.stat.insert(&mut self.extent_map, extent);
                }
            }
            CollectorMsg::NFile(n) => *self.stat.nfile_mut() += n,
        }
    }

    fn on_exit(&mut self, ctx: &mut Context<Self>) -> impl Future<Output = ()> {
        async {
            println!("{}", self.stat.display(Scale::Human));
        }
    }
}

struct TaskPak<T, A>
where
    A: Actor + 'static,
    Box<[T]>: Into<A::Message>,
{
    inner: Vec<T>,
    handler: Addr<A>,
}

impl<T, A> TaskPak<T, A>
where
    A: Actor + 'static,
    Box<[T]>: Into<A::Message>,
{
    const SIZE: usize = 1024 * 32 / size_of::<T>();
    pub fn new(handler: Addr<A>) -> Self {
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

impl<T, A> Drop for TaskPak<T, A>
where
    A: Actor + 'static,
    Box<[T]>: Into<A::Message>,
{
    fn drop(&mut self) {
        // if !self.is_empty() {
        let handler = self.handler.clone();
        let item = take(&mut self.inner).into_boxed_slice().into();
        spawn(async move {
            handler.send(item).await.ok();
        });
        // }
    }
}
