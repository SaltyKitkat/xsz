use std::{
    env::args,
    fmt::{Display, Write},
    fs::OpenOptions,
    mem::{replace, size_of, take},
    os::unix::fs::OpenOptionsExt as _,
    path::Path,
    process::{self, exit},
    sync::atomic::{AtomicBool, Ordering},
    thread::scope,
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use dashmap::DashSet;
use jwalk::WalkDir;
use nohash::BuildNoHashHasher;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod btrfs;
mod scale;
use btrfs::{ExtentType, Sv2Args};
use rustix::fs::{statx, AtFlags, OFlags, StatxFlags};
use scale::Scale;

type WorkerRx = Receiver<Box<[Box<Path>]>>;
type WorkerTx = Sender<Box<[Box<Path>]>>;
// blocking syscall: ioctl, so we run on multiple threads
struct Worker<'rx, 'map, 'sig> {
    rx: &'rx WorkerRx,
    stat: CompsizeStat,
    sv2_arg: Sv2Args,
    extent_map: &'map ExtentMap,
    quit_sig: &'sig AtomicBool,
}
impl<'rx, 'map, 'sig> Worker<'rx, 'map, 'sig> {
    fn new(recv: &'rx WorkerRx, extent_map: &'map ExtentMap, quit_sig: &'sig AtomicBool) -> Self {
        Self {
            rx: recv,
            stat: CompsizeStat::default(),
            sv2_arg: Sv2Args::new(),
            extent_map,
            quit_sig,
        }
    }

    fn run(mut self) -> CompsizeStat {
        while let Ok(paths) = self.rx.recv() {
            if self.quit_sig.load(Ordering::Acquire) {
                break;
            }
            for path in paths {
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
                match self.sv2_arg.search_file(file, ino) {
                    Ok(iter) => {
                        self.stat.nfile += 1;
                        for (key, comp, estat) in iter.filter_map(|item| item.parse().unwrap()) {
                            merge_stat(self.extent_map, key, comp, estat, &mut self.stat);
                        }
                    }
                    Err(e) => {
                        self.quit_sig.store(true, Ordering::Release);
                        if e.raw_os_error() == 25 {
                            eprintln!("{}: Not btrfs (or SEARCH_V2 unsupported)", path.display());
                        } else {
                            eprintln!("{}: SEARCH_V2: {}", path.display(), e);
                        }
                        break;
                    }
                }
            }
        }
        self.stat
    }
}
const WORKER_CNT: usize = 12;

struct TaskPak<T> {
    inner: Vec<T>,
    sender: Sender<Box<[T]>>,
}

impl<T> TaskPak<T> {
    const SIZE: usize = 4096 / size_of::<T>();
    pub fn new(sender: Sender<Box<[T]>>) -> Self {
        Self {
            inner: Vec::with_capacity(Self::SIZE),
            sender,
        }
    }
    pub fn push(&mut self, item: T) {
        self.inner.push(item);
        if self.is_full() {
            self.sender
                .send(replace(&mut self.inner, Vec::with_capacity(Self::SIZE)).into())
                .unwrap();
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
            self.sender
                .send(take(&mut self.inner).into())
                .ok();
        }
    }
}

fn main() {
    let (ftx, frx) = unbounded();
    let extent_map = DashSet::with_hasher(BuildNoHashHasher::default());
    let quit_sig = AtomicBool::new(false);
    let final_stat = scope(|ex| {
        let args: Vec<_> = args().skip(1).collect();
        {
            let quit_sig = &quit_sig;
            ex.spawn(move || {
                let mut pak = TaskPak::new(ftx);
                for arg in args {
                    for entry in WalkDir::new(arg)
                        .sort(false)
                        .skip_hidden(false)
                        .follow_links(false)
                        .parallelism(jwalk::Parallelism::RayonNewPool(4))
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
                        if quit_sig.load(Ordering::Acquire) {
                            return;
                        }
                        pak.push(entry.path().into());
                    }
                }
            });
        }
        let handles: Vec<_> = (0..WORKER_CNT)
            .map(|_| {
                let worker = Worker::new(&frx, &extent_map, &quit_sig);
                ex.spawn(|| worker.run())
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .reduce(|mut a, b| {
                a.merge(b);
                a
            })
            .unwrap()
    });
    if quit_sig.load(Ordering::Acquire) {
        process::exit(1);
    }

    if final_stat.nfile == 0 {
        eprintln!("No files.");
        exit(1);
    } else if final_stat.nref == 0 {
        eprintln!("All empty or still-delalloced files.");
        exit(1);
    }
    println!("{}", final_stat.display(Scale::Human));
}
