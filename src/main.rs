use std::{future::Future, process::exit};

use collector::CollectorMsg;
use executor::block_on;
use fs_util::File_;
use kanal::bounded_async as bounded;
use mimalloc::MiMalloc;

mod actor;
mod btrfs;
mod collector;
mod executor;
mod fs_util;
mod global;
mod scale;
mod taskpak;
mod walkdir;
mod worker;

use actor::{Actor, Runnable as _};
use global::{config, get_err};
use taskpak::TaskPak;
use walkdir::{FileConsumer, WalkDir};
use worker::Worker;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) {
    executor::spawn(future).detach();
}

fn main() {
    let nworkers = config().jobs;
    let (worker_tx, worker_rx) = bounded(4 * 1024 / size_of::<<Worker as Actor>::Message>());
    let (sender, r) = bounded(4 * 1024 / size_of::<CollectorMsg>());
    pub(crate) struct F(TaskPak<File_, <Worker as Actor>::Message>);
    impl FileConsumer for F {
        fn consume(
            &mut self,
            f: File_,
        ) -> impl std::future::Future<Output = ()> + std::marker::Send {
            self.0.push(f)
        }
    }
    let fcb = move || F(TaskPak::new(worker_tx.clone()));
    WalkDir::spawn(fcb, &config().args, nworkers);
    for _ in 0..nworkers {
        let worker = Worker::new(sender.clone());
        spawn(worker.run(worker_rx.clone()));
    }
    drop(sender);
    let collector = collector::Collector::new();
    block_on(collector.run(r));
    if get_err().is_err() {
        exit(1)
    }
}
