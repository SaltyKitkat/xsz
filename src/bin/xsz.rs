use std::process::exit;

use kanal::bounded_async as bounded;
use mimalloc::MiMalloc;
use xsz::{
    actor::{Actor, Runnable},
    collector,
    executor::block_on,
    fs_util::File_,
    global::{config, get_err},
    spawn,
    taskpak::TaskPak,
    walkdir::{FileConsumer, WalkDir},
    worker::Worker,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let nworkers = config().jobs;
    let (worker_tx, worker_rx) = bounded(64);
    let (sender, r) = bounded(64);
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
