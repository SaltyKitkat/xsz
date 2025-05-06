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

use actor::Runnable as _;
use global::{config, get_err};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) {
    executor::spawn(future).detach();
}

fn main() {
    let collector = collector::Collector::new();
    let (s, r) = bounded(4 * 1024 / size_of::<CollectorMsg>());
    collector.start(s, &config().args);
    block_on(collector.run(r));
    if get_err().is_err() {
        exit(1)
    }
}
