use std::{io::stdout, path::PathBuf};

use kanal::{bounded_async as bounded, AsyncSender as Sender};

use crate::{
    actor::{Actor, Runnable as _},
    btrfs::ExtentInfo,
    fs_util::File_,
    global::{config, get_err},
    scale::CompsizeStat,
    spawn,
    taskpak::TaskPak,
    walkdir::{FileConsumer, WalkDir},
    worker::Worker,
};

pub(crate) struct Collector {
    pub(crate) stat: CompsizeStat,
}

impl Collector {
    pub(crate) fn new() -> Self {
        Self {
            stat: CompsizeStat::default(),
        }
    }
    pub(crate) fn start(
        &self,
        sender: Sender<CollectorMsg>,
        paths: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) {
        let nworkers = config().jobs;
        let (worker_tx, worker_rx) = bounded(4 * 1024 / size_of::<<Worker as Actor>::Message>());
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
        WalkDir::spawn(fcb, paths, nworkers);
        for _ in 0..nworkers {
            let worker = Worker::new(sender.clone());
            spawn(worker.run(worker_rx.clone()));
        }
    }
}

impl Actor for Collector {
    type Message = CollectorMsg;

    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        match msg {
            CollectorMsg::Extent(extents) => {
                for extent in extents {
                    self.stat.insert(extent);
                }
            }
            CollectorMsg::NFile(n) => *self.stat.nfile_mut() += n,
        }
        Ok(())
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        if get_err().is_ok() {
            self.stat.fmt(stdout(), config().scale()).unwrap();
        }
    }
}

pub(crate) enum CollectorMsg {
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
