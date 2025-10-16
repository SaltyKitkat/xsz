use std::io::stdout;

use crate::{
    actor::Actor,
    btrfs::ExtentInfo,
    global::{config, get_err},
    scale::CompsizeStat,
};

pub struct Collector {
    pub(crate) stat: CompsizeStat,
}

impl Collector {
    pub fn new() -> Self {
        Self {
            stat: CompsizeStat::default(),
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

pub enum CollectorMsg {
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
