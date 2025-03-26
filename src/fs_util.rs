use std::{
    num::NonZeroU64,
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    path::Path,
    sync::Arc,
};

use rustix::{
    fs::{fstat, open, stat, Mode, OFlags},
    io::Result,
};

pub(crate) type DevId = NonZeroU64;
pub(crate) fn get_dev(path: impl AsRef<Path>) -> DevId {
    // let dev = path.as_ref().symlink_metadata().unwrap().dev();
    // NonZeroU64::new(dev).unwrap()
    let dev = stat(path.as_ref()).unwrap().st_dev;
    NonZeroU64::new(dev).unwrap()
}

pub struct File_ {
    fd: Arc<OwnedFd>,
    path: Box<Path>,
    ino: u64,
}

impl File_ {
    pub fn new(fd: Arc<OwnedFd>, path: Box<Path>, ino: u64) -> Self {
        Self { fd, path, ino }
    }
    pub fn borrow_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
    pub fn ino(&self) -> u64 {
        self.ino
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn from_path(p: Box<Path>) -> Result<Self> {
        let fd = Arc::new(open(p.as_ref(), OFlags::NOFOLLOW, Mode::RUSR)?);
        let ino = fstat(fd.as_fd())?.st_ino;
        Ok(Self::new(fd, p, ino))
    }
}
