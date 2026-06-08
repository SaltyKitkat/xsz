use std::{
    num::NonZeroU64,
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    path::Path,
    sync::Arc,
};

use rustix::{
    fs::{Mode, OFlags, fstat, open, stat},
    io::Result,
};

pub(crate) type DevId = NonZeroU64;
pub(crate) fn get_dev(path: impl AsRef<Path>) -> DevId {
    let dev = stat(path.as_ref()).unwrap().st_dev;
    NonZeroU64::new(dev).unwrap()
}

pub struct File_ {
    fd: Arc<OwnedFd>,
    path: Box<Path>,
    ino: u64,
}

impl File_ {
    #[inline]
    pub fn new(fd: Arc<OwnedFd>, path: Box<Path>, ino: u64) -> Self {
        Self { fd, path, ino }
    }
    #[inline]
    pub fn borrow_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
    #[inline]
    pub fn ino(&self) -> u64 {
        self.ino
    }
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn from_path(p: Box<Path>) -> Result<Self> {
        let fd = Arc::new(open(p.as_ref(), OFlags::NOFOLLOW, Mode::RUSR)?);
        let stat = fstat(fd.as_fd())?;
        let ino = stat.st_ino;
        Ok(Self::new(fd, p, ino))
    }
}
