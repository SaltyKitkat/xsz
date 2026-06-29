use std::{
    num::NonZeroU64,
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    path::{Path, PathBuf},
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

/// Walk up the directory tree from `path` until we find the btrfs
/// subvolume root (inode 256).  Returns the subvolume root path.
pub fn find_subvol_root(path: &Path) -> Result<PathBuf> {
    let mut cur = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("/"))
    };
    loop {
        let fd = open(&cur, OFlags::DIRECTORY | OFlags::NOFOLLOW, Mode::RUSR)?;
        if fstat(fd.as_fd())?.st_ino == 256 {
            return Ok(cur);
        }
        if let Some(parent) = cur.parent() {
            cur = parent.to_path_buf();
        } else {
            break;
        }
    }
    Err(rustix::io::Errno::NOENT)
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
