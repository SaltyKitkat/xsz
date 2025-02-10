use rustix::fs::{fstat, lstat, makedev, statx, AtFlags, StatxFlags, CWD};
use std::{os::fd::AsFd, path::Path};

pub(crate) fn get_dev(path: impl AsRef<Path>) -> u64 {
    // lstat(path.as_ref()).unwrap().st_dev
    let stx = statx(
        CWD,
        path.as_ref(),
        AtFlags::SYMLINK_NOFOLLOW,
        StatxFlags::empty(),
    )
    .unwrap();
    let major = stx.stx_dev_major;
    let minor = stx.stx_dev_minor;
    makedev(major, minor)
}

pub(crate) fn get_ino(file: impl AsFd) -> u64 {
    fstat(file).unwrap().st_ino
}
