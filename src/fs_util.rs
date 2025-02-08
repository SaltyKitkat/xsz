use rustix::fs::{makedev, statx, AtFlags, StatxFlags, CWD};
use std::{os::fd::AsFd, path::Path};

pub(crate) fn get_dev(path: impl AsRef<Path>) -> u64 {
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
    statx(file, "", AtFlags::EMPTY_PATH, StatxFlags::INO)
        .unwrap()
        .stx_ino
}
