use std::{num::NonZeroU64, path::Path};

use rustix::{
    fs::{stat, statx, AtFlags, Dir, Mode, OFlags, StatxFlags, CWD},
    io::Result,
};

pub(crate) type DevId = NonZeroU64;
pub(crate) fn get_dev(path: impl AsRef<Path>) -> DevId {
    // let dev = path.as_ref().symlink_metadata().unwrap().dev();
    // NonZeroU64::new(dev).unwrap()
    let dev = stat(path.as_ref()).unwrap().st_dev;
    NonZeroU64::new(dev).unwrap()
}

pub(crate) fn get_ino(file: &Path) -> u64 {
    statx(CWD, file, AtFlags::SYMLINK_NOFOLLOW, StatxFlags::INO)
        .map(|s| s.stx_ino)
        .unwrap()
}

// use rustix apis to avoid using std
pub(crate) fn read_dir(path: impl AsRef<Path>) -> Result<Dir> {
    let dir = rustix::fs::open(
        path.as_ref(),
        OFlags::DIRECTORY | OFlags::NOFOLLOW,
        Mode::RUSR,
    )?;
    Dir::new(dir)
}
