use std::{fs::File, num::NonZeroU64, os::unix::fs::MetadataExt, path::Path};

pub(crate) type DevId = NonZeroU64;
pub(crate) fn get_dev(path: impl AsRef<Path>) -> DevId {
    let dev = path.as_ref().symlink_metadata().unwrap().dev();
    NonZeroU64::new(dev).unwrap()
}

pub(crate) fn get_ino(file: &File) -> u64 {
    file.metadata().unwrap().ino()
}
