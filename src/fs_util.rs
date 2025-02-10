use std::{fs::File, os::unix::fs::MetadataExt, path::Path};

pub(crate) fn get_dev(path: impl AsRef<Path>) -> u64 {
    path.as_ref().symlink_metadata().unwrap().dev()
}

pub(crate) fn get_ino(file: &File) -> u64 {
    file.metadata().unwrap().ino()
}
