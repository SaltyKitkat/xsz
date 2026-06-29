use std::{hint::cold_path, os::fd::AsFd, path::Path};

use nohash::IntSet;
use rustix::fs::{Mode, OFlags, open};

use crate::{
    actor::Sink,
    btrfs::{
        ExtentInfo, IoctlSearchItem, Sv2Wrapper,
        ioctl::{IoctlSearchKey, Sv2Args},
        tree::{self, ExtentData, TreeItem},
    },
    global::{get_err, set_err},
};

/// Scan a btrfs subvolume's tree for all EXTENT_DATA items,
/// parse them into ExtentInfo, and send to sink.
/// Returns count of unique inodes (files) found.
pub async fn scan_subvol<S: Sink<Item = ExtentInfo>>(
    mut sink: S,
    subvol_path: &Path,
) -> Result<u64, ()> {
    let fd = open(subvol_path, OFlags::DIRECTORY | OFlags::NOFOLLOW, Mode::RUSR).map_err(|e| {
        eprintln!("Failed to open '{}': {}", subvol_path.display(), e);
    })?;

    let mut sv2 = Sv2Wrapper::new(Box::new(Sv2Args::from_sk(IoctlSearchKey::new(
        0,          // tree_id = 0 → fd's subvolume tree
        0,          // min_objectid
        u64::MAX,   // max_objectid
        0,          // min_offset
        u64::MAX,   // max_offset
        0,          // min_transid
        u64::MAX,   // max_transid
        tree::r#type::EXTENT_DATA,
        tree::r#type::EXTENT_DATA,
    ))));

    let mut ino_set = IntSet::default();

    while let Some(result) = sv2.next(fd.as_fd()) {
        get_err()?;
        let (header, buf) = match result {
            Ok(v) => v,
            Err(e) => {
                set_err()?;
                if e.raw_os_error() == 25 {
                    eprintln!("{}: Not btrfs (or SEARCH_V2 unsupported)", subvol_path.display());
                } else {
                    eprintln!("Tree scan error: {}", e);
                }
                break;
            }
        };

        // Kernel search returns all items whose key falls in [min, max].
        // The 3-tuple lexicographic order means other key types (INODE_ITEM,
        // DIR_ITEM, etc.) within the objectid range also match.
        // Check type to only process EXTENT_DATA.
        if header.r#type != tree::r#type::EXTENT_DATA as u32 {
            continue;
        }

        let ext_data = unsafe { ExtentData::from_le_raw(buf) };
        let item = IoctlSearchItem {
            header,
            item: ext_data,
        };

        match item.parse() {
            Ok(Some(extent)) => {
                ino_set.insert(header.objectid);
                sink.consume(extent).await;
            }
            Ok(None) => {} // hole, skip
            Err(e) => {
                cold_path();
                set_err()?;
                eprintln!("{}", e);
                break;
            }
        }
    }

    Ok(ino_set.len() as u64)
}
