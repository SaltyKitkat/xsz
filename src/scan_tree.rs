use std::{hint::cold_path, os::fd::AsFd, path::Path};

use nohash::IntSet;
use rustix::fs::{Mode, OFlags, open};

use crate::{
    actor::Sink,
    btrfs::{
        ExtentInfo, IoctlSearchItem, Sv2Wrapper,
        ioctl::{IoctlSearchKey, SearchHeader, Sv2Args},
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
                    eprintln!(
                        "{}: Not btrfs (or SEARCH_V2 unsupported)",
                        subvol_path.display()
                    );
                } else {
                    eprintln!("Tree scan error: {}", e);
                }
                break;
            }
        };

        // Kernel search returns all items whose key falls in [min, max].
        // The 3-tuple lexicographic order means other types (INODE_ITEM,
        // DIR_ITEM, etc.) within the key range also match.
        if header.r#type != tree::r#type::EXTENT_DATA as u32 {
            skip_to_extent(&mut sv2, &header);
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

/// Optimise the next search key to skip irrelevant items.
///
/// The generic `Sv2Wrapper::next()` advances one position at a time
/// (`offset++ → type++ → objectid++`), so after seeing a `DIR_ITEM`
/// the next ioctl would re-enter the same range.  Since we only
/// care about `EXTENT_DATA`, we can jump straight to the relevant
/// part of the key space:
///
///   * **DIR types** – the objectid is a directory, no extents possible,
///     skip to `(oid+1, EXTENT_DATA, 0)`.
///   * **Other types < EXTENT_DATA** – jump to `(oid, EXTENT_DATA, 0)`.
///   * **Types > EXTENT_DATA** – nothing left for this objectid,
///     skip to `(oid+1, EXTENT_DATA, 0)`.
///
/// The override is applied unconditionally; if the current ioctl buffer
/// still has more items the next `next()` call ignores the key change
/// and returns the buffered item.  Only the next ioctl boundary sees
/// the smarter key.
fn skip_to_extent(sv2: &mut Sv2Wrapper, header: &SearchHeader) {
    const EXT_DATA: u32 = tree::r#type::EXTENT_DATA as u32;

    if header.r#type < EXT_DATA {
        // DIR_ITEM (84) / DIR_INDEX (96) / DIR_LOG_ITEM (60) /
        // DIR_LOG_INDEX (72) → this objectid is a directory.
        if header.r#type == tree::r#type::DIR_ITEM as u32
            || header.r#type == tree::r#type::DIR_INDEX as u32
            || header.r#type == tree::r#type::DIR_LOG_ITEM as u32
            || header.r#type == tree::r#type::DIR_LOG_INDEX as u32
        {
            sv2.set_min_key(header.objectid.saturating_add(1), EXT_DATA, 0);
        } else {
            // INODE_ITEM, INODE_REF, XATTR_ITEM, … — unknown
            // whether file or dir, but jump to EXTENT_DATA anyway.
            sv2.set_min_key(header.objectid, EXT_DATA, 0);
        }
    } else if header.r#type > EXT_DATA {
        // Past EXTENT_DATA – skip entire objectid.
        sv2.set_min_key(header.objectid.saturating_add(1), EXT_DATA, 0);
    }
    // equal → handled by Sv2Wrapper::next() internally
}
