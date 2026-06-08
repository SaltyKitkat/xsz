use std::{fmt::Display, mem::transmute};

#[repr(C, packed)]
pub struct Key {
    pub objectid: u64,
    pub r#type: u8,
    pub offset: u64,
}

pub mod objectid {
    #![allow(dead_code)]
    pub const ROOT_TREE: u64 = 1;
    pub const EXTENT_TREE: u64 = 2;
    pub const CHUNK_TREE: u64 = 3;
    pub const DEV_TREE: u64 = 4;
    pub const FS_TREE: u64 = 5;
    pub const ROOT_TREE_DIR: u64 = 6;
    pub const CSUM_TREE: u64 = 7;
    pub const QUOTA_TREE: u64 = 8;
    pub const UUID_TREE: u64 = 9;
    pub const FREE_SPACE_TREE: u64 = 10;
    pub const BLOCK_GROUP_TREE: u64 = 11;
    pub const RAID_STRIPE_TREE: u64 = 12;
    pub const BALANCE: u64 = -4i64 as u64;
    pub const ORPHAN: u64 = -5i64 as u64;
    pub const TREE_LOG: u64 = -6i64 as u64;
    pub const TREE_LOG_FIXUP: u64 = -7i64 as u64;
    pub const TREE_RELOC: u64 = -8i64 as u64;
    pub const DATA_RELOC_TREE: u64 = -9i64 as u64;
    pub const EXTENT_CSUM: u64 = -10i64 as u64;
    pub const FREE_SPACE: u64 = -11i64 as u64;
    pub const FREE_INO: u64 = -12i64 as u64;

    pub fn name(objectid: u64) -> Option<&'static str> {
        Some(match objectid {
            ROOT_TREE => "ROOT_TREE",
            EXTENT_TREE => "EXTENT_TREE",
            CHUNK_TREE => "CHUNK_TREE",
            DEV_TREE => "DEV_TREE",
            FS_TREE => "FS_TREE",
            ROOT_TREE_DIR => "ROOT_TREE_DIR",
            CSUM_TREE => "CSUM_TREE",
            QUOTA_TREE => "QUOTA_TREE",
            UUID_TREE => "UUID_TREE",
            FREE_SPACE_TREE => "FREE_SPACE_TREE",
            BLOCK_GROUP_TREE => "BLOCK_GROUP_TREE",
            RAID_STRIPE_TREE => "RAID_STRIPE_TREE",
            BALANCE => "BALANCE",
            ORPHAN => "ORPHAN",
            TREE_LOG => "TREE_LOG",
            TREE_LOG_FIXUP => "TREE_LOG_FIXUP",
            TREE_RELOC => "TREE_RELOC",
            DATA_RELOC_TREE => "DATA_RELOC_TREE",
            EXTENT_CSUM => "EXTENT_CSUM",
            FREE_SPACE => "FREE_SPACE",
            FREE_INO => "FREE_INO",
            _ => return None,
        })
    }
}

pub mod r#type {
    #![allow(dead_code)]
    pub const INODE_ITEM: u8 = 1;
    pub const INODE_REF: u8 = 12;
    pub const INODE_EXTREF: u8 = 13;
    pub const XATTR_ITEM: u8 = 24;
    pub const VERITY_DESC_ITEM: u8 = 36;
    pub const VERITY_MERKLE_ITEM: u8 = 37;
    pub const ORPHAN_ITEM: u8 = 48;
    pub const DIR_LOG_ITEM: u8 = 60;
    pub const DIR_LOG_INDEX: u8 = 72;
    pub const DIR_ITEM: u8 = 84;
    pub const DIR_INDEX: u8 = 96;
    pub const EXTENT_DATA: u8 = 108;
    pub const EXTENT_CSUM: u8 = 128;
    pub const ROOT_ITEM: u8 = 132;
    pub const ROOT_BACKREF: u8 = 144;
    pub const ROOT_REF: u8 = 156;
    pub const EXTENT_ITEM: u8 = 168;
    pub const METADATA_ITEM: u8 = 169;
    pub const EXTENT_OWNER_REF: u8 = 172;
    pub const TREE_BLOCK_REF: u8 = 176;
    pub const EXTENT_DATA_REF: u8 = 178;
    pub const SHARED_BLOCK_REF: u8 = 182;
    pub const SHARED_DATA_REF: u8 = 184;
    pub const BLOCK_GROUP_ITEM: u8 = 192;
    pub const FREE_SPACE_INFO: u8 = 198;
    pub const FREE_SPACE_EXTENT: u8 = 199;
    pub const FREE_SPACE_BITMAP: u8 = 200;
    pub const DEV_EXTENT: u8 = 204;
    pub const DEV_ITEM: u8 = 216;
    pub const CHUNK_ITEM: u8 = 228;
    pub const RAID_STRIPE: u8 = 230;
    pub const QGROUP_STATUS: u8 = 240;
    pub const QGROUP_INFO: u8 = 242;
    pub const QGROUP_LIMIT: u8 = 244;
    pub const QGROUP_RELATION: u8 = 246;
    pub const TEMPORARY_ITEM: u8 = 248;
    pub const PERSISTENT_ITEM: u8 = 249;
    pub const DEV_REPLACE: u8 = 250;
    pub const UUID_SUBVOL: u8 = 251;
    pub const UUID_RECEIVED_SUBVOL: u8 = 252;
    pub const STRING_ITEM: u8 = 253;
    pub fn name(r#type: u8) -> Option<&'static str> {
        Some(match r#type {
            INODE_ITEM => "INODE_ITEM",
            INODE_REF => "INODE_REF",
            INODE_EXTREF => "INODE_EXTREF",
            XATTR_ITEM => "XATTR_ITEM",
            VERITY_DESC_ITEM => "VERITY_DESC_ITEM",
            VERITY_MERKLE_ITEM => "VERITY_MERKLE_ITEM",
            ORPHAN_ITEM => "ORPHAN_ITEM",
            DIR_LOG_ITEM => "DIR_LOG_ITEM",
            DIR_LOG_INDEX => "DIR_LOG_INDEX",
            DIR_ITEM => "DIR_ITEM",
            DIR_INDEX => "DIR_INDEX",
            EXTENT_DATA => "EXTENT_DATA",
            EXTENT_CSUM => "EXTENT_CSUM",
            ROOT_ITEM => "ROOT_ITEM",
            ROOT_BACKREF => "ROOT_BACKREF",
            ROOT_REF => "ROOT_REF",
            EXTENT_ITEM => "EXTENT_ITEM",
            METADATA_ITEM => "METADATA_ITEM",
            EXTENT_OWNER_REF => "EXTENT_OWNER_REF",
            TREE_BLOCK_REF => "TREE_BLOCK_REF",
            EXTENT_DATA_REF => "EXTENT_DATA_REF",
            SHARED_BLOCK_REF => "SHARED_BLOCK_REF",
            SHARED_DATA_REF => "SHARED_DATA_REF",
            BLOCK_GROUP_ITEM => "BLOCK_GROUP_ITEM",
            FREE_SPACE_INFO => "FREE_SPACE_INFO",
            FREE_SPACE_EXTENT => "FREE_SPACE_EXTENT",
            FREE_SPACE_BITMAP => "FREE_SPACE_BITMAP",
            DEV_EXTENT => "DEV_EXTENT",
            DEV_ITEM => "DEV_ITEM",
            CHUNK_ITEM => "CHUNK_ITEM",
            RAID_STRIPE => "RAID_STRIPE",
            QGROUP_STATUS => "QGROUP_STATUS",
            QGROUP_INFO => "QGROUP_INFO",
            QGROUP_LIMIT => "QGROUP_LIMIT",
            QGROUP_RELATION => "QGROUP_RELATION",
            TEMPORARY_ITEM => "TEMPORARY_ITEM",
            PERSISTENT_ITEM => "PERSISTENT_ITEM",
            DEV_REPLACE => "DEV_REPLACE",
            UUID_SUBVOL => "UUID_SUBVOL",
            UUID_RECEIVED_SUBVOL => "UUID_RECEIVED_SUBVOL",
            STRING_ITEM => "STRING_ITEM",
            _ => return None,
        })
    }
}

pub trait TreeItem {
    const TYPE: u8;
    fn raw_size(&self) -> u32;
    unsafe fn from_le_raw(buf: &[u8]) -> Self;
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(unused)]
pub enum Compression {
    None = 0,
    Zlib,
    Lzo,
    Zstd,
}
impl Compression {
    #[inline]
    pub fn as_usize(self) -> usize {
        self as usize
    }
    #[inline]
    pub fn from_u8(n: u8) -> Self {
        assert!(n <= 3);
        // safety: the assertion checks that `n`` is in valid `Compression` range.
        unsafe { transmute(n) }
    }
    pub fn name(&self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Zlib => "zlib",
            Compression::Lzo => "lzo",
            Compression::Zstd => "zstd",
        }
    }
}
impl Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(unused)]
pub enum ExtentType {
    Inline = 0,
    Regular,
    Prealloc,
}

impl ExtentType {
    #[inline]
    pub fn from_u8(n: u8) -> Self {
        if n > 2 {
            panic!("Invalid extent type: {}", n);
        }
        // Safety: the assertion checks that `n` is in valid `ExtentType` range.
        unsafe { transmute(n) }
    }
}

// le on disk and eb
pub struct ExtentData {
    pub generation: u64,
    pub ram_bytes: u64,
    pub compression: u8,
    pub encryption: u8,
    pub other_encoding: u16,
    pub r#type: u8,
    // inline data start here
    pub disk_bytenr: u64,
    pub disk_num_bytes: u64,
    pub offset: u64,
    pub num_bytes: u64,
}

impl ExtentData {
    pub const fn inline_header_size() -> usize {
        8 + 8 + 1 + 1 + 2 + 1
    }
    pub fn is_inline(&self) -> bool {
        ExtentType::Inline == ExtentType::from_u8(self.r#type)
    }
}

impl TreeItem for ExtentData {
    const TYPE: u8 = r#type::EXTENT_DATA;
    fn raw_size(&self) -> u32 {
        8 + 8 + 1 + 1 + 2 + 1 + 8 * 4
    }
    unsafe fn from_le_raw(buf: &[u8]) -> Self {
        let mut ptr = buf.as_ptr();
        unsafe {
            let generation = ptr.cast::<u64>().read_unaligned().to_le();
            ptr = ptr.add(8);
            let ram_bytes = ptr.cast::<u64>().read_unaligned().to_le();
            ptr = ptr.add(8);
            let compression = ptr.cast::<u8>().read_unaligned().to_le();
            ptr = ptr.add(1);
            let encryption = ptr.cast::<u8>().read_unaligned().to_le();
            ptr = ptr.add(1);
            let other_encoding = ptr.cast::<u16>().read_unaligned().to_le();
            ptr = ptr.add(2);
            let r#type = ptr.cast::<u8>().read_unaligned().to_le();
            ptr = ptr.add(1);
            let disk_bytenr = ptr.cast::<u64>().read_unaligned().to_le();
            ptr = ptr.add(8);
            let disk_num_bytes = ptr.cast::<u64>().read_unaligned().to_le();
            ptr = ptr.add(8);
            let offset = ptr.cast::<u64>().read_unaligned().to_le();
            ptr = ptr.add(8);
            let num_bytes = ptr.cast::<u64>().read_unaligned().to_le();
            let ret = Self {
                generation,
                ram_bytes,
                compression,
                encryption,
                other_encoding,
                r#type,
                disk_bytenr,
                disk_num_bytes,
                offset,
                num_bytes,
            };
            assert!(buf.len() >= ret.raw_size() as usize);
            ret
        }
    }
}

#[allow(unused)]
pub struct DirItem {
    key: Key,
    transid: u64,
    data_len: u16,
    // name_len: u16,
    r#type: u8,
    name: String,
}
