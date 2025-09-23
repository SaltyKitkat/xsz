use rustix::ioctl::{opcode::read_write, Opcode};

pub const BTRFS_IOCTL_MAGIC: u8 = 0x94;
pub const BTRFS_IOCTL_SEARCH_V2: Opcode = read_write::<Sv2Args>(BTRFS_IOCTL_MAGIC, 17);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct IoctlSearchKey {
    pub tree_id: u64,
    pub min_objectid: u64,
    pub max_objectid: u64,
    pub min_offset: u64,
    pub max_offset: u64,
    pub min_transid: u64,
    pub max_transid: u64,
    pub min_type: u32,
    pub max_type: u32,
    pub nr_items: u32,
    unused: u32,
    unused1: u64,
    unused2: u64,
    unused3: u64,
    unused4: u64,
}

impl IoctlSearchKey {
    pub fn new(
        tree_id: u64,
        min_objectid: u64,
        max_objectid: u64,
        min_offset: u64,
        max_offset: u64,
        min_transid: u64,
        max_transid: u64,
        min_type: u8,
        max_type: u8,
    ) -> Self {
        Self {
            tree_id,
            min_objectid,
            max_objectid,
            min_offset,
            max_offset,
            min_transid,
            max_transid,
            min_type: min_type as _,
            max_type: max_type as _,
            nr_items: u32::MAX,
            unused: 0,
            unused1: 0,
            unused2: 0,
            unused3: 0,
            unused4: 0,
        }
    }
}

// should be reused for different files
#[derive(Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Sv2Args {
    pub key: IoctlSearchKey,
    buf_size: u64,
    buf: [u8; 65536],
}

impl Sv2Args {
    pub fn from_sk(sk: IoctlSearchKey) -> Self {
        Self {
            key: sk,
            buf_size: 65536,
            buf: [0; 65536],
        }
    }

    pub fn buf(&self) -> &[u8; 65536] {
        &self.buf
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct SearchHeader {
    pub transid: u64,
    pub objectid: u64,
    pub offset: u64,
    pub r#type: u32,
    pub len: u32,
}
impl SearchHeader {
    pub unsafe fn from_raw(buf: &[u8]) -> Self {
        buf.as_ptr().cast::<Self>().read()
    }
}
