use std::os::fd::BorrowedFd;

use rustix::ioctl::{opcode::read_write, Opcode};

use super::{tree::BtrfsKeyType, Sv2ItemIter};

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
    fn new(st_ino: u64) -> Self {
        Self {
            tree_id: 0,
            min_objectid: st_ino,
            max_objectid: st_ino,
            min_offset: 0,
            max_offset: u64::MAX,
            min_transid: 0,
            max_transid: u64::MAX,
            min_type: BtrfsKeyType::ExtentData as _,
            max_type: BtrfsKeyType::ExtentData as _,
            nr_items: u32::MAX,
            unused: 0,
            unused1: 0,
            unused2: 0,
            unused3: 0,
            unused4: 0,
        }
    }
    fn init(&mut self, st_ino: u64) {
        self.tree_id = 0;
        self.min_objectid = st_ino;
        self.max_objectid = st_ino;
        self.min_offset = 0;
        self.max_offset = u64::MAX;
        self.min_transid = 0;
        self.max_transid = u64::MAX;
        self.min_type = BtrfsKeyType::ExtentData as _;
        self.max_type = BtrfsKeyType::ExtentData as _;
        self.nr_items = u32::MAX;
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
    pub fn new() -> Self {
        Self {
            key: IoctlSearchKey::new(0),
            buf_size: 65536,
            buf: [0; 65536],
        }
    }

    fn set_key(&mut self, ino: u64) {
        self.key.init(ino);
    }

    pub fn search_file<'fd>(&mut self, fd: BorrowedFd<'fd>, ino: u64) -> Sv2ItemIter<'_, 'fd> {
        self.set_key(ino);
        Sv2ItemIter::new(self, fd)
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
