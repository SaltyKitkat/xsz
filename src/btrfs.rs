use std::{
    fmt::{Debug, Display},
    iter::FusedIterator,
    mem::transmute,
    os::fd::OwnedFd,
};

use rustix::{
    io::Errno,
    ioctl::{ioctl, ReadWriteOpcode, Updater},
};

pub const BTRFS_IOCTL_MAGIC: u8 = 0x94;
pub const BTRFS_EXTENT_DATA_KEY: u32 = 108;
pub const BTRFS_FILE_EXTENT_INLINE: u8 = 0;
pub const BTRFS_FILE_EXTENT_REG: u8 = 1;
pub const BTRFS_FILE_EXTENT_PREALLOC: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ExtentStat {
    pub disk: u64,
    pub uncomp: u64,
    pub refd: u64,
}
impl ExtentStat {
    pub fn is_empty(&self) -> bool {
        self.uncomp == 0
    }
    pub fn get_percent(&self) -> u64 {
        self.disk * 100 / self.uncomp
    }
}

// le on disk
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct IoctlSearchHeader {
    transid: u64,
    objectid: u64,
    offset: u64,
    r#type: u32,
    len: u32,
}
impl IoctlSearchHeader {
    unsafe fn from_le_raw(buf: &[u8]) -> Self {
        let raw = &*(buf.as_ptr() as *const IoctlSearchHeader);
        Self {
            transid: u64::from_le(raw.transid),
            objectid: u64::from_le(raw.objectid),
            offset: u64::from_le(raw.offset),
            r#type: u32::from_le(raw.r#type),
            len: u32::from_le(raw.len),
        }
    }
}

// le on disk
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(packed)]
pub struct FileExtentItem {
    pub generation: u64,
    pub ram_bytes: u64,
    pub compression: u8,
    pub encryption: u8,
    pub other_encoding: u16,
    pub r#type: u8,
    // following u64 * 4 for regular extent, or inline data for inline extent
    pub disk_bytenr: u64,
    pub disk_num_bytes: u64,
    pub offset: u64,
    pub num_bytes: u64,
}
const EXTENT_INLINE_HEADER_SIZE: usize = 21;
impl FileExtentItem {
    unsafe fn from_le_raw(buf: &[u8]) -> Self {
        let raw = &*(buf.as_ptr() as *const FileExtentItem);
        Self {
            generation: u64::from_le(raw.generation),
            ram_bytes: u64::from_le(raw.ram_bytes),
            compression: u8::from_le(raw.compression),
            encryption: u8::from_le(raw.encryption),
            other_encoding: u16::from_le(raw.other_encoding),
            r#type: u8::from_le(raw.r#type),
            disk_bytenr: u64::from_le(raw.disk_bytenr),
            disk_num_bytes: u64::from_le(raw.disk_num_bytes),
            offset: u64::from_le(raw.offset),
            num_bytes: u64::from_le(raw.num_bytes),
        }
    }
}

#[repr(packed)]
pub struct IoctlSearchItem {
    pub(self) header: IoctlSearchHeader,
    pub(self) item: FileExtentItem,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None = 0,
    Zlib,
    Lzo,
    Zstd,
}
impl Compression {
    pub fn as_usize(self) -> usize {
        self as usize
    }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExtentType {
    Inline,
    Regular,
    Prealloc,
}

impl ExtentType {
    pub fn from_u8(n: u8) -> Self {
        match n {
            BTRFS_FILE_EXTENT_INLINE => Self::Inline,
            BTRFS_FILE_EXTENT_REG => Self::Regular,
            BTRFS_FILE_EXTENT_PREALLOC => Self::Prealloc,
            _ => panic!("Invalid extent type: {}", n),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ExtentInfo {
    key: u64,
    r#type: ExtentType,
    comp: Compression,
    stat: ExtentStat,
}

impl ExtentInfo {
    pub fn new(key: u64, r#type: ExtentType, comp: Compression, stat: ExtentStat) -> Self {
        Self {
            key,
            r#type,
            comp,
            stat,
        }
    }

    pub fn key(&self) -> u64 {
        self.key
    }

    pub fn r#type(&self) -> ExtentType {
        self.r#type
    }

    pub fn comp(&self) -> Compression {
        self.comp
    }

    pub fn stat(&self) -> ExtentStat {
        self.stat
    }
}

impl IoctlSearchItem {
    unsafe fn from_le_raw(buf: &[u8]) -> Self {
        let header = IoctlSearchHeader::from_le_raw(&buf[..size_of::<IoctlSearchHeader>()]);
        let item = FileExtentItem::from_le_raw(&buf[size_of::<IoctlSearchHeader>()..]);
        Self { header, item }
    }
    pub fn parse(&self) -> Result<Option<ExtentInfo>, String> {
        let hlen = self.header.len;
        let uncomp_bytes = self.item.ram_bytes;
        let comp_type = Compression::from_u8(self.item.compression);
        let extent_type = ExtentType::from_u8(self.item.r#type);
        if extent_type == ExtentType::Inline {
            let disk_num_bytes = hlen as u64 - EXTENT_INLINE_HEADER_SIZE as u64;
            return Ok(Some(ExtentInfo::new(
                0,
                extent_type,
                comp_type,
                ExtentStat {
                    disk: disk_num_bytes,
                    uncomp: uncomp_bytes,
                    refd: uncomp_bytes,
                },
            )));
        }
        if hlen != size_of::<FileExtentItem>() as u32 {
            let errmsg = format!("Regular extent's header not 53 bytes ({}) long?!?", hlen,);
            return Err(errmsg);
        }
        let disk_bytenr = self.item.disk_bytenr;
        // is hole
        if disk_bytenr == 0 {
            return Ok(None);
        }
        // check 4k alignment
        if disk_bytenr & 0xfff != 0 {
            let errmsg = format!("Extent not 4k aligned at ({:#x})", disk_bytenr);
            return Err(errmsg);
        }

        let disk_bytenr = disk_bytenr >> 12;
        let disk_bytes = self.item.disk_num_bytes;
        let refd_bytes = self.item.num_bytes;
        Ok(Some(ExtentInfo::new(
            disk_bytenr,
            extent_type,
            comp_type,
            ExtentStat {
                disk: disk_bytes,
                uncomp: uncomp_bytes,
                refd: refd_bytes,
            },
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct IoctlSearchKey {
    tree_id: u64,
    min_objectid: u64,
    max_objectid: u64,
    min_offset: u64,
    max_offset: u64,
    min_transid: u64,
    max_transid: u64,
    min_type: u32,
    max_type: u32,
    nr_items: u32,
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
            min_type: BTRFS_EXTENT_DATA_KEY,
            max_type: BTRFS_EXTENT_DATA_KEY,
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
        self.min_type = BTRFS_EXTENT_DATA_KEY;
        self.max_type = BTRFS_EXTENT_DATA_KEY;
        self.nr_items = u32::MAX;
    }
}

// should be reused for different files
#[derive(Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Sv2Args {
    key: IoctlSearchKey,
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

    pub fn search_file(&mut self, fd: OwnedFd, ino: u64) -> Sv2ItemIter {
        self.set_key(ino);
        Sv2ItemIter::new(self, fd)
    }
}
#[derive(Debug)]
pub struct Sv2ItemIter<'arg> {
    sv2_arg: &'arg mut Sv2Args,
    fd: OwnedFd,
    pos: usize,
    nrest_item: u32,
    last: bool,
}
impl Iterator for Sv2ItemIter<'_> {
    type Item = Result<IoctlSearchItem, Errno>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.need_ioctl() {
            if let Err(e) = self.call_ioctl() {
                return Some(Err(e));
            }
        }
        if self.finish() {
            return None;
        }
        let ret = unsafe { IoctlSearchItem::from_le_raw(&self.sv2_arg.buf[self.pos..]) };
        self.pos += size_of::<IoctlSearchHeader>() + ret.header.len as usize;
        self.nrest_item -= 1;
        if self.need_ioctl() {
            self.sv2_arg.key.min_offset = ret.header.offset + 1;
            self.sv2_arg.key.nr_items = u32::MAX;
        }
        Some(Ok(ret))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.nrest_item as usize,
            if self.last {
                Some(self.nrest_item as usize)
            } else {
                None
            },
        )
    }
}
impl FusedIterator for Sv2ItemIter<'_> {}
impl<'arg> Sv2ItemIter<'arg> {
    fn call_ioctl(&mut self) -> Result<(), Errno> {
        unsafe {
            let ctl = Updater::<'_, ReadWriteOpcode<BTRFS_IOCTL_MAGIC, 17, Sv2Args>, _>::new(
                self.sv2_arg,
            );
            ioctl(&self.fd, ctl)?;
        }
        self.nrest_item = self.sv2_arg.key.nr_items;
        self.last = self.nrest_item <= 512;
        self.pos = 0;
        Ok(())
    }
    fn need_ioctl(&self) -> bool {
        self.nrest_item == 0 && !self.last
    }
    fn finish(&self) -> bool {
        self.nrest_item == 0 && self.last
    }
    pub fn new(sv2_arg: &'arg mut Sv2Args, fd: OwnedFd) -> Self {
        sv2_arg.key.nr_items = u32::MAX;
        sv2_arg.key.min_offset = 0;
        // other fields not reset, maybe wrong?
        Self {
            sv2_arg,
            fd,
            pos: 0,
            nrest_item: 0,
            last: false,
        }
    }
}
