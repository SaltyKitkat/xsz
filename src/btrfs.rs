use std::{
    fmt::Display, iter::FusedIterator, marker::PhantomData, mem::transmute, os::fd::BorrowedFd,
};

use ioctl::{BTRFS_IOCTL_SEARCH_V2, SearchHeader, Sv2Args};
use rustix::{
    io::Errno,
    ioctl::{Updater, ioctl},
};

use crate::btrfs::tree::Key;

pub mod ioctl;
pub mod tree;

pub trait FromRaw {
    fn raw_size(&self) -> u32;
    unsafe fn from_le_raw(buf: &[u8]) -> Self;
}

// le on disk and eb
pub struct FileExtent {
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

impl FileExtent {
    const fn inline_header_size() -> usize {
        8 + 8 + 1 + 1 + 2 + 1
    }
    fn is_inline(&self) -> bool {
        ExtentType::Inline == ExtentType::from_u8(self.r#type)
    }
}

impl FromRaw for FileExtent {
    fn raw_size(&self) -> u32 {
        8 + 8 + 1 + 1 + 2 + 1 + 8 * 4
    }
    unsafe fn from_le_raw(buf: &[u8]) -> Self {
        let mut ptr = buf.as_ptr() as *const u8;
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

pub struct DirItem {
    key: Key,
    transid: u64,
    data_len: u16,
    // name_len: u16,
    r#type: u8,
    name: String,
}

pub struct IoctlSearchItem<T> {
    pub(self) header: SearchHeader,
    pub(self) item: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Stat {
    pub disk: u64,
    pub uncomp: u64,
    pub refd: u64,
}
impl Stat {
    pub fn is_empty(&self) -> bool {
        self.uncomp == 0
    }
    pub fn get_percent(&self) -> u64 {
        self.disk * 100 / self.uncomp
    }
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

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(unused)]
pub enum ExtentType {
    Inline = 0,
    Regular,
    Prealloc,
}

impl ExtentType {
    pub fn from_u8(n: u8) -> Self {
        if n > 2 {
            panic!("Invalid extent type: {}", n);
        }
        // Safety: the assertion checks that `n` is in valid `ExtentType` range.
        unsafe { transmute(n) }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ExtentInfo {
    objectid: u64,
    offset: u64,
    disk_bytenr: u64,
    r#type: ExtentType,
    compression: Compression,
    stat: Stat,
}

impl ExtentInfo {
    pub fn key(&self) -> u64 {
        self.disk_bytenr
    }

    pub fn r#type(&self) -> ExtentType {
        self.r#type
    }

    pub fn comp(&self) -> Compression {
        self.compression
    }

    pub fn stat(&self) -> Stat {
        self.stat
    }
}

impl<T: FromRaw> IoctlSearchItem<T> {
    unsafe fn from_le_raw(buf: &[u8]) -> Self {
        unsafe {
            let header = SearchHeader::from_raw(&buf[..size_of::<SearchHeader>()]);
            let item = T::from_le_raw(&buf[size_of::<SearchHeader>()..]);
            Self { header, item }
        }
    }
}

impl IoctlSearchItem<FileExtent> {
    pub fn parse(&self) -> Result<Option<ExtentInfo>, String> {
        let hlen = self.header.len;
        let ram_bytes = self.item.ram_bytes;
        let compression = Compression::from_u8(self.item.compression);
        let r#type = ExtentType::from_u8(self.item.r#type);
        let objectid = self.header.objectid;
        let offset = self.header.offset;
        if self.item.is_inline() {
            let disk_num_bytes = hlen as u64 - FileExtent::inline_header_size() as u64;
            return Ok(Some(ExtentInfo {
                objectid,
                offset,
                disk_bytenr: 0,
                r#type,
                compression,
                stat: Stat {
                    disk: disk_num_bytes,
                    uncomp: ram_bytes,
                    refd: ram_bytes,
                },
            }));
        }
        if hlen != self.item.raw_size() {
            let errmsg = format!("Regular extent's header not 53 bytes ({}) long?!?", hlen);
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
        Ok(Some(ExtentInfo {
            objectid,
            offset,
            disk_bytenr,
            r#type,
            compression,
            stat: Stat {
                disk: disk_bytes,
                uncomp: ram_bytes,
                refd: refd_bytes,
            },
        }))
    }
}

#[derive(Debug)]
pub struct Sv2ItemIter<'arg, 'fd, T> {
    sv2_arg: &'arg mut Sv2Args,
    fd: BorrowedFd<'fd>,
    pos: usize,
    nrest_item: u32,
    last: bool,
    _phantom: PhantomData<T>,
}
impl<T: FromRaw> Iterator for Sv2ItemIter<'_, '_, T> {
    type Item = Result<IoctlSearchItem<T>, Errno>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.need_ioctl() {
            if let Err(e) = self.call_ioctl() {
                return Some(Err(e));
            }
        }
        if self.finish() {
            return None;
        }
        let ret = unsafe { IoctlSearchItem::from_le_raw(&self.sv2_arg.buf()[self.pos..]) };
        self.pos += size_of::<SearchHeader>() + ret.header.len as usize;
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
impl<T: FromRaw> FusedIterator for Sv2ItemIter<'_, '_, T> {}
impl<'arg, 'fd, T> Sv2ItemIter<'arg, 'fd, T> {
    fn call_ioctl(&mut self) -> Result<(), Errno> {
        unsafe {
            let ctl = Updater::<'_, BTRFS_IOCTL_SEARCH_V2, _>::new(self.sv2_arg);
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
    pub fn new(sv2_arg: &'arg mut Sv2Args, fd: BorrowedFd<'fd>) -> Self {
        sv2_arg.key.nr_items = u32::MAX;
        sv2_arg.key.min_offset = 0;
        // other fields not reset, maybe wrong?
        Self {
            sv2_arg,
            fd,
            pos: 0,
            nrest_item: 0,
            last: false,
            _phantom: PhantomData,
        }
    }
}
