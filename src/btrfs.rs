use std::{iter::FusedIterator, marker::PhantomData, os::fd::BorrowedFd};

use ioctl::{BTRFS_IOCTL_SEARCH_V2, SearchHeader, Sv2Args};
use rustix::{
    io::Errno,
    ioctl::{Updater, ioctl},
};

use crate::btrfs::tree::{Compression, ExtentData, ExtentType, TreeItem};

pub mod ioctl;
pub mod tree;

pub struct IoctlSearchItem<T> {
    pub(self) header: SearchHeader,
    pub(self) item: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SizeStat {
    pub disk: u64,
    pub uncomp: u64,
    pub refd: u64,
}
impl SizeStat {
    pub fn is_empty(&self) -> bool {
        self.uncomp == 0
    }
    pub fn get_percent(&self) -> u64 {
        self.disk * 100 / self.uncomp
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ExtentInfo {
    objectid: u64,
    offset: u64,
    disk_bytenr: u64,
    r#type: ExtentType,
    compression: Compression,
    stat: SizeStat,
}

impl ExtentInfo {
    pub fn objectid(&self) -> u64 {
        self.objectid
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn disk_bytenr(&self) -> u64 {
        self.disk_bytenr
    }

    pub fn r#type(&self) -> ExtentType {
        self.r#type
    }

    pub fn comp(&self) -> Compression {
        self.compression
    }

    pub fn stat(&self) -> SizeStat {
        self.stat
    }
}

impl<T: TreeItem> IoctlSearchItem<T> {
    unsafe fn from_le_raw(buf: &[u8]) -> Self {
        unsafe {
            let header = SearchHeader::from_raw(&buf[..size_of::<SearchHeader>()]);
            let item = T::from_le_raw(&buf[size_of::<SearchHeader>()..]);
            Self { header, item }
        }
    }
}

impl IoctlSearchItem<ExtentData> {
    pub fn parse(&self) -> Result<Option<ExtentInfo>, String> {
        let hlen = self.header.len;
        let ram_bytes = self.item.ram_bytes;
        let compression = Compression::from_u8(self.item.compression);
        let r#type = ExtentType::from_u8(self.item.r#type);
        let objectid = self.header.objectid;
        let offset = self.header.offset;
        if self.item.is_inline() {
            let disk_num_bytes = hlen as u64 - ExtentData::inline_header_size() as u64;
            return Ok(Some(ExtentInfo {
                objectid,
                offset,
                disk_bytenr: 0,
                r#type,
                compression,
                stat: SizeStat {
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
            stat: SizeStat {
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
impl<T: TreeItem> Iterator for Sv2ItemIter<'_, '_, T> {
    type Item = Result<IoctlSearchItem<T>, Errno>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.need_ioctl()
            && let Err(e) = self.call_ioctl()
        {
            return Some(Err(e));
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
impl<T: TreeItem> FusedIterator for Sv2ItemIter<'_, '_, T> {}
impl<'arg, 'fd, T: TreeItem> Sv2ItemIter<'arg, 'fd, T> {
    fn call_ioctl(&mut self) -> Result<(), Errno> {
        unsafe {
            let ctl = Updater::<'_, BTRFS_IOCTL_SEARCH_V2, _>::new(self.sv2_arg);
            ioctl(self.fd, ctl)?;
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
    pub fn new(sv2_arg: &'arg mut Sv2Args, fd: BorrowedFd<'fd>, objectid: u64) -> Self {
        sv2_arg.key.min_objectid = objectid;
        sv2_arg.key.max_objectid = objectid;
        sv2_arg.key.nr_items = u32::MAX;
        sv2_arg.key.min_offset = 0;
        sv2_arg.key.max_offset = u64::MAX;
        sv2_arg.key.min_type = T::TYPE as _;
        sv2_arg.key.max_type = T::TYPE as _;
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
