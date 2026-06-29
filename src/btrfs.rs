use std::{hint::cold_path, iter::FusedIterator, marker::PhantomData, os::fd::BorrowedFd};

use ioctl::{BTRFS_IOCTL_SEARCH_V2, SearchHeader, Sv2Args};
use rustix::{
    io::Errno,
    ioctl::{Updater, ioctl},
};

use crate::btrfs::tree::{Compression, ExtentData, ExtentType, TreeItem};

pub mod ioctl;
pub mod tree;

pub struct IoctlSearchItem<T> {
    pub(crate) header: SearchHeader,
    pub(crate) item: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SizeStat {
    pub disk: u64,
    pub uncomp: u64,
    pub refd: u64,
}
impl SizeStat {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.uncomp == 0
    }
    #[inline]
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
            cold_path();
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
            cold_path();
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

/// Advance a btrfs search key `(objectid, type, offset)` by one position.
/// Wraps offset → type → objectid when fields overflow.
#[inline]
fn advance_key(objectid: &mut u64, r#type: &mut u32, offset: &mut u64) {
    if *offset == u64::MAX {
        cold_path();
        *offset = 0;
        if *r#type == u32::MAX {
            cold_path();
            *r#type = 0;
            *objectid = objectid.saturating_add(1);
        } else {
            *r#type += 1;
        }
    } else {
        *offset += 1;
    }
}

#[derive(Debug)]
pub struct Sv2Wrapper {
    sv2_arg: Box<Sv2Args>,
    pos: usize,
    nrest_item: u32,
    last: bool,
}

impl Sv2Wrapper {
    fn call_ioctl(&mut self, fd: BorrowedFd) -> Result<(), Errno> {
        unsafe {
            let ctl = Updater::<'_, BTRFS_IOCTL_SEARCH_V2, _>::new(&mut *self.sv2_arg);
            ioctl(fd, ctl)?;
        }
        self.nrest_item = self.sv2_arg.key.nr_items;
        self.last = self.nrest_item <= 4;
        self.pos = 0;
        Ok(())
    }
    #[inline]
    fn need_ioctl(&self) -> bool {
        self.nrest_item == 0 && !self.last
    }
    #[inline]
    fn finish(&self) -> bool {
        self.nrest_item == 0 && self.last
    }

    pub(crate) fn next(&mut self, fd: BorrowedFd) -> Option<Result<(SearchHeader, &[u8]), Errno>> {
        if self.need_ioctl()
            && let Err(e) = self.call_ioctl(fd)
        {
            return Some(Err(e));
        }
        if self.finish() {
            return None;
        }
        let buf_len = self.sv2_arg.buf().len();
        assert!(self.pos + size_of::<SearchHeader>() <= buf_len);
        let header = unsafe { SearchHeader::from_raw(&self.sv2_arg.buf()[self.pos..]) };
        let item_start = self.pos + size_of::<SearchHeader>();
        let item_end = item_start + header.len as usize;
        assert!(item_end <= buf_len);
        self.pos = item_end;
        self.nrest_item -= 1;
        // Check AFTER decrement so the last item in a batch triggers key advancement.
        // Must update key BEFORE taking the buf slice to avoid borrow-conflict with sv2_arg.
        if self.need_ioctl() {
            self.sv2_arg.key.min_objectid = header.objectid;
            self.sv2_arg.key.min_type = header.r#type;
            self.sv2_arg.key.min_offset = header.offset;
            advance_key(
                &mut self.sv2_arg.key.min_objectid,
                &mut self.sv2_arg.key.min_type,
                &mut self.sv2_arg.key.min_offset,
            );
            self.sv2_arg.key.nr_items = u32::MAX;
        }
        let buf: &[u8] = &self.sv2_arg.buf()[item_start..item_end];
        Some(Ok((header, buf)))
    }

    pub fn new(sv2_arg: Box<Sv2Args>) -> Self {
        Self {
            sv2_arg,
            pos: 0,
            nrest_item: 0,
            last: false,
        }
    }
    pub fn reset(&mut self) {
        self.pos = 0;
        self.nrest_item = 0;
        self.last = false;
        // Ask for the maximum batch on the next ioctl, in case a previous
        // partial batch left a smaller nr_items behind.
        self.sv2_arg.key.nr_items = u32::MAX;
    }
}

#[derive(Debug)]
pub struct Sv2ItemIter<'inner, 'fd, T> {
    inner: &'inner mut Sv2Wrapper,
    fd: BorrowedFd<'fd>,
    _phantom: PhantomData<T>,
}
impl<T: TreeItem> Iterator for Sv2ItemIter<'_, '_, T> {
    type Item = Result<IoctlSearchItem<T>, Errno>;

    fn next(&mut self) -> Option<Self::Item> {
        let (header, buf) = match self.inner.next(self.fd)? {
            Ok((header, buf)) => (header, buf),
            Err(e) => return Some(Err(e)),
        };
        let item = unsafe { T::from_le_raw(buf) };
        let ret = IoctlSearchItem { header, item };
        Some(Ok(ret))
    }
}
impl<T: TreeItem> FusedIterator for Sv2ItemIter<'_, '_, T> {}
impl<'inner, 'fd, T: TreeItem> Sv2ItemIter<'inner, 'fd, T> {
    pub fn new(sv2: &'inner mut Sv2Wrapper, fd: BorrowedFd<'fd>, objectid: u64) -> Self {
        sv2.sv2_arg.key.min_objectid = objectid;
        sv2.sv2_arg.key.max_objectid = objectid;
        sv2.sv2_arg.key.nr_items = u32::MAX;
        sv2.sv2_arg.key.min_offset = 0;
        sv2.sv2_arg.key.max_offset = u64::MAX;
        sv2.sv2_arg.key.min_type = T::TYPE as _;
        sv2.sv2_arg.key.max_type = T::TYPE as _;
        sv2.reset();
        // tree_id, min_transid, max_transid, and the unused fields are
        // initialized once in Worker::new() and never change across files.
        Self {
            inner: sv2,
            fd,
            _phantom: PhantomData,
        }
    }
}
