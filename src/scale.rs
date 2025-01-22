use std::{
    collections::HashSet,
    fmt::{Display, Write},
};

use nohash::BuildNoHashHasher;

use crate::btrfs::{Compression, ExtentInfo, ExtentStat, ExtentType};

const UNITS: &[u8; 7] = b"BKMGTPE";
pub type ExtentMap = HashSet<u64, BuildNoHashHasher<u64>>;

pub enum Scale {
    Bytes,
    Human,
}
impl Scale {
    pub fn scale(&self, num: u64) -> String {
        match self {
            Scale::Bytes => return format!("{}B", num),
            Scale::Human => {
                let base = 1024;
                let mut num = num;
                let mut cnt = 0;
                while num > base * 10 {
                    num >>= 10;
                    cnt += 1;
                }
                if num < base {
                    return format!("{:>4}{}", num, UNITS[cnt] as char);
                } else {
                    return format!(
                        " {}.{}{}",
                        num >> 10,
                        num * 10 / 1024 % 10,
                        UNITS[cnt + 1] as char
                    );
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct CompsizeStat {
    nfile: u64,
    ninline: u64,
    nref: u64,
    nextent: u64,
    prealloc: ExtentStat,
    stat: [ExtentStat; 4],
}

impl CompsizeStat {
    pub fn display(&self, scale: Scale) -> CompsizeStatDisplay<'_> {
        CompsizeStatDisplay { stat: self, scale }
    }
    pub fn nfile(&self) -> u64 {
        self.nfile
    }
    pub fn nfile_mut(&mut self) -> &mut u64 {
        &mut self.nfile
    }
    pub fn nref(&self) -> u64 {
        self.nref
    }

    pub fn insert(
        &mut self,
        extent_map: &mut ExtentMap,
        extent: ExtentInfo,
    ) {
        let comp = extent.comp();
        let stat = extent.stat();
        match extent.r#type() {
            ExtentType::Inline => {
                self.ninline += 1;
                self.stat[comp.as_usize()].disk += stat.disk;
                self.stat[comp.as_usize()].uncomp += stat.uncomp;
                self.stat[comp.as_usize()].refd += stat.refd;
            }
            ExtentType::Regular => {
                self.nref += 1;
                if extent_map.insert(extent.key()) {
                    self.nextent += 1;
                    self.stat[comp.as_usize()].disk += stat.disk;
                    self.stat[comp.as_usize()].uncomp += stat.uncomp;
                }
                self.stat[comp.as_usize()].refd += stat.refd;
            }
            ExtentType::Prealloc => {
                self.nref += 1;
                if extent_map.insert(extent.key()) {
                    self.nextent += 1;
                    self.prealloc.disk += stat.disk;
                    self.prealloc.uncomp += stat.uncomp;
                }
                self.prealloc.refd += stat.refd;
            }
        }
    }    
}

pub struct CompsizeStatDisplay<'a> {
    stat: &'a CompsizeStat,
    scale: Scale,
}
impl Display for CompsizeStatDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { stat, scale } = self;
        writeln!(
            f,
            "Processed {} files, {} regular extents ({} refs), {} inline.",
            stat.nfile, stat.nextent, stat.nref, stat.ninline
        )?;
        // Processed 3356969 files, 653492 regular extents (2242077 refs), 2018321 inline.
        // Type       Perc     Disk Usage   Uncompressed Referenced
        // TOTAL       78%     100146085502 127182733170 481020538738
        // none       100%     88797796415  88797796415  364255758399
        // zstd        29%     11348289087  38384936755  116764780339
        fn write_table(
            f: &mut impl Write,
            ty: impl Display,
            percentage: impl Display,
            disk_usage: impl Display,
            uncomp_usage: impl Display,
            refd_usage: impl Display,
        ) -> std::fmt::Result {
            writeln!(
                f,
                "{:<10} {:<8} {:<12} {:<12} {:<12}",
                ty, percentage, disk_usage, uncomp_usage, refd_usage
            )
        }
        write_table(
            f,
            "Type",
            "Perc",
            "Disk Usage",
            "Uncompressed",
            "Referenced",
        )?;
        // total
        {
            let total_disk = stat.prealloc.disk + stat.stat.iter().map(|s| s.disk).sum::<u64>();
            let total_uncomp =
                stat.prealloc.uncomp + stat.stat.iter().map(|s| s.uncomp).sum::<u64>();
            let total_refd = stat.prealloc.refd + stat.stat.iter().map(|s| s.refd).sum::<u64>();
            let total_percentage = total_disk * 100 / total_uncomp; // bug: div by 0
            // if total_disk == 0 {
            //     if stat.nfile == 0 {
            //         return Err(())
            //     }
            // }
            write_table(
                f,
                "TOTAL",
                format!("{:>3}%", total_percentage),
                scale.scale(total_disk),
                scale.scale(total_uncomp),
                scale.scale(total_refd),
            )?;
        }
        // normal
        for (i, s0) in stat.stat.iter().enumerate() {
            if s0.is_empty() {
                continue;
            }
            write_table(
                f,
                Compression::from_usize(i).name(),
                format!("{:>3}%", s0.get_percent()),
                scale.scale(s0.disk),
                scale.scale(s0.uncomp),
                scale.scale(s0.refd),
            )?;
        }
        // prealloc
        if !stat.prealloc.is_empty() {
            write_table(
                f,
                "Prealloc",
                format!("{:3.0}%", stat.prealloc.get_percent()),
                scale.scale(stat.prealloc.disk),
                scale.scale(stat.prealloc.uncomp),
                scale.scale(stat.prealloc.refd),
            )?;
        }
        Ok(())
    }
}
