use std::{collections::HashSet, fmt::Display, io::Write};

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
    pub fn nfile(&self) -> u64 {
        self.nfile
    }
    pub fn nfile_mut(&mut self) -> &mut u64 {
        &mut self.nfile
    }
    pub fn nref(&self) -> u64 {
        self.nref
    }

    pub fn insert(&mut self, extent_map: &mut ExtentMap, extent: ExtentInfo) {
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

    // example compsize output format:
    // Processed 3356969 files, 653492 regular extents (2242077 refs), 2018321 inline.
    // Type       Perc     Disk Usage   Uncompressed Referenced
    // TOTAL       78%     100146085502 127182733170 481020538738
    // none       100%     88797796415  88797796415  364255758399
    // zstd        29%     11348289087  38384936755  116764780339
    pub fn fmt(&self, mut f: impl Write, scale: Scale) -> std::io::Result<()> {
        fn write_table(
            f: &mut impl Write,
            ty: impl Display,
            percentage: impl Display,
            disk_usage: impl Display,
            uncomp_usage: impl Display,
            refd_usage: impl Display,
        ) -> std::io::Result<()> {
            writeln!(
                f,
                "{:<10} {:<8} {:<12} {:<12} {:<12}",
                ty, percentage, disk_usage, uncomp_usage, refd_usage
            )
        }
        // total
        {
            let total_disk = self.prealloc.disk + self.stat.iter().map(|s| s.disk).sum::<u64>();
            let total_uncomp =
                self.prealloc.uncomp + self.stat.iter().map(|s| s.uncomp).sum::<u64>();
            let total_refd = self.prealloc.refd + self.stat.iter().map(|s| s.refd).sum::<u64>();
            if total_uncomp == 0 {
                if self.nfile() == 0 {
                    eprintln!("No Files.");
                } else {
                    eprintln!("All empty or still-delalloced files.");
                }
                return Ok(());
            }
            writeln!(
                f,
                "Processed {} files, {} regular extents ({} refs), {} inline.",
                self.nfile, self.nextent, self.nref, self.ninline
            )?;
            write_table(
                &mut f,
                "Type",
                "Perc",
                "Disk Usage",
                "Uncompressed",
                "Referenced",
            )?;

            let total_percentage = total_disk * 100 / total_uncomp;
            write_table(
                &mut f,
                "TOTAL",
                format!("{:>3}%", total_percentage),
                scale.scale(total_disk),
                scale.scale(total_uncomp),
                scale.scale(total_refd),
            )?;
        }
        let mut write_stat = |name, s: &ExtentStat| {
            if !s.is_empty() {
                write_table(
                    &mut f,
                    name,
                    format!("{:>3}%", s.get_percent()),
                    scale.scale(s.disk),
                    scale.scale(s.uncomp),
                    scale.scale(s.refd),
                )?;
            }
            Ok::<_, std::io::Error>(())
        };
        // normal
        for (i, s0) in self.stat.iter().enumerate() {
            write_stat(Compression::from_u8(i as _).name(), s0)?;
        }
        // prealloc
        write_stat("prealloc", &self.prealloc)?;
        Ok(())
    }
}
