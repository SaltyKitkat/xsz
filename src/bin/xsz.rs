use std::{
    fmt::Display,
    io::{Write, stdout},
    num::NonZeroU64,
    path::PathBuf,
    process::exit,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use kanal::bounded_async as bounded;
use mimalloc::MiMalloc;
use nohash::IntSet;
use xsz::{
    actor::{Actor, Runnable, Sink},
    btrfs::{
        ExtentInfo, SizeStat,
        tree::{Compression, ExtentType},
    },
    executor::block_on,
    fs_util::File_,
    global::{config, get_err},
    scan_tree,
    spawn,
    taskpak::TaskPak,
    walkdir::WalkDir,
    worker::Worker,
};

#[derive(Clone, Copy)]
pub enum Scale {
    Bytes,
    Human,
}
impl Scale {
    pub fn scale(&self, num: u64) -> String {
        const UNITS: &[u8; 7] = b"BKMGTPE";

        match self {
            Scale::Bytes => format!("{}", num),
            Scale::Human => {
                let base = 1024;
                let mut cnt = 0;
                while num >= base << (cnt * 10) {
                    cnt += 1;
                }
                let bits = cnt * 10;
                let integer = num >> bits;
                let tail = num & ((1 << bits) - 1);
                let real_v = (num as f64) / (1u64 << bits) as f64;
                if tail == 0 || integer >= 10 {
                    return format!("{:.0}{}", real_v, UNITS[cnt] as char);
                }
                format!("{:.1}{}", real_v, UNITS[cnt] as char)
            }
        }
    }
}

trait ExtentInfoSink {
    fn duplic(&mut self, extent: &ExtentInfo);
    fn unique(&mut self, extent: &ExtentInfo);
    fn fmt(&self, f: &mut dyn Write, use_bytes: bool) -> std::io::Result<()>;
}

#[derive(Debug)]
struct FragStat {
    min: u64,
    max: u64,
    count: u64,
    sum: u64,
    bins: [u64; Self::FRAG_BINS],
}

impl FragStat {
    const FRAG_BINS: usize = 16;
    fn new() -> Self {
        Self {
            min: u64::MAX,
            max: u64::MIN,
            count: 0,
            sum: 0,
            bins: [0; Self::FRAG_BINS],
        }
    }
    fn record(&mut self, len: u64) {
        if len == 0 {
            return;
        }
        self.min = self.min.min(len);
        self.max = self.max.max(len);
        self.count += 1;
        self.sum += len;
        let idx = if len < 4096 {
            0
        } else {
            let bit = 63 - len.leading_zeros() as usize;
            (bit.saturating_sub(12)).min(Self::FRAG_BINS - 1)
        };
        self.bins[idx] += 1;
    }

    fn avg(&self) -> u64 {
        self.sum.checked_div(self.count).unwrap_or(0)
    }

    fn fmt(&self, f: &mut dyn Write) -> std::io::Result<()> {
        let scale = Scale::Human;
        struct BinLabel {
            lo: u64,
            hi: Option<NonZeroU64>,
        }

        impl Display for BinLabel {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let s = Scale::Human;
                match self.hi {
                    Some(hi) => write!(f, "{:>4}- {:>4}", s.scale(self.lo), s.scale(hi.into())),
                    None => write!(f, "    >={:>4}", s.scale(self.lo)),
                }
            }
        }

        let bin_label = |idx| {
            if idx == 0 {
                BinLabel {
                    lo: 0,
                    hi: NonZeroU64::new(4096),
                }
            } else {
                let lo = 1u64 << (idx + 12);
                if idx < Self::FRAG_BINS - 1 {
                    BinLabel {
                        lo,
                        hi: NonZeroU64::new(lo << 1),
                    }
                } else {
                    BinLabel { lo, hi: None }
                }
            }
        };

        writeln!(
            f,
            "  Count: {}, Min: {}, Max: {}, Avg: {}",
            self.count,
            scale.scale(self.min),
            scale.scale(self.max),
            scale.scale(self.avg()),
        )?;
        if self.count == 0 {
            return Ok(());
        }
        writeln!(f, "  Distribution:")?;
        for (i, &cnt) in self.bins.iter().enumerate() {
            if cnt > 0 {
                let pct = cnt * 1000 / self.count;
                let label = bin_label(i);
                writeln!(
                    f,
                    "    {}: {:>6} ({:>2}.{}%)",
                    label,
                    cnt,
                    pct / 10,
                    pct % 10
                )?;
            }
        }
        Ok(())
    }
}

struct XFragStat {
    refd: FragStat,
}

impl XFragStat {
    fn new() -> Self {
        Self {
            refd: FragStat::new(),
        }
    }
}

impl ExtentInfoSink for XFragStat {
    fn duplic(&mut self, extent: &ExtentInfo) {
        self.unique(extent);
    }

    fn unique(&mut self, extent: &ExtentInfo) {
        self.refd.record(extent.stat().uncomp);
    }

    fn fmt(&self, f: &mut dyn Write, _: bool) -> std::io::Result<()> {
        writeln!(f, "File extent size distribution:")?;
        self.refd.fmt(f)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct CompsizeStat {
    prealloc: SizeStat,
    stat: [SizeStat; 4],
}

impl ExtentInfoSink for CompsizeStat {
    fn duplic(&mut self, extent: &ExtentInfo) {
        let comp = extent.comp();
        let stat = extent.stat();
        match extent.r#type() {
            ExtentType::Inline => {
                unreachable!()
            }
            ExtentType::Regular => {
                self.stat[comp.as_usize()].refd += stat.refd;
            }
            ExtentType::Prealloc => {
                self.prealloc.refd += stat.refd;
            }
        }
    }

    fn unique(&mut self, extent: &ExtentInfo) {
        let comp = extent.comp();
        let stat = extent.stat();
        match extent.r#type() {
            ExtentType::Inline => {
                self.stat[comp.as_usize()].disk += stat.disk;
                self.stat[comp.as_usize()].uncomp += stat.uncomp;
                self.stat[comp.as_usize()].refd += stat.refd;
            }
            ExtentType::Regular => {
                self.stat[comp.as_usize()].disk += stat.disk;
                self.stat[comp.as_usize()].uncomp += stat.uncomp;
                self.stat[comp.as_usize()].refd += stat.refd;
            }
            ExtentType::Prealloc => {
                self.prealloc.disk += stat.disk;
                self.prealloc.uncomp += stat.uncomp;
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
    fn fmt(&self, f: &mut dyn Write, use_bytes: bool) -> std::io::Result<()> {
        let scale = if use_bytes {
            Scale::Bytes
        } else {
            Scale::Human
        };
        // total
        self.write_total(f, scale)?;
        // normal
        let mut write_stat = |name, s: &SizeStat| {
            if !s.is_empty() {
                write_table(
                    f,
                    name,
                    format!("{:>3}%", s.get_percent()),
                    scale.scale(s.disk),
                    scale.scale(s.uncomp),
                    scale.scale(s.refd),
                )?;
            }
            Ok::<_, std::io::Error>(())
        };
        for (i, s0) in self.stat.iter().enumerate() {
            write_stat(Compression::from_u8(i as _).name(), s0)?;
        }
        // prealloc
        write_stat("prealloc", &self.prealloc)?;
        Ok(())
    }
}

impl CompsizeStat {
    fn write_total(&self, f: &mut dyn Write, scale: Scale) -> Result<(), std::io::Error> {
        let total_disk = self.prealloc.disk + self.stat.iter().map(|s| s.disk).sum::<u64>();
        let total_uncomp = self.prealloc.uncomp + self.stat.iter().map(|s| s.uncomp).sum::<u64>();
        let total_refd = self.prealloc.refd + self.stat.iter().map(|s| s.refd).sum::<u64>();
        write_table(
            f,
            "Type",
            "Perc",
            "Disk Usage",
            "Uncompressed",
            "Referenced",
        )?;
        let total_percentage = total_disk * 100 / total_uncomp;
        write_table(
            f,
            "TOTAL",
            format!("{:>3}%", total_percentage),
            scale.scale(total_disk),
            scale.scale(total_uncomp),
            scale.scale(total_refd),
        )?;
        Ok(())
    }
}

fn write_table(
    f: &mut dyn Write,
    ty: impl Display,
    percentage: impl Display,
    disk_usage: impl Display,
    uncomp_usage: impl Display,
    refd_usage: impl Display,
) -> std::io::Result<()> {
    writeln!(
        f,
        "{:<10} {:>4} {:>14} {:>16} {:>16}",
        ty, percentage, disk_usage, uncomp_usage, refd_usage
    )
}

pub struct Collector {
    stat: Box<dyn ExtentInfoSink>,
    nextent: u64,
    ninline: u64,
    extent_set: IntSet<u64>,
}

impl Collector {
    pub fn new() -> Self {
        let stat: Box<dyn ExtentInfoSink> = if config().frag {
            Box::new(XFragStat::new())
        } else {
            Box::new(CompsizeStat::default())
        };
        Self {
            stat,
            nextent: 0,
            ninline: 0,
            extent_set: Default::default(),
        }
    }
    pub fn nextent_unique(&self) -> u64 {
        self.extent_set.len() as _
    }
    pub fn nextent(&self) -> u64 {
        self.nextent
    }
    pub fn ninline(&self) -> u64 {
        self.ninline
    }
    pub fn fmt(&self, f: &mut dyn Write, nfile: u64) -> std::io::Result<()> {
        if nfile == 0 {
            eprintln!("No Files.");
            return Ok(());
        }
        if self.nextent == 0 {
            eprintln!("All empty or still-delalloced files.");
            return Ok(());
        }
        writeln!(
            f,
            "Processed {} files, {} regular extents ({} refs), {} inline.",
            nfile,
            self.nextent_unique(),
            self.nextent - self.ninline,
            self.ninline,
        )?;
        self.stat.fmt(f, config().bytes)
    }
}

impl Actor for Collector {
    type Message = Box<[ExtentInfo]>;

    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
        get_err()?;
        for extent in msg {
            self.nextent += 1;
            let bytenr = extent.disk_bytenr();
            if bytenr == 0 {
                self.ninline += 1;
                self.stat.unique(&extent);
            } else if self.extent_set.insert(bytenr) {
                self.stat.unique(&extent);
            } else {
                self.stat.duplic(&extent);
            }
        }
        Ok(())
    }
}

struct F {
    taskpak: TaskPak<File_>,
    global_nfile: Arc<AtomicU64>,
    local_nfile: u64,
}
impl Sink for F {
    type Item = File_;
    fn consume(&mut self, f: File_) -> impl Future + Send {
        self.local_nfile += 1;
        self.taskpak.push(f)
    }
}
impl Drop for F {
    fn drop(&mut self) {
        self.global_nfile
            .fetch_add(self.local_nfile as _, Ordering::Relaxed);
    }
}

struct S(TaskPak<ExtentInfo>);
impl Sink for S {
    type Item = ExtentInfo;
    fn consume(&mut self, f: ExtentInfo) -> impl Future + Send {
        self.0.push(f)
    }
}

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let nworkers = config().jobs;
    let (sender, r) = bounded(nworkers as usize);
    let collector = Collector::new();
    let nfile = Arc::new(AtomicU64::new(0));

    if config().tree_scan {
        let path = PathBuf::from(&config().args[0]);
        let nfile_clone = nfile.clone();
        spawn(async move {
            let sink = S(TaskPak::new(sender));
            match scan_tree::scan_subvol(sink, &path).await {
                Ok(cnt) => nfile_clone.store(cnt, Ordering::Relaxed),
                Err(()) => {}
            }
        });
    } else {
        let (worker_tx, worker_rx) = bounded(nworkers as usize);
        let fcb = {
            let nfile = nfile.clone();
            move || F {
                taskpak: TaskPak::new(worker_tx.clone()),
                global_nfile: nfile.clone(),
                local_nfile: 0,
            }
        };
        WalkDir::spawn(fcb, &config().args, nworkers);
        for _ in 0..nworkers {
            let sender = sender.clone();
            let worker = Worker::new(S(TaskPak::new(sender)));
            spawn(worker.run(worker_rx.clone()));
        }
        drop(sender);
    }

    let collector = block_on(collector.run(r));
    if get_err().is_err() {
        exit(1)
    }
    collector
        .fmt(&mut stdout(), nfile.load(Ordering::Relaxed))
        .unwrap();
}
