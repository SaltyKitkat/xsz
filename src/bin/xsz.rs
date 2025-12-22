use std::{
    io::stdout,
    process::exit,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use kanal::bounded_async as bounded;
use mimalloc::MiMalloc;
use xsz::{
    actor::Runnable,
    btrfs::ExtentInfo,
    executor::block_on,
    fs_util::File_,
    global::{config, get_err},
    spawn,
    taskpak::TaskPak,
    walkdir::{self, WalkDir},
    worker::{self, Worker},
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

struct F {
    taskpak: TaskPak<File_>,
    global_nfile: Arc<AtomicU64>,
    local_nfile: u16,
}
impl walkdir::Sink for F {
    fn consume(&mut self, f: File_) -> impl Future + Send {
        self.local_nfile += 1;
        if self.local_nfile > 16 * 1024 {
            self.global_nfile
                .fetch_add(self.local_nfile as _, Ordering::Relaxed);
            self.local_nfile = 0;
        }
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
impl worker::Sink for S {
    fn consume(&mut self, f: ExtentInfo) -> impl Future + Send {
        self.0.push(f)
    }
}

fn main() {
    let nworkers = config().jobs;
    let (worker_tx, worker_rx) = bounded(nworkers as usize);
    let (sender, r) = bounded(nworkers as usize);
    let collector = collector::Collector::new();
    let nfile = collector.stat.nfile_ref().clone();
    let fcb = {
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
    let collector = block_on(collector.run(r));
    if get_err().is_err() {
        exit(1)
    }
    collector.stat.fmt(stdout(), config().bytes).unwrap();
}

mod collector {

    use xsz::{actor::Actor, btrfs::ExtentInfo, global::get_err};

    use crate::scale::CompsizeStat;

    pub struct Collector {
        pub(crate) stat: CompsizeStat,
    }

    impl Collector {
        pub fn new() -> Self {
            Self {
                stat: CompsizeStat::default(),
            }
        }
    }

    impl Actor for Collector {
        type Message = Box<[ExtentInfo]>;

        async fn handle(&mut self, msg: Self::Message) -> Result<(), ()> {
            get_err()?;
            for extent in msg {
                self.stat.insert(&extent);
            }
            Ok(())
        }
    }
}

mod scale {
    use std::{
        fmt::Display,
        io::Write,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use nohash::IntSet;
    use xsz::btrfs::{Compression, ExtentInfo, ExtentType, Stat};

    const UNITS: &[u8; 7] = b"BKMGTPE";

    #[derive(Clone, Copy)]
    pub enum Scale {
        Bytes,
        Human,
    }
    impl Scale {
        pub fn scale(&self, num: u64) -> String {
            match self {
                Scale::Bytes => return format!("{}", num),
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
        nfile: Arc<AtomicU64>,
        ninline: u64,
        nref: u64,
        prealloc: Stat,
        stat: [Stat; 4],
        extent_map: IntSet<u64>,
    }

    impl CompsizeStat {
        pub fn nfile(&self) -> u64 {
            self.nfile.load(Ordering::Relaxed)
        }
        pub fn nfile_ref(&self) -> &Arc<AtomicU64> {
            &self.nfile
        }
        pub fn nref(&self) -> u64 {
            self.nref
        }
        pub fn ninline(&self) -> u64 {
            self.ninline
        }
        pub fn nextent(&self) -> u64 {
            self.extent_map.len() as _
        }

        pub fn insert(&mut self, extent: &ExtentInfo) {
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
                    if self.extent_map.insert(extent.disk_bytenr()) {
                        self.stat[comp.as_usize()].disk += stat.disk;
                        self.stat[comp.as_usize()].uncomp += stat.uncomp;
                    }
                    self.stat[comp.as_usize()].refd += stat.refd;
                }
                ExtentType::Prealloc => {
                    self.nref += 1;
                    if self.extent_map.insert(extent.disk_bytenr()) {
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
        pub fn fmt(&self, mut f: impl Write, use_bytes: bool) -> std::io::Result<()> {
            let scale = if use_bytes {
                Scale::Bytes
            } else {
                Scale::Human
            };
            // total
            self.write_total(&mut f, scale)?;
            // normal
            let mut write_stat = |name, s: &Stat| {
                if !s.is_empty() {
                    write_table(
                        &mut f,
                        &name,
                        &format!("{:>3}%", s.get_percent()),
                        &scale.scale(s.disk),
                        &scale.scale(s.uncomp),
                        &scale.scale(s.refd),
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

        fn write_total(&self, mut f: impl Write, scale: Scale) -> Result<(), std::io::Error> {
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
                &mut f,
                "Processed {} files, {} regular extents ({} refs), {} inline.",
                self.nfile(),
                self.nextent(),
                self.nref(),
                self.ninline(),
            )?;
            write_table(
                &mut f,
                &"Type",
                &"Perc",
                &"Disk Usage",
                &"Uncompressed",
                &"Referenced",
            )?;
            let total_percentage = total_disk * 100 / total_uncomp;
            write_table(
                &mut f,
                &"TOTAL",
                &format!("{:>3}%", total_percentage),
                &scale.scale(total_disk),
                &scale.scale(total_uncomp),
                &scale.scale(total_refd),
            )?;
            Ok(())
        }
    }
    fn write_table(
        mut f: impl Write,
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
}
