use kanal::AsyncSender as Sender;

use crate::{
    actor::Actor,
    btrfs::{ExtentInfo, Sv2Args},
    collector::CollectorMsg,
    fs_util::File_,
    global::{get_err, set_err},
    spawn,
    taskpak::TaskPak,
};

pub(crate) struct Worker {
    collector: TaskPak<ExtentInfo, CollectorMsg>,
    nfile: u64,
    sv2_args: Sv2Args,
}

impl Worker {
    pub(crate) fn new(collector: Sender<CollectorMsg>) -> Self {
        Self {
            collector: TaskPak::new(collector),
            nfile: 0,
            sv2_args: Sv2Args::new(),
        }
    }

    pub(crate) async fn handle_file(&mut self, f: File_) -> Result<(), ()> {
        self.nfile += 1;
        let iter = self.sv2_args.search_file(f.borrow_fd(), f.ino());
        Ok(for extent in iter {
            let extent = match extent {
                Ok(extent) => extent,
                Err(e) => {
                    set_err()?;
                    if e.raw_os_error() == 25 {
                        eprintln!(
                            "{}: Not btrfs (or SEARCH_V2 unsupported)",
                            f.path().display()
                        );
                    } else {
                        eprintln!("{}: SEARCH_V2: {}", f.path().display(), e);
                    }
                    break;
                }
            };
            match extent.parse() {
                Ok(Some(extent)) => {
                    self.collector.push(extent).await;
                }
                Err(e) => {
                    set_err()?;
                    eprintln!("{}", e);
                    break;
                }
                _ => (),
            }
        })
    }
}

impl Actor for Worker {
    type Message = Box<[File_]>;
    async fn handle(&mut self, files: Self::Message) -> Result<(), ()> {
        for f in files {
            get_err()?;
            self.handle_file(f).await?;
        }
        Ok(())
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let sender = self.collector.sender().clone();
        let nfile = self.nfile;
        spawn(async move {
            sender.send(nfile.into()).await.ok();
        });
    }
}
