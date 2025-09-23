use kanal::AsyncSender as Sender;

use crate::{
    actor::Actor,
    btrfs::{
        ioctl::{IoctlSearchKey, Sv2Args}, tree, ExtentInfo, Sv2ItemIter
    },
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
            sv2_args: Sv2Args::from_sk(IoctlSearchKey::new(
                0,
                0,
                0,
                0,
                u64::MAX,
                0,
                u64::MAX,
                tree::r#type::EXTENT_DATA,
                tree::r#type::EXTENT_DATA,
            )),
        }
    }

    pub(crate) async fn handle_file(&mut self, f: File_) -> Result<(), ()> {
        self.nfile += 1;
        self.sv2_args.key.min_objectid = f.ino();
        self.sv2_args.key.max_objectid = f.ino();
        let iter = Sv2ItemIter::new(&mut self.sv2_args, f.borrow_fd());
        for extent in iter {
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
        }
        Ok(())
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
