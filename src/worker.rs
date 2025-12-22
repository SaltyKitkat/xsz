use crate::{
    actor::{Actor, Sink},
    btrfs::{
        ExtentInfo, Sv2ItemIter,
        ioctl::{IoctlSearchKey, Sv2Args},
        tree,
    },
    fs_util::File_,
    global::{get_err, set_err},
};

pub struct Worker<S> {
    sink: S,
    sv2_args: Box<Sv2Args>,
}

impl<S: Sink<Item = ExtentInfo>> Worker<S> {
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            sv2_args: Box::new(Sv2Args::from_sk(IoctlSearchKey::new(
                0,
                0,
                0,
                0,
                u64::MAX,
                0,
                u64::MAX,
                tree::r#type::EXTENT_DATA,
                tree::r#type::EXTENT_DATA,
            ))),
        }
    }

    pub(crate) async fn handle_file(&mut self, f: File_) -> Result<(), ()> {
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
                    self.sink.consume(extent).await;
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

impl<S: Sink<Item = ExtentInfo>> Actor for Worker<S> {
    type Message = Box<[File_]>;
    async fn handle(&mut self, files: Self::Message) -> Result<(), ()> {
        for f in files {
            get_err()?;
            self.handle_file(f).await?;
        }
        Ok(())
    }
}
