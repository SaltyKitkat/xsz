use std::{
    future::Future,
    io::{self, ErrorKind::NotADirectory},
    num::NonZero,
    path::{Path, PathBuf},
};

use crate::{
    actor::{Actor, Addr, Context},
    spawn, TaskPak, Worker,
};

pub struct WalkDir {
    root_fs: (),
    nwalker: usize,
    file_sender: Addr<Worker>,
    dir_list: Vec<Box<Path>>,
    __keep_alive: Option<Addr<Self>>,
}

impl Actor for WalkDir {
    type Message = Box<[Box<Path>]>;
    type Ret = ();

    fn on_start(&mut self, ctx: &mut Context<Self>) -> impl Future<Output = ()> + Send
    where
        Self: Sized,
    {
        async {
            let addr = ctx.addr().unwrap();
            for _ in 0..self.nwalker {
                if let Some(path) = self.dir_list.pop() {
                    self.spawn_walker(&addr, path);
                } else {
                    break;
                }
            }
            self.__keep_alive = Some(addr)
        }
    }

    fn handle(
        &mut self,
        ctx: &mut Context<Self>,
        msg: Self::Message,
    ) -> impl Future<Output = ()> + Send
    where
        Self: Sized,
    {
        async {
            self.dir_list.extend(msg);
            for _ in self.current_walker()..self.nwalker {
                if let Some(path) = self.dir_list.pop() {
                    let addr = self
                        .__keep_alive
                        .as_ref()
                        .expect("we should have addr here");
                    self.spawn_walker(addr, path);
                } else {
                    break;
                }
            }
            let cw = self.current_walker();
            if self.dir_list.is_empty() && cw == 0 && self.__keep_alive.as_ref().unwrap().is_empty()
            {
                self.__keep_alive = None;
            }
        }
    }

    fn on_exit(&mut self, ctx: &mut Context<Self>) -> impl Future<Output = Self::Ret> + Send
    where
        Self: Sized,
    {
        async {}
    }
}

impl WalkDir {
    pub fn new(
        file_sender: Addr<Worker>,
        path: impl Into<PathBuf>,
        nwalker: NonZero<usize>,
    ) -> Result<Self, io::Error> {
        let path = path.into();
        if !path.symlink_metadata().map_or(false, |f| f.is_dir()) {
            return Err(io::Error::new(NotADirectory, path.display().to_string()));
        }

        Ok(Self {
            root_fs: (),
            nwalker: nwalker.into(),
            file_sender,
            dir_list: vec![path.into()],
            __keep_alive: None,
        })
    }

    fn spawn_walker(&self, addr: &Addr<WalkDir>, path: Box<Path>) {
        let mut dir_pak = TaskPak::new(addr.clone());
        let mut file_pak = TaskPak::new(self.file_sender.clone());
        spawn(async move {
            for entry in path.read_dir()? {
                let entry = entry?;
                let fty = entry.file_type()?;
                let path = entry.path().into_boxed_path();
                if fty.is_dir() {
                    dir_pak.push(path).await;
                } else if fty.is_file() {
                    file_pak.push(path).await;
                }
            }
            Ok::<_, io::Error>(())
        });
    }

    fn current_walker(&self) -> usize {
        self.__keep_alive
            .as_ref()
            .map(|addr| addr.ref_count() - 1) // minus the one hold by self.__keep_alive
            .unwrap_or_default()
    }
}
