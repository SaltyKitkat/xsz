use std::{
    io,
    path::PathBuf,
};

use crate::{
    actor::{spawn_actor, Actor, Addr},
    TaskPak, Worker,
};

pub struct WalkDir {
    root_fs: (),
    file_sender: Addr<Worker>,
    dir: PathBuf,
    dir_list: Vec<PathBuf>,
    new_dir_list: Vec<PathBuf>,
}

impl Actor for WalkDir {
    type Message = ();
    type Ret = ();

    fn on_start(
        &mut self,
        ctx: &mut crate::actor::Context<Self>,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        Self: Sized,
    {
        async {}
    }

    fn handle(
        &mut self,
        ctx: &mut crate::actor::Context<Self>,
        msg: Self::Message,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        Self: Sized,
    {
        async {}
    }

    fn on_exit(
        &mut self,
        ctx: &mut crate::actor::Context<Self>,
    ) -> impl std::future::Future<Output = Self::Ret> + Send
    where
        Self: Sized,
    {
        async {
            async {
                let path = &self.dir;
                if path.symlink_metadata().map_or(false, |f| f.is_dir()) {
                    let mut pak = TaskPak::new(self.file_sender.clone());
                    for entry in path.read_dir()? {
                        let entry = entry?;
                        let metadata = entry.metadata()?;
                        let path = entry.path();
                        if metadata.is_dir() {
                            spawn_actor(WalkDir::new(self.file_sender.clone(), path));
                        } else if metadata.is_file() {
                            pak.push(path.into_boxed_path()).await;
                        } else {
                            drop(path);
                        }
                    }
                } else {
                    todo!()
                }
                Ok::<_, io::Error>(())
            }
            .await
            .ok();
        }
    }
}

impl WalkDir {
    pub fn new(file_sender: Addr<Worker>, path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self {
            root_fs: (),
            file_sender,
            dir: path.clone(),
            dir_list: vec![path],
            new_dir_list: vec![],
        }
    }
}
