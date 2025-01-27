use std::{
    collections::VecDeque,
    future::Future,
    io,
    path::{Path, PathBuf},
};

use crate::{
    actor::{spawn_actor, Actor, Addr, Context},
    spawn,
};

const MAX_LOCAL_LEN: usize = 4096 / size_of::<Box<Path>>();

pub trait FileConsumer {
    fn consume(
        &mut self,
        path: Box<Path>,
    ) -> impl std::future::Future<Output = ()> + std::marker::Send;
}

pub struct WalkDir<F> {
    root_fs: (),
    nwalker: usize,
    file_consumer: F,
    pending_walkers: Vec<WalkerAddr>,
    global_dirlist: Vec<Box<Path>>,
}

impl<F, FC> WalkDir<F>
where
    F: FnMut() -> FC + Send + 'static,
    FC: FileConsumer + Send + 'static,
{
    pub fn new(
        mut file_consumer: F,
        path: impl IntoIterator<Item = impl Into<PathBuf>>,
        nwalker: usize,
    ) -> Result<Self, io::Error> {
        assert_ne!(nwalker, 0);
        let mut files = vec![];
        let dirs = path
            .into_iter()
            .map(|p| p.into().into_boxed_path())
            .filter_map(|p| {
                if p.is_file() {
                    files.push(p);
                    None
                } else {
                    Some(p)
                }
            })
            .collect();
        let mut cb = file_consumer();
        spawn(async move {
            for p in files {
                cb.consume(p).await;
            }
        });

        Ok(Self {
            root_fs: (),
            nwalker,
            file_consumer,
            pending_walkers: vec![],
            global_dirlist: dirs,
        })
    }
}

pub enum WalkDirMsg {
    PushJobs(Vec<Box<Path>>),
    RequireJobs(WalkerAddr),
}

pub type WalkDirAddr = Addr<WalkDirMsg>;

impl<F, FC> Actor for WalkDir<F>
where
    F: FnMut() -> FC + Send + 'static,
    FC: FileConsumer + Send + 'static,
{
    type Message = WalkDirMsg;
    type Ret = ();

    fn on_start(&mut self, ctx: &mut Context<Self::Message>) -> impl Future<Output = ()> + Send
    where
        Self: Sized,
    {
        let addr = ctx.take_addr().unwrap();
        for _ in 0..self.nwalker {
            let addr = addr.clone();
            let file_handler = (self.file_consumer)();
            spawn_actor(Walker::new(addr, file_handler));
        }
        async {}
    }

    fn handle(
        &mut self,
        _ctx: &mut Context<Self::Message>,
        msg: Self::Message,
    ) -> impl Future<Output = ()> + Send
    where
        Self: Sized,
    {
        async {
            match msg {
                WalkDirMsg::PushJobs(vec) => {
                    self.global_dirlist.extend(vec);
                }
                WalkDirMsg::RequireJobs(addr) => {
                    self.pending_walkers.push(addr);
                }
            }
            while !self.global_dirlist.is_empty() {
                if let Some(addr) = self.pending_walkers.pop() {
                    let l = self.global_dirlist.len().saturating_sub(MAX_LOCAL_LEN / 2);
                    let dirs = self.global_dirlist.drain(l..).collect();
                    let addr2 = addr.clone();
                    addr2.send(WalkerMsg { dirs, addr }).await.ok();
                } else {
                    break;
                }
            }
            if self.pending_walkers.len() == self.nwalker {
                self.pending_walkers.clear();
            }
        }
    }

    fn on_exit(
        &mut self,
        _ctx: &mut Context<Self::Message>,
    ) -> impl Future<Output = Self::Ret> + Send
    where
        Self: Sized,
    {
        async {}
    }
}

struct Walker<F> {
    master: Addr<WalkDirMsg>,
    file_handler: F,
    local_dirlist: Vec<Box<Path>>,
}
impl<F> Walker<F> {
    fn new(addr: Addr<WalkDirMsg>, file_handler: F) -> Self {
        Self {
            master: addr,
            file_handler,
            local_dirlist: vec![],
        }
    }
}

type WalkerAddr = Addr<WalkerMsg>;
struct WalkerMsg {
    dirs: VecDeque<Box<Path>>,
    addr: Addr<Self>,
}

impl<F> Actor for Walker<F>
where
    F: FileConsumer + Send,
{
    type Message = WalkerMsg;

    type Ret = ();

    fn on_start(&mut self, ctx: &mut Context<Self::Message>) -> impl Future<Output = ()> + Send {
        async {
            let addr = ctx.take_addr().unwrap();
            self.master.send(WalkDirMsg::RequireJobs(addr)).await.ok();
        }
    }

    fn handle(
        &mut self,
        _ctx: &mut Context<Self::Message>,
        msg: Self::Message,
    ) -> impl Future<Output = ()> + Send {
        async {
            let WalkerMsg { dirs, addr } = msg;
            self.local_dirlist.extend(dirs);
            while let Some(dir) = self.local_dirlist.pop() {
                for entry in dir.read_dir().unwrap() {
                    let entry = entry.unwrap();
                    let fty = entry.file_type().unwrap();
                    let path = entry.path().into_boxed_path();
                    if fty.is_dir() {
                        self.local_dirlist.push(path);
                    } else if fty.is_file() {
                        self.file_handler.consume(path).await;
                    }
                }
                if self.local_dirlist.len() > MAX_LOCAL_LEN {
                    let r = self.local_dirlist.len() - MAX_LOCAL_LEN / 2;
                    let v: Vec<_> = self.local_dirlist.drain(0..r).collect();
                    self.master.send(WalkDirMsg::PushJobs(v)).await.ok();
                }
            }
            self.master.send(WalkDirMsg::RequireJobs(addr)).await.ok();
        }
    }

    fn on_exit(
        &mut self,
        _ctx: &mut Context<Self::Message>,
    ) -> impl Future<Output = Self::Ret> + Send {
        async {}
    }
}
