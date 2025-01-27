use std::{future::Future, marker::Send, sync::Arc};

use async_channel::{Receiver, Sender};

use crate::spawn;

fn channel<T>() -> (Sender<T>, Receiver<T>) {
    async_channel::bounded(8)
}

pub trait Actor {
    type Message: Send + 'static;
    type Ret: Send + 'static;
    fn on_start(&mut self, ctx: &mut Context<Self::Message>) -> impl Future<Output = ()> + Send
    where
        Self: Sized,
    {
        async {
            ctx.take_addr().unwrap();
        }
    }
    fn handle(
        &mut self,
        ctx: &mut Context<Self::Message>,
        msg: Self::Message,
    ) -> impl Future<Output = ()> + Send
    where
        Self: Sized;
    fn on_exit(
        &mut self,
        ctx: &mut Context<Self::Message>,
    ) -> impl Future<Output = Self::Ret> + Send
    where
        Self: Sized;
}

pub struct Addr<M> {
    sender: Sender<M>,
}

impl<M> Clone for Addr<M> {
    fn clone(&self) -> Self {
        Addr {
            sender: self.sender.clone(),
        }
    }
}

impl<M> Addr<M> {
    pub async fn send(&self, msg: M) -> Result<(), M> {
        self.sender.send(msg).await.map_err(|e| e.0)
    }
}

pub struct Context<M> {
    addr: Option<Addr<M>>,
}

// pub struct OnExit<A: Actor>(A::Ret);

impl<M: Send + 'static> Context<M> {
    pub fn addr(&self) -> Option<&Addr<M>> {
        self.addr.as_ref()
    }
    pub fn take_addr(&mut self) -> Option<Addr<M>> {
        self.addr.take()
    }
    pub fn spawn<Act>(&self, actor: Act) -> Addr<Act::Message>
    where
        Act: Actor + Send + 'static,
        Act::Ret: Into<M>,
    {
        let ret_sender = self.addr().unwrap().clone();
        run_one_actor(actor, move |ret| async move {
            ret_sender.send(ret.into()).await.ok();
        })
    }
    pub fn spawn_n<Act>(
        &self,
        n: usize,
        f: impl Fn(usize) -> Act + Send + Sync + 'static,
    ) -> Addr<Act::Message>
    where
        Act: Actor + Send + 'static,
        Act::Ret: Into<M>,
    {
        let ret_sender = self.addr().unwrap().clone();
        run_n_actor(n, f, move |ret| {
            let ret_sender = ret_sender.clone();
            async move {
                ret_sender.send(ret.into()).await.ok();
            }
        })
    }
}

async fn run_actor<A: Actor>(
    addr: Addr<A::Message>,
    mut actor: A,
    receiver: Receiver<A::Message>,
) -> <A as Actor>::Ret {
    let mut ctx = Context { addr: Some(addr) };
    actor.on_start(&mut ctx).await;
    while let Ok(msg) = receiver.recv().await {
        actor.handle(&mut ctx, msg).await;
    }
    actor.on_exit(&mut ctx).await
}

fn run_one_actor<A, F, Fut>(actor: A, ret_handler: F) -> Addr<A::Message>
where
    A: Actor + Send + 'static,
    F: FnOnce(A::Ret) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let (sender, receiver) = channel();
    let addr = Addr { sender };
    {
        let addr = addr.clone();
        spawn(async move {
            let ret = run_actor(addr, actor, receiver).await;
            ret_handler(ret).await;
        });
    }
    addr
}

fn run_n_actor<F, A, FRet, Fut>(n: usize, f: F, f_ret: FRet) -> Addr<A::Message>
where
    F: Fn(usize) -> A + Send + Sync + 'static,
    A: Actor + Send + 'static,
    FRet: Fn(A::Ret) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    let (sender, receiver) = channel();
    let addr = Addr { sender };
    let f = Arc::new((f, f_ret));
    (0..n).for_each(|i| {
        let f = f.clone();
        {
            let addr = addr.clone();
            let receiver = receiver.clone();
            spawn(async move {
                let actor = (f.0)(i);
                let ret = run_actor(addr, actor, receiver).await;
                (f.1)(ret).await;
            });
        }
    });
    addr
}

pub async fn block_on<A: Actor>(actor: A) -> A::Ret {
    let (sender, receiver) = channel();
    let addr = Addr { sender };
    run_actor(addr, actor, receiver).await
}

pub fn spawn_actor<A>(actor: A) -> Addr<A::Message>
where
    A: Actor + Send + 'static,
{
    run_one_actor(actor, |_| async {})
}

pub fn spawn_n_actor<F, A>(n: usize, f: F) -> Addr<A::Message>
where
    F: Fn(usize) -> A + Send + Sync + 'static,
    A: Actor + Send + 'static,
{
    run_n_actor(n, f, |_| async {})
}
