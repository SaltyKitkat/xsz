use std::{
    future::Future,
    marker::Send,
    sync::{Arc, Weak},
};

use async_channel::{unbounded, Receiver, Sender};
use smol::spawn;

pub trait Actor {
    type Message: Send + 'static;
    type Ret: Send + 'static;
    fn on_start(&mut self, ctx: &mut Context<Self>) -> impl Future<Output = ()> + Send
    where
        Self: Sized,
    {
        async {}
    }
    fn handle(
        &mut self,
        ctx: &mut Context<Self>,
        msg: Self::Message,
    ) -> impl Future<Output = ()> + Send
    where
        Self: Sized;
    fn on_exit(&mut self, ctx: &mut Context<Self>) -> impl Future<Output = Self::Ret> + Send
    where
        Self: Sized;
}

pub struct Addr<A: Actor> {
    sender: Arc<Sender<A::Message>>,
}

impl<A: Actor> Clone for Addr<A> {
    fn clone(&self) -> Self {
        Addr {
            sender: Arc::clone(&self.sender),
        }
    }
}

impl<A: Actor> Addr<A> {
    pub async fn send(&self, msg: A::Message) -> Result<(), A::Message> {
        self.sender.send(msg).await.map_err(|e| e.0)
    }
    pub fn send_blocking(&self, msg: A::Message) -> Result<(), A::Message> {
        self.sender.send_blocking(msg).map_err(|e| e.0)
    }
}

pub struct Context<A: Actor> {
    addr: Weak<Sender<A::Message>>,
}

pub struct OnExit<A: Actor>(A::Ret);

impl<A: Actor + 'static> Context<A> {
    pub fn addr(&self) -> Option<Addr<A>> {
        self.addr.upgrade().map(|sender| Addr { sender })
    }
    pub fn spawn<Act>(&self, actor: Act) -> Addr<Act>
    where
        Act: Actor + Send + 'static,
        Act::Ret: Into<A::Message>,
    {
        let ret_sender = self.addr().unwrap();
        run_one_actor(actor, move |ret| async move {
            ret_sender.send(ret.into()).await.ok();
        })
    }
    pub fn spawn_n<Act>(
        &self,
        n: usize,
        f: impl Fn(usize) -> Act + Send + Sync + 'static,
    ) -> Addr<Act>
    where
        Act: Actor + Send + 'static,
        Act::Ret: Into<A::Message>,
    {
        let ret_sender = self.addr().unwrap();
        run_n_actor(n, f, move |ret| {
            let ret_sender = ret_sender.clone();
            async move {
                ret_sender.send(ret.into()).await.ok();
            }
        })
    }
}

async fn run_actor<A: Actor>(
    addr: Addr<A>,
    mut actor: A,
    receiver: Receiver<A::Message>,
) -> <A as Actor>::Ret {
    let mut ctx = Context {
        addr: Arc::downgrade(&addr.sender),
    };
    actor.on_start(&mut ctx).await;
    // drop the sender here, prevent dead lock lead to actor not stop
    drop(addr);
    while let Ok(msg) = receiver.recv().await {
        actor.handle(&mut ctx, msg).await;
    }
    actor.on_exit(&mut ctx).await
}

fn run_one_actor<A, F, Fut>(actor: A, ret_handler: F) -> Addr<A>
where
    A: Actor + Send + 'static,
    F: FnOnce(A::Ret) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let (sender, receiver) = unbounded();
    let addr = Addr {
        sender: Arc::new(sender),
    };
    {
        let addr = addr.clone();
        spawn(async move {
            let ret = run_actor(addr, actor, receiver).await;
            ret_handler(ret).await;
        })
        .detach();
    }
    addr
}

fn run_n_actor<F, A, FRet, Fut>(n: usize, f: F, f_ret: FRet) -> Addr<A>
where
    F: Fn(usize) -> A + Send + Sync + 'static,
    A: Actor + Send + 'static,
    FRet: Fn(A::Ret) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    let (sender, receiver) = unbounded();
    let addr = Addr {
        sender: Arc::new(sender),
    };
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
            })
            .detach();
        }
    });
    addr
}

pub async fn block_on<A: Actor>(actor: A) -> A::Ret {
    let (sender, receiver) = unbounded();
    let addr = Addr {
        sender: Arc::new(sender),
    };
    run_actor(addr, actor, receiver).await
}

pub fn spawn_actor<A>(actor: A) -> Addr<A>
where
    A: Actor + Send + 'static,
{
    run_one_actor(actor, |_| async {})
}

pub fn spawn_n_actor<F, A>(n: usize, f: F) -> Addr<A>
where
    F: Fn(usize) -> A + Send + Sync + 'static,
    A: Actor + Send + 'static,
{
    run_n_actor(n, f, |_| async {})
}
