use std::{
    sync::{Arc, Weak},
    thread::spawn,
};

use crossbeam::channel::{unbounded, Receiver, Sender};

pub trait Actor {
    type Message: Send + 'static;
    type Ret: Send + 'static;
    fn on_start(&mut self, ctx: &mut Context<Self>)
    where
        Self: Sized,
    {
    }
    fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Message)
    where
        Self: Sized;
    fn on_exit(&mut self, ctx: &mut Context<Self>) -> Self::Ret
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
    pub fn send(&self, msg: A::Message) -> Result<(), A::Message> {
        self.sender.send(msg).map_err(|e| e.0)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ActorState {
    Running,
    ToStop,
}

pub struct Context<A: Actor> {
    addr: Weak<Sender<A::Message>>,
    state: ActorState,
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
        run_one_actor(actor, move |ret| {
            ret_sender.send(ret.into()).ok();
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
            ret_sender.send(ret.into()).ok();
        })
    }
}

fn run_actor<A: Actor>(
    addr: Addr<A>,
    mut actor: A,
    receiver: Receiver<A::Message>,
) -> <A as Actor>::Ret {
    let mut ctx = Context {
        addr: Arc::downgrade(&addr.sender),
        state: ActorState::Running,
    };
    actor.on_start(&mut ctx);
    // drop the sender here, prevent dead lock lead to actor not stop
    drop(addr);
    for msg in receiver.into_iter() {
        actor.handle(&mut ctx, msg);
        if ctx.state == ActorState::ToStop {
            break;
        }
    }
    actor.on_exit(&mut ctx)
}

fn run_one_actor<A, F>(actor: A, ret_handler: F) -> Addr<A>
where
    A: Actor + Send + 'static,
    F: FnOnce(A::Ret) + Send + 'static,
{
    let (sender, receiver) = unbounded();
    let addr = Addr {
        sender: Arc::new(sender),
    };
    {
        let addr = addr.clone();
        spawn(move || {
            let ret = run_actor(addr, actor, receiver);
            ret_handler(ret);
        });
    }
    addr
}

fn run_n_actor<F, A, FRet>(n: usize, f: F, f_ret: FRet) -> Addr<A>
where
    F: Fn(usize) -> A + Send + Sync + 'static,
    A: Actor + 'static,
    FRet: Fn(A::Ret) + Send + Sync + 'static,
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
            spawn(move || {
                let actor = (f.0)(i);
                let ret = run_actor(addr, actor, receiver);
                (f.1)(ret);
            });
        }
    });
    addr
}

pub fn block_on<A: Actor>(actor: A) -> A::Ret {
    let (sender, receiver) = unbounded();
    let addr = Addr {
        sender: Arc::new(sender),
    };
    run_actor(addr, actor, receiver)
}

pub fn spawn_actor<A>(actor: A) -> Addr<A>
where
    A: Actor + Send + 'static,
{
    run_one_actor(actor, |_| {})
}

pub fn spawn_n_actor<F, A>(n: usize, f: F) -> Addr<A>
where
    F: Fn(usize) -> A + Send + Sync + 'static,
    A: Actor + 'static,
{
    run_n_actor(n, f, |_| {})
}
