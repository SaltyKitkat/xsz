use std::{
    future::{pending, Future},
    pin::pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll, Wake, Waker},
    thread::{current, park, Builder, Thread},
};

use async_channel::{unbounded, Receiver, Sender};
use async_task::{Runnable, Task};
use futures_lite::FutureExt;

use crate::global::config;

pub struct Executor {
    sender: Sender<Runnable>,
    receiver: Receiver<Runnable>,
}

impl Executor {
    fn new(nthreads: usize) -> Self {
        let (sender, receiver) = unbounded::<Runnable>();
        for i in 0..nthreads {
            let receiver = receiver.clone();
            if let Err(e) = Builder::new()
                .name(format!("xsz-worker{}", i))
                .stack_size(16 * 1024)
                .spawn(move || {
                    while let Ok(r) = receiver.recv_blocking() {
                        r.run();
                    }
                })
            {
                eprintln!("Failed to spawn worker thread: {}", e);
            }
        }
        Self { sender, receiver }
    }
    fn schedule(&self, runnable: Runnable) {
        self.sender.send_blocking(runnable).unwrap();
    }
}

pub fn global() -> &'static Executor {
    static EXECUTOR: LazyLock<Executor> = LazyLock::new(|| Executor::new(config().jobs));
    &EXECUTOR
}

pub fn spawn<F>(fut: F) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let schedule = |runnable| global().schedule(runnable);
    let (runnable, task) = async_task::spawn(fut, schedule);
    runnable.schedule();
    task
}

pub fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let recv = global().receiver.clone();
    let f = fut.or(async move {
        if let Ok(r) = recv.recv().await {
            r.run();
        }
        pending().await
    });
    let mut f = pin!(f);
    let thread = current();
    struct T(Thread);
    impl Wake for T {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(T(thread)));
    let mut cx = Context::from_waker(&waker);
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(r) => return r,
            Poll::Pending => park(),
        }
    }
}
