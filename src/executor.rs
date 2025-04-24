use std::{
    future::Future,
    pin::pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll, Wake, Waker},
    thread::{current, park, Builder, Thread},
};

use async_task::{Runnable, Task};
use futures_lite::{future::yield_now, FutureExt};
use kanal::{unbounded, Receiver, Sender};

use crate::global::config;

pub struct Executor {
    sender: Sender<Runnable>,
    receiver: Receiver<Runnable>,
}

impl Executor {
    fn new(nthreads: u8) -> Self {
        let (sender, receiver) = unbounded::<Runnable>();
        for i in 0..nthreads {
            let receiver = receiver.clone();
            if let Err(e) = Builder::new()
                .name(format!("xsz-worker{}", i))
                .stack_size(4 * 1024)
                .spawn(move || {
                    while let Ok(r) = receiver.recv() {
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
        self.sender.send(runnable).unwrap();
    }
}

pub fn global() -> &'static Executor {
    // jobs - 1 because the main thread is also a worker thread when calling block_on
    static EXECUTOR: LazyLock<Executor> = LazyLock::new(|| Executor::new(config().jobs - 1));
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
    F: Future,
{
    let recv = global().receiver.clone();
    let f = fut.or(async move {
        loop {
            match recv.as_async().recv().await {
                Ok(r) => {
                    r.run();
                }
                Err(e) => eprintln!("{}", e),
            }
        }
    });
    let mut f = pin!(f);
    let thread = current();
    struct ThreadWaker(Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(ThreadWaker(thread)));
    let mut cx = Context::from_waker(&waker);
    let mut cnt = 0;
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(r) => return r,
            Poll::Pending => {
                cnt += 1;
                // park the thread when there's no enough work to do
                // prevent it from spinning and use a lot of cpu
                // 32 is just some random number
                // this is not that correct, but it can work
                if cnt >= 32 {
                    cnt = 0;
                    park();
                }
            }
        }
    }
}
