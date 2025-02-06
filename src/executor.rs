use std::{future::Future, sync::LazyLock, thread::Builder};

use async_channel::{unbounded, Sender};
use async_task::{Runnable, Task};

use crate::nthreads;

pub struct Executor {
    sender: Sender<Runnable>,
}

impl Executor {
    fn new(nthreads: usize) -> Self {
        let (sender, receiver) = unbounded::<Runnable>();
        for i in 0..nthreads {
            let receiver = receiver.clone();
            Builder::new()
                .name(format!("xsz-worker{}", i))
                .spawn(move || {
                    while let Ok(r) = receiver.recv_blocking() {
                        r.run();
                    }
                })
                .unwrap();
        }
        Self { sender }
    }
    fn schedule(&self, runnable: Runnable) {
        self.sender.send_blocking(runnable).unwrap();
    }
}

pub fn global() -> &'static Executor {
    static EXECUTOR: LazyLock<Executor> = LazyLock::new(|| Executor::new(nthreads() + 2));
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
