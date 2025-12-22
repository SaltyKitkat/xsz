#![allow(async_fn_in_trait)]
use kanal::AsyncReceiver as Receiver;

pub trait Actor {
    type Message;
    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()>;
}

pub trait Runnable: Actor {
    async fn run(mut self, receiver: Receiver<Self::Message>) -> Self
    where
        Self: Sized,
    {
        while let Ok(msg) = receiver.recv().await {
            let ret = self.handle(msg).await;
            if ret.is_err() {
                break;
            }
        }
        self
    }
}

impl<T: Actor> Runnable for T {}

pub trait Sink {
    type Item;
    fn consume(&mut self, f: Self::Item) -> impl Future + Send;
}
