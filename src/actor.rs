use async_channel::Receiver;

pub trait Actor {
    type Message;
    async fn handle(&mut self, msg: Self::Message) -> Result<(), ()>;
}

pub trait Runnable: Actor {
    async fn run(mut self, receiver: Receiver<Self::Message>)
    where
        Self: Sized,
    {
        while let Ok(msg) = receiver.recv().await {
            let ret = self.handle(msg).await;
            if ret.is_err() {
                break;
            }
        }
    }
}

impl<T: Actor> Runnable for T {}
