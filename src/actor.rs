use async_channel::Receiver;

pub trait Actor {
    type Message;
    async fn handle(&mut self, msg: Self::Message);
    async fn run(mut self, receiver: Receiver<Self::Message>)
    where
        Self: Sized,
    {
        while let Ok(msg) = receiver.recv().await {
            self.handle(msg).await;
        }
    }
}
