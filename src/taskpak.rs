use std::mem::{replace, take};

use kanal::AsyncSender as Sender;

use crate::spawn;

pub struct TaskPak<T, M>
where
    M: Send + 'static,
    Box<[T]>: Into<M>,
{
    pub(crate) inner: Vec<T>,
    pub(crate) sender: Sender<M>,
}

impl<T, M> TaskPak<T, M>
where
    M: Send + 'static,
    Box<[T]>: Into<M>,
{
    const SIZE: usize = 1024 * 8 / size_of::<T>();
    pub fn new(sender: Sender<M>) -> Self {
        Self {
            inner: Vec::with_capacity(Self::SIZE),
            sender,
        }
    }
    pub async fn push(&mut self, item: T) {
        self.inner.push(item);
        if self.is_full() {
            self.sender
                .send(
                    replace(&mut self.inner, Vec::with_capacity(Self::SIZE))
                        .into_boxed_slice()
                        .into(),
                )
                .await
                .ok();
        }
    }

    pub fn sender(&self) -> &Sender<M> {
        &self.sender
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub(crate) fn is_full(&self) -> bool {
        self.inner.len() >= Self::SIZE
    }
}

impl<T, M> Drop for TaskPak<T, M>
where
    M: Send + 'static,
    Box<[T]>: Into<M>,
{
    fn drop(&mut self) {
        if !self.is_empty() {
            let handler = self.sender.clone();
            let item = take(&mut self.inner).into_boxed_slice().into();
            spawn(async move {
                handler.send(item).await.ok();
            });
        }
    }
}
