use std::mem::take;

use kanal::AsyncSender as Sender;

use crate::spawn;

pub struct TaskPak<T: Send + 'static> {
    pub(crate) inner: Vec<T>,
    pub(crate) sender: Sender<Box<[T]>>,
}

impl<T: Send + 'static> TaskPak<T> {
    const SIZE: usize = 1024 * 16 / size_of::<T>();
    pub fn new(sender: Sender<Box<[T]>>) -> Self {
        Self {
            inner: Vec::with_capacity(Self::SIZE),
            sender,
        }
    }
    pub async fn push(&mut self, item: T) {
        self.inner.push(item);
        if self.is_full() {
            let mut tmp = Vec::with_capacity(Self::SIZE);
            tmp.extend(self.inner.drain(..));
            self.sender.send(tmp.into_boxed_slice().into()).await.ok();
        }
    }

    pub fn sender(&self) -> &Sender<Box<[T]>> {
        &self.sender
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub(crate) fn is_full(&self) -> bool {
        self.inner.len() >= Self::SIZE
    }
}

impl<T: Send + 'static> Drop for TaskPak<T> {
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
