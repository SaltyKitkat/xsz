use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;
use replace_with::replace_with_or_abort_and_return;

enum Inner<T> {
    Idle,
    Sent(T),
    ReceiverWaiting(Waker),
    Closed,
}

impl<T> Inner<T> {
    fn put_msg(&mut self, msg: T) -> Result<(), T> {
        replace_with_or_abort_and_return(self, |old| match old {
            Inner::Idle => (Ok(()), Inner::Sent(msg)),
            Inner::Sent(_) => unreachable!("send to a oneshot channel more than once!"),
            Inner::ReceiverWaiting(waker) => {
                waker.wake();
                (Ok(()), Inner::Sent(msg))
            }
            Inner::Closed => (Err(msg), Inner::Closed),
        })
    }
    fn get_msg(&mut self, waker: &Waker) -> Result<Option<T>, ()> {
        replace_with_or_abort_and_return(self, |old| match old {
            Inner::Idle => (Ok(None), Inner::ReceiverWaiting(waker.clone())),
            Inner::Sent(msg) => (Ok(Some(msg)), Inner::Closed),
            Inner::ReceiverWaiting(_) => unreachable!("recv a oneshot channel more than once!"),
            Inner::Closed => (Err(()), Inner::Closed),
        })
    }
}

type I<T> = Arc<Mutex<Inner<T>>>;

pub struct Sender<T>(I<T>);
impl<T> Sender<T> {
    pub fn send(self, message: T) -> Result<(), T> {
        self.0.lock().put_msg(message)
    }
}

pub struct Receiver<T>(I<T>);
impl<T> Future for Receiver<T> {
    type Output = Result<T, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let msg = self.0.lock().get_msg(cx.waker());
        if let Ok(None) = msg {
            Poll::Pending
        } else {
            Poll::Ready(msg.map(|m| m.unwrap()))
        }
    }
}
