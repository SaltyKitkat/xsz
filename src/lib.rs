pub mod actor;
pub mod btrfs;
pub mod executor;
pub mod fs_util;
pub mod global;
pub mod taskpak;
pub mod walkdir;
pub mod worker;

#[inline]
pub fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) {
    executor::spawn(future).detach();
}
