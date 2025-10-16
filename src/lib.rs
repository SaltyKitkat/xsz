pub mod actor;
pub mod btrfs;
pub mod collector;
pub mod executor;
pub mod fs_util;
pub mod global;
pub mod scale;
pub mod taskpak;
pub mod walkdir;
pub mod worker;

pub fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) {
    executor::spawn(future).detach();
}
