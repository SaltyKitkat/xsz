use std::sync::atomic::{AtomicBool, Ordering};

fn global_err() -> &'static AtomicBool {
    static ERR: AtomicBool = AtomicBool::new(false);
    &ERR
}

pub fn get() -> Result<(), ()> {
    if global_err().load(Ordering::Relaxed) {
        Err(())
    } else {
        Ok(())
    }
}

pub fn set() {
    global_err().store(true, Ordering::Relaxed);
}
