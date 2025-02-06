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

pub fn set() -> Result<(), ()> {
    match global_err().compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed) {
        Ok(_) => Ok(()),
        Err(_) => Err(()),
    }
}
