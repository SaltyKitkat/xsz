use std::sync::atomic::{AtomicBool, Ordering};

struct Global {
    err: AtomicBool,
    one_fs: AtomicBool,
}

impl Global {
    const fn new() -> Self {
        let err = AtomicBool::new(false);
        let one_fs = AtomicBool::new(false);
        Self { err, one_fs }
    }
}

fn global() -> &'static Global {
    static GLOBAL: Global = Global::new();
    &GLOBAL
}

fn global_err() -> &'static AtomicBool {
    &global().err
}

pub fn get_err() -> Result<(), ()> {
    if global_err().load(Ordering::Relaxed) {
        Err(())
    } else {
        Ok(())
    }
}

pub fn set_err() -> Result<(), ()> {
    match global_err().compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed) {
        Ok(_) => Ok(()),
        Err(_) => Err(()),
    }
}

fn one_fs() -> &'static AtomicBool {
    &global().one_fs
}

pub fn set_one_fs() {
    one_fs().store(true, Ordering::Relaxed);
}

pub fn get_one_fs() -> bool {
    one_fs().load(Ordering::Relaxed)
}
