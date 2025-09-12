use std::{
    process::exit,
    sync::{
        atomic::{AtomicBool, Ordering},
        LazyLock,
    },
};

use palc::Parser;

use crate::scale::Scale;

const HELP_MSG: &str = "xsz displays total space used by set of files, taking into account
compression, reflinks, partially overwritten extents.";

#[derive(Parser)]
#[command(long_about = HELP_MSG)]
pub struct Config {
    /// don't cross filesystem boundaries
    #[arg(short = 'x', long)]
    pub one_fs: bool,
    /// display raw bytes instead of human-readable sizes
    #[arg(short, long)]
    pub bytes: bool,
    /// allow N jobs at once
    #[arg(short, long, default_value_t = 1)]
    pub jobs: u8,
    #[arg(required = true, value_name = "file-or-dir")]
    pub args: Vec<String>,
}
impl Config {
    pub const fn scale(&self) -> Scale {
        if self.bytes {
            Scale::Bytes
        } else {
            Scale::Human
        }
    }
    fn from_args() -> Self {
        let opt = Config::parse();
        if opt.jobs == 0 {
            eprintln!("-j requires an non-zero integer");
            exit(1);
        }
        opt
    }
}
struct Global {
    err: AtomicBool,
    config: LazyLock<Config>,
}

impl Global {
    const fn new() -> Self {
        let err = AtomicBool::new(false);
        let config: LazyLock<Config> = LazyLock::new(|| Config::from_args());
        Self { err, config }
    }
}

const fn global() -> &'static Global {
    static GLOBAL: Global = Global::new();
    &GLOBAL
}

const fn global_err() -> &'static AtomicBool {
    &global().err
}

const fn bool_to_result(is_err: bool) -> Result<(), ()> {
    if is_err {
        Err(())
    } else {
        Ok(())
    }
}
pub fn get_err() -> Result<(), ()> {
    bool_to_result(global_err().load(Ordering::Relaxed))
}

pub fn set_err() -> Result<(), ()> {
    bool_to_result(global_err().swap(true, Ordering::Relaxed))
}

pub fn config() -> &'static Config {
    &global().config
}
