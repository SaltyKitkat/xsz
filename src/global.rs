use std::{
    env::args,
    process::exit,
    sync::{
        atomic::{AtomicBool, Ordering},
        LazyLock,
    },
    thread::available_parallelism,
};

use just_getopt::{OptFlags, OptSpecs, OptValueType};

use crate::scale::Scale;

fn print_help() {
    const HELP_MSG: &str = include_str!("./helpmsg.txt");
    eprint!("{}", HELP_MSG);
}

fn nthreads() -> u32 {
    static NTHREADS: LazyLock<u32> =
        LazyLock::new(|| available_parallelism().map(|n| n.get() as _).unwrap_or(4));
    *NTHREADS
}

pub struct Config {
    pub one_fs: bool,
    pub bytes: bool,
    pub jobs: u32,
    pub args: Box<[String]>,
}
impl Config {
    pub fn scale(&self) -> Scale {
        if self.bytes {
            Scale::Bytes
        } else {
            Scale::Human
        }
    }
    fn from_args() -> Self {
        let opt_spec = OptSpecs::new()
            .flag(OptFlags::OptionsEverywhere)
            .option("b", "b", OptValueType::None)
            .option("b", "bytes", OptValueType::None)
            .option("x", "x", OptValueType::None)
            .option("x", "one-file-system", OptValueType::None)
            .option("h", "h", OptValueType::None)
            .option("h", "help", OptValueType::None)
            .option("j", "j", OptValueType::Required)
            .option("j", "jobs", OptValueType::Required);
        let opt = opt_spec.getopt(args().skip(1));
        if let Some(unknown_arg) = opt.unknown.first() {
            if unknown_arg.len() > 1 {
                eprintln!("xsz: unrecognized option '--{}'", unknown_arg);
            } else {
                eprintln!("xsz: invalid option -- '{}'", unknown_arg);
            }
            exit(1);
        }
        let mut one_fs = false;
        let mut bytes = false;
        let mut jobs = nthreads() + 2;
        for opt in opt.options {
            match opt.id.as_str() {
                "b" => bytes = true,
                "x" => one_fs = true,
                "j" => {
                    let Some(arg_jobs): Option<u32> = opt.value.and_then(|n| n.parse().ok()) else {
                        eprintln!("-j requires an integer option");
                        exit(1)
                    };
                    if arg_jobs == 0 {
                        eprintln!("-j requires an non-zero integer");
                    }
                    jobs = arg_jobs;
                }
                "h" => {
                    print_help();
                    exit(0)
                }
                _ => unreachable!(),
            }
        }
        let args = opt.other.into_boxed_slice();
        if args.is_empty() {
            print_help();
            exit(1);
        }
        Self {
            one_fs,
            bytes,
            jobs,
            args,
        }
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

pub fn config() -> &'static Config {
    &global().config
}
