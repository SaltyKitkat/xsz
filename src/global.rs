use std::{
    env::args,
    process::exit,
    sync::{
        atomic::{AtomicBool, Ordering},
        LazyLock,
    },
    u8,
};

use just_getopt::{OptFlags, OptSpecs, OptValueType};

use crate::scale::Scale;

fn print_help() {
    const HELP_MSG: &str = include_str!("./helpmsg.txt");
    eprint!("{}", HELP_MSG);
}

pub struct Config {
    pub one_fs: bool,
    pub bytes: bool,
    pub jobs: u8,
    pub args: Box<[String]>,
}
impl Config {
    pub const fn scale(&self) -> Scale {
        if self.bytes {
            Scale::Bytes
        } else {
            Scale::Human
        }
    }
    fn from_args<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: ToString,
    {
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
        let opt = opt_spec.getopt(args);
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
        let mut jobs = 1;
        for opt in opt.options {
            match opt.id.as_str() {
                "b" => bytes = true,
                "x" => one_fs = true,
                "j" => {
                    let Some(arg_jobs) = opt.value.and_then(|n| n.parse().ok()) else {
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
        assert!(jobs >= 1);
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
        let config: LazyLock<Config> = LazyLock::new(|| Config::from_args(args().skip(1)));
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

fn bool_to_result(is_err: bool) -> Result<(), ()> {
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
