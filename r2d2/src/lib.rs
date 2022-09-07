#![allow(clippy::type_complexity)]

use serde::Deserialize;

pub mod core;
mod master;
pub mod runner;
mod worker;

// can't fix diz
#[allow(clippy::derive_partial_eq_without_eq)]
pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

use clap::Parser;

// This way we can allow user to have their own custom cli.
/// User can parse this directly from cli args or construct it themselves.
#[derive(Parser, Debug, Clone)]
#[clap(about = "Default arguments for generic r2d2 app")]
pub struct Args {
    #[clap(long, takes_value = false)]
    pub master: bool,

    #[clap(long, takes_value = true)]
    pub id: usize,

    // #[clap(short, long, value_parser, value_name = "DIR")]
    // fs_root: PathBuf,

    #[clap(long, takes_value = true, default_value_t = 8888)]
    pub port: usize,
}

#[derive(Clone, Deserialize)]
pub struct Config {
    worker_addrs: Vec<String>,
    master_addr: String,
}
