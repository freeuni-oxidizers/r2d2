#![allow(non_snake_case)]

use std::path::PathBuf;

use serde::Deserialize;

pub mod core;
mod master;
pub mod runner;
mod worker;

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

use clap::Parser;

pub const ADDR_BASE: &str = "0.0.0.0";
pub const MASTER_ADDR: &str = "0.0.0.0:6969";

// This way we can allow user to have their own custom cli.
/// User can parse this directly from cli args or construct it themselves.
#[derive(Parser, Debug, Clone)]
#[clap(about = "Default arguments for generic r2d2 app")]
pub struct Args {
    #[clap(long, takes_value = false)]
    pub master: bool,

    #[clap(long, takes_value = true)]
    pub id: usize,

    #[clap(short, long, value_parser, value_name = "DIR")]
    fs_root: PathBuf,

    #[clap(long, takes_value = true, default_value_t = 8888)]
    pub port: usize,
}

#[derive(Deserialize)]
pub struct Config {
    worker_addrs: Vec<String>,
    master_addr: String,
}
