#![allow(non_snake_case)]

mod master;
pub mod runner;
mod worker;

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

use clap::Parser;

pub const ADDR_BASE: &str = "127.0.0.1";
pub const MASTER_ADDR: &str = "127.0.0.1:6969";

// This way we can allow user to have their own custom cli.
/// User can parse this directly from cli args or construct it themselves.
#[derive(Parser, Debug, Clone)]
#[clap(about = "Default arguments for generic r2d2 app")]
pub struct Config {
    #[clap(long, takes_value = false)]
    pub master: bool,

    #[clap(long, short, takes_value = true, default_value = "1")]
    pub n_workers: usize,

    #[clap(long, takes_value = true)]
    pub id: u32,

    #[clap(long, takes_value = true)]
    pub port: u32,
}

pub mod core;
