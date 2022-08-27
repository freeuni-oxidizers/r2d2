#![allow(non_snake_case)]

mod master;
pub mod rdd;
pub mod runner;
mod worker;

pub mod r2d2 {
    // TODO: maybe do normal rust types since we don't care about sharing this rpc server with
    // anyone
    tonic::include_proto!("r2d2");
}

use clap::Parser;

pub const RUNNER_ADDR: &str = "127.0.0.1:6901";
pub const MASTER_ADDR: &str = "127.0.0.1:6969";

// This way we can allow user to have their own custom cli.
/// User can parse this directly from cli args or construct it themselves.
#[derive(Parser, Debug, Clone)]
#[clap(about = "Stiffler")]
pub struct Config {
    #[clap(long, takes_value = false)]
    master: bool,

    #[clap(long, short, takes_value = true, default_value = "1")]
    n_workers: u32,

    #[clap(long, takes_value = true)]
    id: u32,
}
