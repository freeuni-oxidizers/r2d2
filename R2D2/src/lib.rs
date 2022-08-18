#![allow(non_snake_case)]

mod master;
pub mod rdd;
pub mod runner;
mod worker;

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

use clap::Parser;

pub const RUNNER_ADDR: &str = "[::1]:51158";
pub const MASTER_ADDR: &str = "[::1]:51159";

#[derive(Parser, Debug, Clone)]
#[clap(about = "Stiffler")]
pub struct Config {
    #[clap(short, long, parse(from_os_str))]
    pub intput_path: std::path::PathBuf,

    #[clap(short, long, parse(from_os_str))]
    pub output_path: std::path::PathBuf,

    #[clap(long, takes_value = false)]
    master: bool,

    #[clap(long, short, takes_value = true, default_value = "1")]
    n_workers: u32,

    #[clap(long, takes_value = true)]
    id: u32,
}
