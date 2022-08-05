#![allow(non_snake_case)]

use clap::Parser;

mod master;
pub mod runner;
mod worker;

#[derive(Parser, Debug)]
#[clap(about = "Stiffler")]
pub struct Args {
    #[clap(
        short,
        long,
        parse(from_os_str),
        default_value = "./master_w_single_worker/input"
    )]
    pub intput_path: std::path::PathBuf,

    #[clap(
        short,
        long,
        parse(from_os_str),
        default_value = "./master_w_single_worker/output"
    )]
    pub output_path: std::path::PathBuf,

    #[clap(long, takes_value = false)]
    master: bool,
}

pub async fn initialize() {
    let args = Args::parse();
    if args.master {
        master::start().await.expect("Failed to start master");
        std::process::exit(0);
    } else {
        worker::start().await.expect("Failed to start worker");
    }
    // drop(args);
}

/// terminate is only reachable for worker nodes
pub async fn terminate() {
    worker::task_finished().await.expect("Failed to end worker");
}
