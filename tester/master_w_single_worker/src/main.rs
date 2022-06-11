use clap::Parser;
use std::fs;

#[derive(Parser, Debug)]
#[clap(about="Stiffler")]
struct Args {
    #[clap(short, long, parse(from_os_str), default_value = "./master_w_single_worker/input")]
    intput_path: std::path::PathBuf,

    #[clap(short, long, parse(from_os_str), default_value = "./master_w_single_worker/output")]
    output_path: std::path::PathBuf,

    #[clap(long, takes_value=false)]
    master: bool,
}

pub mod blast {
    use master::Master;
    use worker::Worker;
    use super::{Args, Parser};

    pub fn initialize() {
        let args = Args::parse();
        if args.master {
            Master::run();
            std::process::exit(0);
        } else {
            Worker::run();
        }
        // drop(args);
    }
    
    /// terminate is only reachable for worker nodes
    pub fn terminate() {
        Worker::terminate();
    }
}

fn main() {
    blast::initialize();
    let args = Args::parse();

    let input = fs::read_to_string(args.intput_path).unwrap();
    fs::write(args.output_path, input).unwrap();

    blast::terminate();
}
