use clap::Parser;
use std::fs;

#[derive(Parser, Debug)]
#[clap(about="Niffler")]
struct Args {
    #[clap(short, long, parse(from_os_str), default_value = "./it_works/input")]
    intput_path: std::path::PathBuf,

    #[clap(short, long, parse(from_os_str), default_value = "./it_works/output")]
    output_path: std::path::PathBuf,
}

mod Blast {
    pub fn init() {
        let args = Args::parse();
        
        ///
        /// if worker {
        ///     send master addr and connect to it
        ///     wait master for jobs
        /// 
        /// } else {
        ///     open conn
        ///     send addr to runner
        ///     recv worker conns
        ///     get/send jobs
        ///     send runner final status
        /// }
    }
}

fn main() {
    Blast::init();

    let input = fs::read_to_string(args.intput_path).unwrap();
    fs::write(args.output_path, input).unwrap();
}
