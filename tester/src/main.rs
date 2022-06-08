use clap::Parser;
use runner;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(about="Chupacabra")]
struct Args {
    #[clap(short, long, parse(from_os_str), default_value = "./it_works")]
    code_path: PathBuf,

    #[clap(short, long, parse(from_os_str), default_value = "./it_works/input")]
    intput_path: PathBuf,

    #[clap(short, long, parse(from_os_str), default_value = "./it_works/output")]
    output_path: PathBuf,
}

fn main() {
    let args = Args::parse();
    // option 1: 
    // use hardcoded or argv to get the paths for code in and out

    // option 2:
    // traverse dir to find all test subdirs with one *.rs, *.in, *.out files
    
    runner::run(args.code_path, args.intput_path, args.output_path);    
    // run tests, [delegate that to runner]
    //   - run test subdirs in parallel??
    //   - run test subdirs in serial

    // assert output files match
    // assert errorcodes and stderrs
}
