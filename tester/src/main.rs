use clap::Parser;
use runner;
use std::path::PathBuf;
use std::fs;

#[cfg(test)]
use pretty_assertions::assert_eq;

#[derive(Parser, Debug)]
#[clap(about="Chupacabra")]
struct Args {
    #[clap(short, long, parse(from_os_str), default_value = "./it_works")]
    code_path: PathBuf,

    #[clap(short, long, parse(from_os_str), default_value = "./it_works/input")]
    input_path: PathBuf,

    #[clap(short, long, parse(from_os_str), default_value = "./it_works/output")]
    output_path: PathBuf,
    
    #[clap(short, long, parse(from_os_str), default_value = "./it_works/expected_output")]
    expected_output_path: PathBuf,
}

fn main() {
    let args = Args::parse();
    runner::run(&args.code_path, &args.input_path, &args.output_path);    

    // assert output files match
    let output = fs::read_to_string(&args.output_path).unwrap();
    let expected = fs::read_to_string(&args.expected_output_path).unwrap();
    println!("{}", output);
    println!("{}", expected);
    assert_eq!(output, expected);

    // TODO(zvikinoza): assert errorcodes and stderrs
    // TODO(zvikinoza): run test in parallel 
    // TODO(zvikinoza): move testing from main to test module
    // TODO(zvikinoza): impl prologue and epilogue (e.g. setup and teardown)
}
