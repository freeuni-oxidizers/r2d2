use clap::Parser;
use std::fs;

#[derive(Parser, Debug)]
#[clap(about="Niffler")]
struct Args {
    #[clap(short, long, parse(from_os_str), default_value = "./input")]
    intput_path: std::path::PathBuf,

    #[clap(short, long, parse(from_os_str), default_value = "./output")]
    output_path: std::path::PathBuf,
}

fn main() {
    let args = Args::parse();
    let input = fs::read_to_string(args.intput_path).unwrap();
    fs::write(args.output_path, input).unwrap();
}