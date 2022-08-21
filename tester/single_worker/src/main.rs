use clap::Parser;
use std::fs;
use R2D2::Config;

#[tokio::main]
async fn main() {
    R2D2::initialize().await;
    let args = Config::parse();

    let input = fs::read_to_string(args.intput_path).unwrap();
    fs::write(args.output_path, input).unwrap();

    R2D2::terminate().await;
}
