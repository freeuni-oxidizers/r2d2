use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(about="Chupacabra")]
struct Args {
    #[clap(short, long, parse(from_os_str), default_value = "it_works")]
    cargo_package_path: PathBuf,

    #[clap(short, long, parse(from_os_str), default_value = "./it_works/input")]
    input_path: PathBuf,

    #[clap(short, long, parse(from_os_str), default_value = "./it_works/output")]
    output_path: PathBuf,
    
    #[clap(short, long, parse(from_os_str), default_value = "./it_works/expected_output")]
    expected_output_path: PathBuf,
}


#[cfg(test)]
mod tests {
    /// TODO(zvikinoza): assert errorcodes and stderrs
    /// TODO(zvikinoza): run test in parallel 
    /// TODO(zvikinoza): move testing from main to test module
    /// TODO(zvikinoza): impl prologue and epilogue (e.g. setup and teardown)

    use std::fs;
    use runner;
    #[cfg(test)]
    use pretty_assertions::assert_eq;

    use super::*;

   #[test]
    fn it_works() {
        let args = Args::parse();
        runner::run(&args.cargo_package_path, &args.input_path, &args.output_path).unwrap();

        // assert output files match
        let output = fs::read_to_string(&args.output_path).unwrap();
        let expected = fs::read_to_string(&args.expected_output_path).unwrap();
        assert_eq!(output, expected);

        // THIS WILL BECOME VERY TEDIOUS AND UNSTABLE !!!
        fs::remove_file(args.output_path).unwrap();
    }
}
