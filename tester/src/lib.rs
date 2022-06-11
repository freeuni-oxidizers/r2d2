
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
    use std::path::PathBuf;

    // #[test]
    // fn it_works() {
    //     let package_path = PathBuf::from("it_works");
    //     let input_path = PathBuf::from("./it_works/input");
    //     let output_path = PathBuf::from("./it_works/output");
    //     let expected_output_path = PathBuf::from("./it_works/expected_output");
    //     runner::run(&package_path, &input_path, &output_path).unwrap();

    //     // assert output files match
    //     let output = fs::read_to_string(&output_path).unwrap();
    //     let expected = fs::read_to_string(&expected_output_path).unwrap();
    //     assert_eq!(output, expected);

    //     // THIS WILL BECOME VERY TEDIOUS AND UNSTABLE !!!
    //     fs::remove_file(output_path).unwrap();
    // }

    #[test]
    fn master_w_single_worker() {
        eprintln!("master_w_single_worker");
        let package_path = PathBuf::from("master_w_single_worker");
        let input_path = PathBuf::from("./master_w_single_worker/input");
        let output_path = PathBuf::from("./master_w_single_worker/output");
        let expected_output_path = PathBuf::from("./master_w_single_worker/expected_output");

        runner::run_wm(&package_path, &input_path, &output_path);
        // assert output files match
        let output = fs::read_to_string(&output_path).unwrap();
        println!("tester sees woker's output: {}", output);
        let expected = fs::read_to_string(&expected_output_path).unwrap();
        assert_eq!(output, expected);

        // THIS WILL BECOME VERY TEDIOUS AND UNSTABLE !!!
        fs::remove_file(&output_path).unwrap();
    }
}
