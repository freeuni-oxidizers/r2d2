
#[cfg(test)]
mod tests {
    /// TODO(zvikinoza): impl prologue and epilogue (e.g. setup and teardown)
    /// TODO(zvikinoza): assert errorcodes and stderrs
    /// TODO(zvikinoza): run test in parallel 
    /// TODO(zvikinoza): move testing from main to test module

    use std::fs;
    use runner;
    #[cfg(test)]
    use pretty_assertions::assert_eq;
    use std::path::PathBuf;

    #[test]
    fn it_works() {
        let program= PathBuf::from("it_works");
        let input= program.join("input");
        let output= program.join("output");
        let expected= program.join("expected");

        runner::run(&program, &input, &output).unwrap();

        // assert output files match
        let output = fs::read_to_string(&output).unwrap();
        let expected = fs::read_to_string(&expected).unwrap();
        assert_eq!(output, expected);

        // THIS WILL BECOME VERY TEDIOUS AND UNSTABLE !!!
        fs::remove_file(output).unwrap();
    }

    #[test]
    fn master_w_single_worker() {
        let program= PathBuf::from("it_works");
        let input= program.join("input");
        let output= program.join("output");
        let expected= program.join("expected");

        runner::run_wm(&program, &input, &output);

        // assert output files match
        let output = fs::read_to_string(&output).unwrap();
        let expected = fs::read_to_string(&expected).unwrap();
        assert_eq!(output, expected);

        // THIS WILL BECOME VERY TEDIOUS AND UNSTABLE !!!
        fs::remove_file(&output).unwrap();
    }
}
