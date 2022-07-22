#[cfg(test)]
mod tests {
    /// TODO(zvikinoza): run test in parallel 

    #[cfg(test)]
    use pretty_assertions::assert_eq;
    use std::path::PathBuf;
    use std::{fs, panic};
    use runner;

    #[test]
    fn it_works() {
        // run_test("it_works", runner::run);
        let program= PathBuf::from("it_works");
        let input_file= program.join("input");
        let output_file= program.join("output");
        let expected_file= program.join("expected");

        runner::run(&program, &input_file, &output_file).unwrap();

        // assert output files match
        let output = fs::read_to_string(&output_file).unwrap();
        let expected = fs::read_to_string(&expected_file).unwrap();
        assert_eq!(output, expected);

        fs::remove_file(output_file).unwrap();
    }

    #[test]
    fn master_w_single_worker() {
        run_test("master_w_single_worker", runner::run_wm); 
    }

    fn run_test<T>(test_program: &str, test_runner: T) -> ()
        where T: FnOnce(&PathBuf, &PathBuf, &PathBuf) -> () + panic::UnwindSafe
    {
        // setup 
        let program = PathBuf::from(test_program);
        let input_file = program.join("input");
        let output_file = program.join("output");
        let expected_file = program.join("expected");

        let result = panic::catch_unwind(|| {
            test_runner(&program, &input_file, &output_file);

            // assert output files match
            let output = fs::read_to_string(&output_file).unwrap();
            let expected = fs::read_to_string(&expected_file).unwrap();
            assert_eq!(output, expected);
        });

        // teardown
        // fs::remove_file(&output_file).unwrap();

        assert!(result.is_ok())
    }
}