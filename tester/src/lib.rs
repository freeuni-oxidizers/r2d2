#[cfg(test)]
mod tests {
    /// TODO(zvikinoza): run test in parallel

    #[cfg(test)]
    use pretty_assertions::assert_eq;
    use std::path::PathBuf;
    use std::fs;
    use R2D2::runner::Config;

    #[test]
    fn master_w_single_worker() {
        // setup
        println!("\n\n\n master w single worker \n\n\n");
        let program = PathBuf::from("master_w_single_worker");
        let input_file = program.join("input");
        let output_file = program.join("output");
        let outcp = output_file.clone();
        let expected_file = program.join("expected");
        let cfg = Config {
            code_path: program,
            input_path: input_file,
            output_path: output_file,
        };
        tokio_test::block_on(R2D2::runner::run_wm(cfg));
        // R2D2::runner::run_wm(cfg).await;

        // assert output files match
        let output = fs::read_to_string(&outcp).unwrap();
        let expected = fs::read_to_string(&expected_file).unwrap();
        assert_eq!(output, expected);
        //fs::remove_file(&outcp).unwrap();
    }
}
