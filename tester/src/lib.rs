#[cfg(test)]
mod tests {
    // TODO(zvikinoza): run test in parallel

    #[test]
    fn single_worker() {
        tokio_test::block_on(R2D2::runner::run(&R2D2::runner::Config {
            code_path: "single_worker",
            input_path: "single_worker/input",
            output_path: "single_worker/output",
        }));
        // assert output files match
        let output = std::fs::read_to_string(&"single_worker/output").unwrap();
        let expected = std::fs::read_to_string(&"single_worker/expected").unwrap();
        assert_eq!(output, expected);
        std::fs::remove_file(&"single_worker/output").unwrap();
    }
}
