#[cfg(test)]
mod tests {
    // #[test]
    #[allow(dead_code)]
    fn single_worker() {
        tokio_test::block_on(R2D2::runner::run(&R2D2::runner::Config {
            code_path: "single_worker",
            input_path: "single_worker/input",
            output_path: "single_worker/output",
            n_workers: 1,
        }));
        // assert output files match
        let output = std::fs::read_to_string(&"single_worker/output@0").unwrap();
        let expected = std::fs::read_to_string(&"single_worker/expected").unwrap();
        assert_eq!(output, expected);
        // cleanup
        std::fs::remove_file(&"single_worker/output@0").unwrap();
    }

    // #[test]
    #[allow(dead_code)]
    fn multiple_workers() {
        let cfg = &R2D2::runner::Config {
            code_path: "multiple_workers",
            input_path: "multiple_workers/input",
            output_path: "multiple_workers/output",
            n_workers: 10,
        };
        tokio_test::block_on(R2D2::runner::run(cfg));
        let ex_fname = "multiple_workers/expected";
        let expected = std::fs::read_to_string(ex_fname).unwrap();
        for id in 0..cfg.n_workers {
            let out_fname = &format!("multiple_workers/output@{}", id);
            let output = std::fs::read_to_string(out_fname).unwrap();
            assert_eq!(output, expected);
            // cleanup
            std::fs::remove_file(out_fname).unwrap();
        }
    }

    #[test]
    fn map_square() {
        let cfg = &R2D2::runner::Config {
            code_path: "map_square",
            input_path: "map_square/input",
            output_path: "map_square/output",
            n_workers: 1,
        };
        tokio_test::block_on(R2D2::runner::run(cfg));

        let ex_fname = "map_square/expected";
        let out_fname = "map_square/output";

        let mut expected = std::fs::read_to_string(ex_fname).unwrap();
        let mut output = std::fs::read_to_string(out_fname).unwrap();
        // eat all witespaces when comapring
        output.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(output, expected);
        // cleanup
        std::fs::remove_file(out_fname).unwrap();
    }
}
