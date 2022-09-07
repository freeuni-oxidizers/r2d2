use std::process::Command;

#[derive(Clone, Debug)]
pub struct Config<'a> {
    pub code_path: &'a str,
    pub input_path: &'a str,
    pub output_path: &'a str,
    pub n_workers: usize,
}

pub async fn run(cfg: &Config<'_>) {
    run_master(cfg);
    run_workers(cfg);
}

// 0 <= id < n_worker is for workers
fn run_workers(cfg: &Config) {
    for id in 0..cfg.n_workers {
        Command::new("cargo")
            .args([
                "run",
                "-p",
                cfg.code_path,
                "--",
                "-i",
                cfg.input_path,
                "-o",
                &format!("{}@{}", cfg.output_path, id),
                "--id",
                &id.to_string(),
            ])
            .spawn()
            .expect("failed to start worker");
    }
}

fn run_master(cfg: &Config) {
    let n_workers = &cfg.n_workers.to_string();
    Command::new("cargo")
        .args([
            "run",
            "-p",
            cfg.code_path,
            "--",
            "--master",
            "-i",
            cfg.input_path,
            "-o",
            cfg.output_path,
            "-n",
            n_workers,
            "--id",
            n_workers,
        ])
        .spawn()
        .expect("failed to start master");
}
