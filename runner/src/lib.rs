use std::io;
use std::process::Command;
use std::{path::PathBuf, process::ExitStatus};

pub fn run(
    code_path: &PathBuf,
    input_path: &PathBuf,
    output_path: &PathBuf,
) -> io::Result<ExitStatus> {

    let p = code_path.to_str().unwrap();
    let input_arg = input_path.to_str().unwrap();
    let output_arg = output_path.to_str().unwrap();
    Command::new("cargo")
        .args(["run", "-p", p, "--", "-i", input_arg, "-o", output_arg])
        .status()
}

pub fn run_wm(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf) {
    run_master(code_path, input_path, output_path);

    run_worker(code_path, input_path, output_path);

    // wait master to call done gRPC
    std::thread::sleep(std::time::Duration::from_secs(5));
}

fn run_worker(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf) {
    let p = code_path.to_str().unwrap();
    let input_arg = input_path.to_str().unwrap();
    let output_arg = output_path.to_str().unwrap();
    Command::new("cargo")
        .args(["run", "-p", p, "--", "-i", input_arg, "-o", output_arg])
        .spawn()
        .expect("failed to start running worker");
}

fn run_master(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf) {
    let p = code_path.to_str().unwrap();
    let input_arg = input_path.to_str().unwrap();
    let output_arg = output_path.to_str().unwrap();
    Command::new("cargo")
        .args([
            "run", "-p", p, "--", "--master", "-i", input_arg, "-o", output_arg,
        ])
        .spawn()
        .expect("failed to start running master");
}
