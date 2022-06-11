use std::{path::PathBuf, process::ExitStatus};
use std::io;
use std::process::Command;

pub const MASTER_PORT: u16 = 8080;
pub const RUNNER_PORT: u16 = 8081;

pub fn run(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf) -> io::Result<ExitStatus> {
    // TODO(zvikinoza): add master worker support

    let p = code_path.to_str().unwrap();
    let input_arg = input_path.to_str().unwrap();
    let output_arg = output_path.to_str().unwrap();
    Command::new("cargo").args(["run", "-p", p, "--", "-i", input_arg, "-o", output_arg]).status()
}

pub fn run_wm(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf) {
    use std::net::TcpListener;
    use std::io::Read;

    // open tcp conn 
    let listener = TcpListener::bind(format!("127.0.0.1:{}", RUNNER_PORT))
        .expect("failed to bind master port");
    
    // run master
    run_master(code_path, input_path, output_path);

    // run woker 
    run_worker(code_path, input_path, output_path);

    // wait for task to finish
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut data = [0 as u8; 1024]; // 
                let size = stream.read(&mut data)
                    .expect(&format!("runner: failed to receive response from master"));
                if &data[..size] == b"Done" {
                    return;
                }
            }
            Err(e) => {
                println!("Error, runner connection with master: {}", e);
            }
        }
        break;
    }
    // return result
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
        .args(["run", "-p", p, "--", "--master", "-i", input_arg, "-o", output_arg])
        .spawn()
        .expect("failed to start running master");
}