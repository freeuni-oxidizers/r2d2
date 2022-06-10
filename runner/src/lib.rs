use std::{path::PathBuf, process::ExitStatus};
use std::io;
use std::process::Command;

pub fn run(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf) -> io::Result<ExitStatus> {
    // TODO(zvikinoza): add master worker support

    let p = code_path.to_str().unwrap();
    let input_arg = input_path.to_str().unwrap();
    let output_arg = output_path.to_str().unwrap();
    Command::new("cargo").args(["run", "-p", p, "--", "-i", input_arg, "-o", output_arg]).status()
}

pub fn run_wm(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf) -> io::Result<()> {
    use std::net::TcpStream;
    use std::net::TcpListener;
    use std::io::Read;

    // open tcp conn 
    let listener = TcpListener::bind("127.0.0.1:8080")?;


    // run master
    // get master tcp addr
    let master_addr = run_master(code_path, input_path, output_path, &listener);

    // run woker with master tcp addr
    run_worker(code_path, input_path, output_path, master_addr);

    // wait for task to finish
    for stream in listener.incoming() {
        let mut stream = stream?;
        let mut buf = [0; 1024];
        stream.read(&mut buf)?;
        let response = String::from_utf8_lossy(&buf);
        println!("{}", response);
        if response.eq("ok") {
            println!("finished with success");
            return Ok(());
        }
    }
    
    // return result
    Ok(())
}

fn run_worker(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf, master_addr: std::net::SocketAddr) {
    todo!()
}

fn run_master(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf, listener: &std::net::TcpListener) -> std::net::SocketAddr {
    // returns masters tcp addr
    todo!();
}