use std::{net::TcpStream, io::{Write, Read}};
use runner::MASTER_PORT;

pub struct Worker;

impl Worker {

    pub fn run() {
        println!("woker: starting");
        match TcpStream::connect(format!("127.0.0.1:{}", MASTER_PORT)) {
            Ok(mut stream) => {
                // TODO(zvikinoza): horrible way of doing this
                // try rpc or `remoc` crate !!!
                loop {
                    stream.write(b"Can I start?").unwrap();

                    let mut data = [0 as u8; 1024]; // 
                    let size = stream.read(&mut data)
                        .expect(&format!("Failed to receive response from master on Question: Can I start?"));
                    if &data[..size] == b"You must!" {
                        println!("worker: starting task");  
                        break;
                    }
                }
            }, 
            Err(e) => {
                println!("failded to connect Error: {}", e);
            },
        }

    } 

    pub fn terminate() {
        println!("worker terminating");
        match TcpStream::connect(format!("127.0.0.1:{}", MASTER_PORT)) {
            Ok(mut stream) => {
                stream.write(b"Task finished").unwrap();
            }, 
            Err(e) => {
                println!("failded to connect & update master about termiantion and task finish : {}", e);
            },
        }
        println!("worker terminated");
    }
}
