use runner::{MASTER_PORT, RUNNER_PORT};
use std::io::{Read, Write};
use std::{
    net::{TcpListener, TcpStream},
    thread,
};

pub struct Master;

impl Master {
    pub fn run() {
        println!("master: starting");
        let listener = TcpListener::bind(format!("127.0.0.1:{}", MASTER_PORT))
            .expect("failed to bind master port");

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    // thread::spawn(move|| {
                    // connection succeeded
                    Master::handle_worker(stream)
                    // });
                }
                Err(e) => {
                    println!("Master Error: {}", e);
                }
            }
            break;
        }
        println!("Master handled worker, notifying runner");

        // update runner
        match TcpStream::connect(format!("127.0.0.1:{}", RUNNER_PORT)) {
            Ok(mut stream) => {
                stream.write(b"Done").unwrap();
            }
            Err(e) => {
                println!("Master Error: connecting to runner: {}", e);
            }
        }
        // close the socket server
        // drop(listener);
        println!("master: terminating");
    }

    fn handle_worker(mut stream: TcpStream) {
        let mut data = [0 as u8; 1024]; // using 1024 byte buffer
        let size = stream.read(&mut data).expect(&format!(
            "An error occurred, terminating connection with {}",
            stream.peer_addr().unwrap()
        ));
        if &data[..size] == b"Can I start?" {
            println!("master: replying to worker: You must!");
            stream.write(b"You must!").unwrap();
        } else {
            return;
        }
        println!("master: starting asking for update");
        loop {
            // TODO(zvikinoza): horrible way of doing this
            // try rpc or `remoc` crate !!!
            thread::sleep(std::time::Duration::from_millis(1000));
            let size = stream.read(&mut data).expect(&format!(
                "An error occurred, terminating connection with {}",
                stream.peer_addr().unwrap()
            ));
            if &data[..size] == b"Task finished" {
                println!("master: reply from worker: Task finished");
                break;
            }
        }
    }
}
