use crate::MASTER_ADDR;
use r2d2::master_client::MasterClient;
use r2d2::Empty;
use tokio::time::{sleep, Duration};
use tonic::Request;

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    // master may not be running yet
    let mut retries = 3;
    let mut wait = 100;
    let master_conn = loop {
        let master_addr = format!("http://{}", MASTER_ADDR);
        match MasterClient::connect(master_addr).await {
            Err(_) if retries > 0 => {
                retries -= 1;
                sleep(Duration::from_millis(wait)).await;
                wait *= 2;
            }
            master_conn => break master_conn,
        }
    };
    let response = master_conn?
        .ready(Request::new(Empty {}))
        .await?
        .into_inner();
    if !response.start {
        unimplemented!();
    }

    Ok(())
}

pub async fn task_finished() -> Result<(), Box<dyn std::error::Error>> {
    let master_addr = format!("http://{}", MASTER_ADDR);
    let response = MasterClient::connect(master_addr)
        .await?
        .task_finished(Request::new(Empty {}))
        .await?
        .into_inner();
    if !response.terminate {
        unimplemented!();
    }
    Ok(())
}
