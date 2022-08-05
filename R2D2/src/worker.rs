use r2d2::r2d2_client::R2d2Client;
use r2d2::{ReadyRequest, TaskFinishedRequest};

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let master_addr = "http://[::1]:59742";
    let mut client = R2d2Client::connect(master_addr).await?;

    let request = tonic::Request::new(ReadyRequest {});
    let response = client.ready(request).await?;
    if !response.into_inner().start {
        unimplemented!();
    }

    Ok(())
}

pub async fn task_finished() -> Result<(), Box<dyn std::error::Error>> {
    let master_addr = "http://[::1]:59742";
    let mut client = R2d2Client::connect(master_addr).await?;

    let request = tonic::Request::new(TaskFinishedRequest {});
    let _response = client.task_finished(request).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start().await
}
