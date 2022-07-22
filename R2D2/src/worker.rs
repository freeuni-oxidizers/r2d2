use r2d2::r2d2_client::R2d2Client;
use r2d2::{DoneRequest, ReadyRequest};

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    std::thread::sleep(std::time::Duration::from_secs(3));
    let mut client = R2d2Client::connect("http://[::1]:59749").await?;

    let request = tonic::Request::new(ReadyRequest {});

    let response = client.ready(request).await?;
    println!("Response: {:?}", response);

    if !response.into_inner().start {
        unimplemented!();
    }

    Ok(())
}

pub async fn end() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = R2d2Client::connect("http://[::1]:59749").await?;

    let request = tonic::Request::new(DoneRequest {});
    let response = client.done(request).await?;
    println!("Response: {:?}", response);

    Ok(())
}

#[allow(unused)]
fn main() {
    unimplemented!();
}
