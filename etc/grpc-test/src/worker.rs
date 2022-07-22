use r2d2::r2d2_client::R2d2Client;
use r2d2::{ReadyRequest, DoneRequest};

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = R2d2Client::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(
        ReadyRequest {}
    );

    let response = client.ready(request).await?;

    if !response.into_inner().start {
        println!("Not start");
        return Ok(());
    }

    println!("Start");
    println!("working working bee bop");
    let request = tonic::Request::new(
        DoneRequest {}
    );
    let responce = client.done(request).await?;
    println!("{:?}", responce.into_inner().terminate);

    Ok(())
}