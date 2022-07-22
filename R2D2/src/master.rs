use tonic::{transport::Server, Request, Response, Status};

use r2d2::r2d2_server::{R2d2, R2d2Server};
use r2d2::{DoneRequest, DoneResponse, ReadyRequest, ReadyResponse};

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

#[derive(Debug, Default)]
pub struct R2D2Service {}

#[tonic::async_trait]
impl R2d2 for R2D2Service {
    async fn ready(
        &self,
        request: Request<ReadyRequest>,
    ) -> Result<Response<ReadyResponse>, Status> {
        println!("Got a request: {:?}", request);

        let _req = request.into_inner();

        let reply = ReadyResponse { start: true };

        Ok(Response::new(reply))
    }

    async fn done(&self, request: Request<DoneRequest>) -> Result<Response<DoneResponse>, Status> {
        println!("Got a request: {:?}", request);

        let _req = request.into_inner();

        let reply = DoneResponse { terminate: true };

        Ok(Response::new(reply))
    }
}

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:59749".parse()?;
    let service = R2D2Service::default();

    println!("\n\n\n\nStarting MASTER\n\n\n\n");

    Server::builder()
        .add_service(R2d2Server::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

#[allow(unused)]
fn main() {
    unimplemented!();
}

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let addr = "[::1]:50051".parse()?;
//     let service = R2D2Service::default();

//     Server::builder()
//         .add_service(R2d2Server::new(service))
//         .serve(addr)
//         .await?;

//     Ok(())
// }
