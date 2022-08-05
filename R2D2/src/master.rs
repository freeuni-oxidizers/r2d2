use tonic::{transport::Server, Request, Response, Status};

use r2d2::r2d2_server::{R2d2, R2d2Server};
use r2d2::{ReadyRequest, ReadyResponse, TaskFinishedRequest, TaskFinishedResponse};

use self::r2d2::runner_client::RunnerClient;
use self::r2d2::{JobFinishedRequest, MasterStartedRequest};

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

#[derive(Debug, Default)]
pub struct R2D2Service {}

#[tonic::async_trait]
impl R2d2 for R2D2Service {
    async fn ready(
        &self,
        _request: Request<ReadyRequest>,
    ) -> Result<Response<ReadyResponse>, Status> {
        let reply = ReadyResponse { start: true };
        Ok(Response::new(reply))
    }

    async fn task_finished(
        &self,
        _request: Request<TaskFinishedRequest>,
    ) -> Result<Response<TaskFinishedResponse>, Status> {
        let reply = TaskFinishedResponse { terminate: true };

        // we can consider job done when task is done since,
        // we are not yet sharding job as multiple tasks.
        let all_tasks_finished = true;
        if all_tasks_finished {
            let runner_addr = "http://[::1]:59745";
            let mut client = RunnerClient::connect(runner_addr)
                .await
                .expect("When job finished, failed to connect runner from master");

            let request = Request::new(JobFinishedRequest {});
            let _response = client.job_finished(request).await?;
        }

        Ok(Response::new(reply))
    }
}

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let master_addr = "[::1]:59742".parse()?;
    let service = R2D2Service::default();

    Server::builder()
        .add_service(R2d2Server::new(service))
        .serve(master_addr)
        .await?;

    // notify runner that we are running
    let runner_addr = "http://[::1]:59745";
    let mut client = RunnerClient::connect(runner_addr).await?;

    let request = Request::new(MasterStartedRequest {});
    let _response = client.master_started(request).await?;
    println!("master started at {:?}", master_addr);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let master_addr = "[::1]:59742".parse()?;
    let service = R2D2Service::default();

    Server::builder()
        .add_service(R2d2Server::new(service))
        .serve(master_addr)
        .await?;

    Ok(())
}
