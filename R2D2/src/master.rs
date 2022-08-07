use tonic::{transport::Server, Request, Response, Status};

use r2d2::master_server::{Master, MasterServer};
use r2d2::{Empty, ReadyResponse, TaskFinishedResponse};

use self::r2d2::runner_client::RunnerClient;
use crate::{MASTER_ADDR, RUNNER_ADDR};
use std::sync::Arc;
use tokio::sync::Notify;

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

#[derive(Debug)]
struct MasterService {
    shutdown: Arc<Notify>,
}

impl MasterService {
    fn new() -> Self {
        Self {
            shutdown: Arc::new(Notify::new()),
        }
    }
}

#[tonic::async_trait]
impl Master for MasterService {
    async fn ready(&self, _request: Request<Empty>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { start: true }))
    }

    async fn task_finished(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<TaskFinishedResponse>, Status> {
        let reply = TaskFinishedResponse { terminate: true };
        // only one task for now
        let all_tasks_finished = true;
        if all_tasks_finished {
            let runner_addr = format!("http://{}", RUNNER_ADDR);
            let _response = RunnerClient::connect(runner_addr)
                .await
                .expect("Failed to notify 'job finished' to runner")
                .job_finished(Request::new(Empty {}))
                .await?;
        }

        self.shutdown.notify_one();
        Ok(Response::new(reply))
    }
}

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let service = MasterService::new();
    let shutdown = service.shutdown.clone();

    Server::builder()
        .add_service(MasterServer::new(service))
        .serve_with_shutdown(MASTER_ADDR.parse().unwrap(), shutdown.notified())
        .await
        .expect("Unable to start master service");
    Ok(())
}
