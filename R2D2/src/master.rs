use tonic::{transport::Server, Request, Response, Status};

use r2d2::master_server::{Master, MasterServer};
use r2d2::{Empty, ReadyResponse, TaskFinishedResponse};

use self::r2d2::runner_client::RunnerClient;
use crate::{MASTER_ADDR, RUNNER_ADDR};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

#[derive(Debug)]
struct MasterService {
    shutdown: Arc<Notify>,
    n_running: AtomicUsize,
}

impl MasterService {
    fn new(n_workers: usize) -> Self {
        Self {
            shutdown: Arc::new(Notify::new()),
            n_running: AtomicUsize::new(n_workers),
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
        let all_finished = self.n_running.fetch_sub(1, Ordering::SeqCst) == 1;
        if all_finished {
            let runner_addr = format!("http://{}", RUNNER_ADDR);
            let _response = RunnerClient::connect(runner_addr)
                .await
                .expect("Failed to notify 'job finished' to runner")
                .job_finished(Request::new(Empty {}))
                .await?;

            self.shutdown.notify_one();
        }

        Ok(Response::new(TaskFinishedResponse { terminate: true }))
    }
}

pub async fn start(n_workers: usize) -> Result<(), Box<dyn std::error::Error>> {
    let service = MasterService::new(n_workers);
    let shutdown = service.shutdown.clone();

    Server::builder()
        .add_service(MasterServer::new(service))
        .serve_with_shutdown(MASTER_ADDR.parse().unwrap(), shutdown.notified())
        .await
        .expect("Unable to start master service");
    Ok(())
}
