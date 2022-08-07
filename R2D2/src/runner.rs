use std::process::Command;

use self::r2d2::runner_server::RunnerServer;
use r2d2::runner_server::Runner;
use r2d2::Empty;
use tonic::{transport::Server, Request, Response, Status};

use crate::RUNNER_ADDR;
use std::sync::Arc;
use tokio::sync::Notify;

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

#[derive(Debug)]
struct RunnerService {
    shutdown: Arc<Notify>,
}

impl RunnerService {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(Notify::new()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config<'a> {
    pub code_path: &'a str,
    pub input_path: &'a str,
    pub output_path: &'a str,
}

#[tonic::async_trait]
impl Runner for RunnerService {
    async fn job_finished(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        self.shutdown.notify_one();
        Ok(Response::new(Empty {}))
    }
}

pub async fn run(cfg: &Config<'_>) {
    run_master(cfg);
    run_worker(cfg);

    let service = RunnerService::new();
    let shutdown = service.shutdown.clone();

    Server::builder()
        .add_service(RunnerServer::new(service))
        .serve_with_shutdown(RUNNER_ADDR.parse().unwrap(), shutdown.notified())
        .await
        .expect("Unable to start runner service");
}

fn run_worker(cfg: &Config) {
    Command::new("cargo")
        .args([
            "run",
            "-p",
            cfg.code_path,
            "--",
            "-i",
            cfg.input_path,
            "-o",
            cfg.output_path,
        ])
        .spawn()
        .expect("failed to start worker");
}

fn run_master(cfg: &Config) {
    Command::new("cargo")
        .args([
            "run",
            "-p",
            cfg.code_path,
            "--",
            "--master",
            "-i",
            cfg.input_path,
            "-o",
            cfg.output_path,
        ])
        .spawn()
        .expect("failed to start master");
}
