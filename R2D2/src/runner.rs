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
    pub n_workers: usize,
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
    run_workers(cfg);

    let service = RunnerService::new();
    let shutdown = service.shutdown.clone();

    Server::builder()
        .add_service(RunnerServer::new(service))
        .serve_with_shutdown(RUNNER_ADDR.parse().unwrap(), shutdown.notified())
        .await
        .expect("Unable to start runner service");
}

fn run_workers(cfg: &Config) {
    for id in 0..cfg.n_workers {
        Command::new("cargo")
            .args([
                "run",
                "-p",
                cfg.code_path,
                "--",
                "-i",
                cfg.input_path,
                "-o",
                &format!("{}@{}", cfg.output_path, id),
            ])
            .spawn()
            .expect("failed to start worker");
    }
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
            "-n",
            &cfg.n_workers.to_string(),
        ])
        .spawn()
        .expect("failed to start master");
}
