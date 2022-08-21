use std::process::Command;

use crate::r2d2::runner_server::{Runner, RunnerServer};
use crate::r2d2::{Empty, JobFinishedRequest};
use tonic::{transport::Server, Request, Response, Status};

use crate::RUNNER_ADDR;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Default, Debug)]
struct RunnerService {
    shutdown: Arc<Notify>,
}

impl RunnerService {
    pub fn new() -> Self {
        Self::default()
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
    async fn job_finished(&self, request: Request<JobFinishedRequest>) -> Result<Response<Empty>, Status> {
        // serialize for results for testing
        std::fs::write("map_square/output", request.into_inner().result)?;
        self.shutdown.notify_one();
        println!("\n\nrunner shutting down\n\n");
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

// TODO(zvikinoza): id=n_workers is for master
// 0 <= id < n_worker is for workers
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
                "--id",
                &id.to_string(),
            ])
            .spawn()
            .expect("failed to start worker");
    }
}

fn run_master(cfg: &Config) {
    let n_workers = &cfg.n_workers.to_string();
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
            n_workers,
            "--id",
            n_workers,
        ])
        .spawn()
        .expect("failed to start master");
}
