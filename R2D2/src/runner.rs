use std::path::PathBuf;
use std::process::Command;

use self::r2d2::runner_server::RunnerServer;
use r2d2::runner_server::Runner;
use r2d2::{JobFinishedRequest, JobFinishedResponse};
use r2d2::{MasterStartedRequest, MasterStartedResponse};
use tonic::{transport::Server, Request, Response, Status};

pub mod r2d2 {
    tonic::include_proto!("r2d2");
}

#[derive(Debug)]
pub struct RunnerService {
    cfg: Config,
}

impl RunnerService {
    pub fn new(config: Config) -> Self {
        Self {
            cfg: config.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub code_path: PathBuf,
    pub input_path: PathBuf,
    pub output_path: PathBuf,
}

#[tonic::async_trait]
impl Runner for RunnerService {
    async fn master_started(
        &self,
        _request: Request<MasterStartedRequest>,
    ) -> Result<Response<MasterStartedResponse>, Status> {
        run_worker(self.cfg.clone());
        let response = MasterStartedResponse {};
        println!("runner got master_started call & worker is started");
        Ok(Response::new(response))
    }

    async fn job_finished(
        &self,
        _request: Request<JobFinishedRequest>,
    ) -> Result<Response<JobFinishedResponse>, Status> {
        let response = JobFinishedResponse {};
        Ok(Response::new(response))
    }
}

pub async fn run_wm(cfg: Config) {
    let runner_addr = "[::1]:59745".parse().unwrap();
    let service = RunnerService::new(cfg.clone());

    Server::builder()
        .add_service(RunnerServer::new(service))
        .serve(runner_addr)
        .await
        .expect("Failed to build runner server");

    run_master(cfg);
    println!("runner started master");
}

fn run_worker(cfg: Config) {
    let p = cfg.code_path.to_str().unwrap();
    let input_arg = cfg.input_path.to_str().unwrap();
    let output_arg = cfg.output_path.to_str().unwrap();
    Command::new("cargo")
        .args(["run", "-p", p, "--", "-i", input_arg, "-o", output_arg])
        .spawn()
        .expect("failed to start worker");
}

fn run_master(cfg: Config) {
    let p = cfg.code_path.to_str().unwrap();
    let input_arg = cfg.input_path.to_str().unwrap();
    let output_arg = cfg.output_path.to_str().unwrap();
    Command::new("cargo")
        .args([
            "run", "-p", p, "--", "--master", "-i", input_arg, "-o", output_arg,
        ])
        .spawn()
        .expect("failed to start master");
}

#[allow(unused)]
fn main() {
    unimplemented!();
}
