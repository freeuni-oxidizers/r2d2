use crate::r2d2::master_server::{Master, MasterServer};
use crate::r2d2::{JobFinishedRequest, GetTaskRequest, Task, TaskAction, TaskFinishedResponse, TaskFinishedRequest};
use tonic::{transport::Server, Request, Response, Status};

use crate::r2d2::runner_client::RunnerClient;
use crate::{MASTER_ADDR, RUNNER_ADDR};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct WorkerState {
    rx: broadcast::Receiver<Task>,
}

impl WorkerState {
    fn new(rx: broadcast::Receiver<Task>) -> Self {
        Self { rx }
    }
}

#[derive(Debug)]
pub struct MasterService {
    shutdown: Arc<Notify>,
    n_running: AtomicU32,
    n_workers: usize,
    workers: Vec<Mutex<WorkerState>>,
}

impl MasterService {
    pub async fn new(n_workers: u32, tx: &broadcast::Sender<Task>) -> Self {
        let mut ms = Self {
            shutdown: Arc::new(Notify::new()),
            n_running: AtomicU32::new(n_workers),
            workers: Vec::new(),
            n_workers: n_workers as usize,
        };
        {
            for _ in 0..n_workers {
                ms.workers.push(Mutex::new(WorkerState::new(tx.subscribe())));
            }
        }
        ms
    }
}

#[tonic::async_trait]
impl Master for MasterService {
    async fn get_task(&self, request: Request<GetTaskRequest>) -> Result<Response<Task>, Status> {
        let id = request.into_inner().id as usize;
        // println!("master got req from woker w id={}", id);
        assert!(id < self.n_workers);
        // TODO(zvikinoza): must remove mutex, there gotta be some rusty way to do this
        // if i remove it compiler complans
        // is this safty argument sufficient?
        // self.workers are immutable.
        // all request.id are distinct and < self.n_workers
        // self.workers[id] are thread safe
        let task = { self.workers[id].lock().await.rx.try_recv() };
        // println!("sending woker={} task={:?}", id, task);
        match task {
            Ok(task) => Ok(Response::new(task)),
            Err(_) => Ok(Response::new(Task {
                action: TaskAction::Wait as i32,
                context: "".to_string(),
                rdd: "".to_string(),
            })),
        }
    }

    // TODO(zvikinoza): merge task_finsied w get task?
    async fn task_finished(
        &self,
        request: Request<TaskFinishedRequest>,
    ) -> Result<Response<TaskFinishedResponse>, Status> {
        // TODO(zvikinoza): change Ordering::SeqCst
        // job is finished when all tasks finished and there are no more collects
        // rn only one task/collect so, ok for now
        let job_finished = self.n_running.fetch_sub(1, Ordering::SeqCst) == 1;
        let request = request.into_inner();
        // println!("master: from worker={} got result={:?}", request.id, request.result);
        if job_finished {
            // // pass results to runner 
            // // when more workers, then aggregate results
            // let results = Request::new(JobFinishedRequest {
            //     result: request.result,
            // });
            // let runner_addr = format!("http://{}", RUNNER_ADDR);
            // RunnerClient::connect(runner_addr)
            //     .await
            //     .expect("Failed to notify 'job finished' to runner")
            //     .job_finished(results)
            //     .await?;

            self.shutdown.notify_one();
        }

        Ok(Response::new(TaskFinishedResponse {
            shutdown: job_finished,
        }))
    }
}

pub async fn start(n_workers: u32, tx: &broadcast::Sender<Task>) -> JoinHandle<()> {
    // println!("master starting");
    let service = MasterService::new(n_workers, tx).await;
    let shutdown = service.shutdown.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(MasterServer::new(service))
            .serve_with_shutdown(MASTER_ADDR.parse().unwrap(), shutdown.notified())
            .await
            .expect("Error: couldn't start master service");
    })
}
