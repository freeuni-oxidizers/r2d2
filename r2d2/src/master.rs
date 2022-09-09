use crate::core::task_scheduler::{WorkerEvent, WorkerMessage};
use crate::r2d2::master_server::{Master, MasterServer};
use crate::r2d2::{Empty, GetTaskRequest, GetTaskResponse, TaskResultRequest};
use tonic::{transport::Server, Request, Response, Status};

use tokio::sync::mpsc;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct WorkerState {
    rx: mpsc::Receiver<WorkerMessage>,
}

impl WorkerState {
    fn new(rx: mpsc::Receiver<WorkerMessage>) -> Self {
        Self { rx }
    }
}

#[derive(Debug)]
pub struct MasterService {
    n_workers: usize,
    event_sender: mpsc::Sender<WorkerEvent>,
    workers: Vec<Mutex<WorkerState>>,
}

impl MasterService {
    pub fn new(
        rxs: Vec<mpsc::Receiver<WorkerMessage>>,
        event_sender: mpsc::Sender<WorkerEvent>,
    ) -> Self {
        Self {
            n_workers: rxs.len(),
            workers: rxs
                .into_iter()
                .map(|rx| Mutex::new(WorkerState::new(rx)))
                .collect(),
            event_sender,
        }
    }
}

#[tonic::async_trait]
impl Master for MasterService {
    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskResponse>, Status> {
        let id = request.into_inner().id as usize;
        assert!(id < self.n_workers);
        let task = { self.workers[id].lock().await.rx.try_recv() };
        // TODO: maybe tokio tracing?
        let task = task.unwrap_or(WorkerMessage::Wait);
        println!("Sending worker #{id} task={task:?}");
        let serialized_task = rmp_serde::to_vec(&task).unwrap();
        Ok(Response::new(GetTaskResponse { serialized_task }))
    }

    async fn post_task_result(
        &self,
        request: Request<TaskResultRequest>,
    ) -> Result<Response<Empty>, Status> {
        let task_result = request.into_inner().serialized_task_result;
        let task_result = rmp_serde::from_slice(&task_result).expect("Bad task result");
        self.event_sender
            .send(task_result)
            .await
            .expect("Can't send worker event to task scheduler");
        Ok(Response::new(Empty {}))
    }
}

impl MasterService {
    pub async fn start(self, port: usize) {
        let addr = format!("0.0.0.0:{port}").parse().unwrap();
        Server::builder()
            .add_service(MasterServer::new(self))
            .serve(addr)
            .await
            .expect("Error: couldn't start master service");
    }
}
