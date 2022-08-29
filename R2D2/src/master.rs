use crate::r2d2::master_server::{Master, MasterServer};
use crate::r2d2::{Empty, GetTaskRequest, GetTaskResponse, TaskResultRequest};
use crate::rdd::task_scheduler::{WorkerEvent, WorkerMessage};
use tonic::{transport::Server, Request, Response, Status};

use crate::MASTER_ADDR;
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
        println!("Sending woker #{id} task={task:?}");
        let task = task.unwrap_or(WorkerMessage::Wait);
        let serialized_task = serde_json::to_vec(&task).unwrap();
        Ok(Response::new(GetTaskResponse { serialized_task }))
    }

    async fn post_task_result(
        &self,
        request: Request<TaskResultRequest>,
    ) -> Result<Response<Empty>, Status> {
        let task_result = request.into_inner().serialized_task_result;
        let task_result = serde_json::from_slice(&task_result).expect("Bad task result");
        self.event_sender
            .send(task_result)
            .await
            .expect("Can't send worker event to task scheduler");
        Ok(Response::new(Empty {}))
    }
}

impl MasterService {
    pub async fn start(self) {
        Server::builder()
            .add_service(MasterServer::new(self))
            .serve(MASTER_ADDR.parse().unwrap())
            .await
            .expect("Error: couldn't start master service");
    }
}
