use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::{
    dag_scheduler::TaskId,
    graph::Graph,
    rdd::{RddId, RddPartitionId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailReason {
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketReceivedEvent {
    pub worker_id: usize,
    pub wide_partition: RddPartitionId,
    pub narrow_partition_id: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerEvent {
    Success(Task, Vec<u8>),
    BucketReceived(BucketReceivedEvent),
    // (worker_id)
    GraphReceived(usize),
    Fail(FailReason),
}

#[derive(Debug)]
pub enum DagMessage {
    NewGraph(Graph),
    SubmitTask(Task),
}

/// This will go to worker over network
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerMessage {
    NewGraph(Graph),
    RunTask(Task),
    Wait,
    Shutdown, //?
}

pub struct TaskScheduler {
    /// Channel to receive `TaskSet`
    // from dag scheduler
    taskset_receiver: mpsc::Receiver<DagMessage>,
    // to dag scheduler
    dag_event_sender: mpsc::Sender<WorkerEvent>,
    /// Channel for each worker
    worker_queues: Vec<mpsc::Sender<WorkerMessage>>,
    event_receiver: mpsc::Receiver<WorkerEvent>,
    current_graph: Graph,
}

impl TaskScheduler {
    pub fn new(
        taskset_receiver: mpsc::Receiver<DagMessage>,
        dag_event_sender: mpsc::Sender<WorkerEvent>,
        worker_queues: Vec<mpsc::Sender<WorkerMessage>>,
        event_receiver: mpsc::Receiver<WorkerEvent>,
    ) -> Self {
        Self {
            taskset_receiver,
            dag_event_sender,
            worker_queues,
            event_receiver,
            current_graph: Graph::default(),
        }
    }

    pub async fn handle_dag_message(&mut self, dag_message: DagMessage) {
        match dag_message {
            DagMessage::NewGraph(g) => {
                self.current_graph = g.clone();
                for worker_queue in self.worker_queues.clone().into_iter() {
                    worker_queue
                        .send(WorkerMessage::NewGraph(g.clone()))
                        .await
                        .expect("Can't send graph to rpc server")
                }
            }
            DagMessage::SubmitTask(task) => {
                self.worker_queues[task.worker_id]
                    .send(WorkerMessage::RunTask(task))
                    .await
                    .expect("Can't send task to rpc server");
            }
        }
    }

    pub async fn handle_event(&mut self, event: WorkerEvent) {
        let dag_event_sender = self.dag_event_sender.clone();
        dag_event_sender
            .send(event)
            .await
            .expect("Can't send event to dag scheduler");
    }

    pub async fn start(mut self) {
        loop {
            tokio::select! {
                Some(dag_message) = self.taskset_receiver.recv() => {
                    self.handle_dag_message(dag_message).await;
                }
                Some(event) = self.event_receiver.recv() => {
                    self.handle_event(event).await;
                }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WideTask {
    pub wide_rdd_id: RddId,
    // need this on master. sry :')
    pub narrow_rdd_id: RddId,
    pub narrow_partition_id: usize,
    pub target_workers: Vec<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResultTask {
    pub rdd_partition_id: RddPartitionId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskKind {
    ResultTask(ResultTask),
    WideTask(WideTask),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub worker_id: usize,
    pub kind: TaskKind,
}
