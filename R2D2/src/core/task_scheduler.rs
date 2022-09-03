use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::{
    graph::Graph,
    rdd::{RddId, RddPartitionId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailReason {
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerEvent {
    Success(Task, Vec<u8>),
    // worker_id, partition_id
    BucketReceived(usize, RddPartitionId),
    Fail(FailReason),
}

#[derive(Debug)]
pub enum DagMessage {
    NewGraph(Graph),
    SubmitTaskSet(TaskSet),
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
            DagMessage::SubmitTaskSet(task_set) => {
                for task in task_set.tasks {
                    self.worker_queues[task.preffered_worker_id]
                        .send(WorkerMessage::RunTask(task))
                        .await
                        .expect("Can't send task to rpc server");
                }
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

#[derive(Debug)]
pub struct TaskSet {
    // TODO: do JobId newtype
    // job_id: usize,
    pub tasks: Vec<Task>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub wide_rdd_id: RddId, // final rdd
    pub narrow_partition_id: usize,
    // pub num_partitions: usize,
    pub preffered_worker_id: usize,
    pub target_workers: Vec<usize>,
}
