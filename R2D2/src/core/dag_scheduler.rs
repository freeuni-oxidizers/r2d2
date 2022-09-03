use std::any::Any;

use tokio::sync::{mpsc, oneshot};

use crate::core::task_scheduler::{Task, TaskSet};

use super::{
    graph::Graph,
    rdd::RddId,
    task_scheduler::{DagMessage, WorkerEvent},
};

// collect
pub struct Job {
    pub graph: Graph,
    pub target_rdd_id: RddId,
    pub materialized_data_channel: oneshot::Sender<Vec<Box<dyn Any + Send>>>,
}

pub struct DagScheduler {
    job_receiver: mpsc::Receiver<Job>,
    task_sender: mpsc::Sender<DagMessage>,
    event_receiver: mpsc::Receiver<WorkerEvent>,
    n_workers: usize,
}

impl DagScheduler {
    pub fn new(
        job_receiver: mpsc::Receiver<Job>,
        task_sender: mpsc::Sender<DagMessage>,
        event_receiver: mpsc::Receiver<WorkerEvent>,
        n_workers: usize,
    ) -> Self {
        Self {
            job_receiver,
            task_sender,
            event_receiver,
            n_workers,
        }
    }

    pub async fn start(mut self) {
        println!("Dag scheduler is running!");
        while let Some(job) = self.job_receiver.recv().await {
            println!("new job received target_rdd_id={:?}", job.target_rdd_id);

            let mut task_set = TaskSet { tasks: Vec::new() };
            let target_rdd = job.graph.get_rdd(job.target_rdd_id).expect("rdd not found");
            // TODO: actually find stages
            for partition_id in 0..target_rdd.partitions_num() {
                task_set.tasks.push(Task {
                    wide_rdd_id: job.target_rdd_id,
                    narrow_partition_id: partition_id,
                    // num_partitions: target_rdd.partitions_num(),
                    preffered_worker_id: partition_id % self.n_workers,
                    target_workers: todo!(),
                })
            }
            let graph = job.graph.clone();
            self.task_sender
                .send(DagMessage::NewGraph(graph))
                .await
                .expect("can't send graph to task scheduler");

            // TODO: send taskset per stage
            self.task_sender
                .send(DagMessage::SubmitTaskSet(task_set))
                .await
                .expect("can't send graph to task scheduler");

            let mut result_v: Vec<Option<Box<dyn Any + Send>>> = Vec::new();
            for _ in 0..target_rdd.partitions_num() {
                result_v.push(None);
            }
            let mut num_received = 0;
            // TODO: We don't need to receive task materialized results every time.
            // only for last tasks in graph
            while let Some(result) = self.event_receiver.recv().await {
                match result {
                    WorkerEvent::Success(task, serialized_rdd_data) => {
                        let materialized_partition =
                            target_rdd.deserialize_raw_data(serialized_rdd_data);
                        assert!(result_v[task.narrow_partition_id].is_none());
                        result_v[task.narrow_partition_id] = Some(materialized_partition);
                        num_received += 1;
                        if num_received == target_rdd.partitions_num() {
                            let final_results = result_v.into_iter().map(|v| v.unwrap()).collect();
                            job.materialized_data_channel
                                .send(final_results)
                                .expect("can't returned materialied result to spark");
                            break;
                        }
                    }
                    WorkerEvent::Fail(_) => panic!("Task somehow failed?"),
                    WorkerEvent::BucketReceived(_, _) => todo!(),
                }
            }
        }
    }
}
