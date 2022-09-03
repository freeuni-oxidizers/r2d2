use std::{
    any::Any,
    collections::{HashMap, HashSet},
};

use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::core::{
    dag_scheduler::dsu::{find_groups, GroupId},
    task_scheduler::{ResultTask, Task},
};

use super::{
    graph::Graph,
    rdd::{Dependency, RddId, RddPartitionId},
    task_scheduler::{BucketReceivedEvent, DagMessage, TaskKind, WideTask, WorkerEvent},
};

// collect
pub struct Job {
    pub graph: Graph,
    pub target_rdd_id: RddId,
    pub materialized_data_channel: oneshot::Sender<Vec<Box<dyn Any + Send>>>,
}

mod dsu {
    use std::collections::{HashMap, HashSet};

    use crate::core::{
        graph::Graph,
        rdd::{RddId, RddPartitionId},
    };

    #[derive(Default, Debug)]
    struct PartitionDsu {
        parent: HashMap<RddPartitionId, RddPartitionId>,
        visited: HashSet<RddPartitionId>,
    }

    #[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)]
    pub struct GroupId(usize);

    pub fn find_groups(graph: &Graph, target_rdd: RddId) -> HashMap<RddPartitionId, GroupId> {
        let dsu = PartitionDsu {
            parent: HashMap::default(),
            visited: Default::default(),
        };
        dsu.run(graph, target_rdd);
        let cur_id = 0;
        let ids: HashMap<_, _> = HashMap::default();
        let res: HashMap<_, _> = HashMap::default();
        for (rpid, _) in dsu.parent.into_iter() {
            let p = dsu.par(rpid);
            let id = match ids.get(&p) {
                Some(id) => *id,
                None => {
                    cur_id += 1;
                    ids.insert(p, cur_id);
                    cur_id
                }
            };
            res.insert(rpid, GroupId(id));
        }
        res
    }

    impl PartitionDsu {
        fn par(&mut self, x: RddPartitionId) -> RddPartitionId {
            let p = match self.parent.get(&x) {
                Some(p) => {
                    if x == *p {
                        x
                    } else {
                        self.par(*p)
                    }
                }
                None => x,
            };
            self.parent.insert(x, p);
            p
        }

        fn join(&mut self, a: RddPartitionId, b: RddPartitionId) {
            let a = self.par(a);
            let b = self.par(b);
            if a == b {
                return;
            }
            self.parent.insert(b, a);
        }

        fn dfs(&mut self, graph: &Graph, rpid: RddPartitionId) {
            if self.visited.contains(&rpid) {
                return;
            }
            self.visited.insert(rpid);
            let rdd = graph.get_rdd(rpid.rdd_id).unwrap();
            match rdd.rdd_dependency() {
                crate::core::rdd::Dependency::Narrow(par_rdd_id) => {
                    let parent_rpid = RddPartitionId {
                        rdd_id: par_rdd_id,
                        partition_id: rpid.partition_id,
                    };
                    self.join(rpid, parent_rpid);
                    self.dfs(graph, parent_rpid)
                }
                crate::core::rdd::Dependency::Wide(par_rdd_id) => {
                    // no join here at all. These guys can be on different partitions
                    let par_rdd = graph.get_rdd(par_rdd_id).unwrap();
                    for pid in 0..par_rdd.partitions_num() {
                        self.dfs(
                            graph,
                            RddPartitionId {
                                rdd_id: par_rdd_id,
                                partition_id: pid,
                            },
                        );
                    }
                }
                crate::core::rdd::Dependency::No => {}
            }
        }

        fn run(&mut self, graph: &Graph, target_rdd_id: RddId) {
            for pid in 0..graph.get_rdd(target_rdd_id).unwrap().partitions_num() {
                self.dfs(
                    graph,
                    RddPartitionId {
                        rdd_id: target_rdd_id,
                        partition_id: pid,
                    },
                )
            }
        }
    }
}

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct TaskId(usize);

pub struct DagScheduler {
    /// used to receive jobs from user code
    job_receiver: mpsc::Receiver<Job>,
    /// used to send tasks to taskscheduler
    task_sender: mpsc::Sender<DagMessage>,
    /// used to receive events from workers
    event_receiver: mpsc::Receiver<WorkerEvent>,
    /// number of workers
    n_workers: usize,
    /// used to generate unique ids for tasks
    task_id_ctr: usize,

    /// id -> task
    tasks: HashMap<TaskId, Task>,
    /// tasks waiting for some dependency
    waiting_tasks: HashSet<TaskId>,
    /// tasks ready to run
    running_tasks: HashSet<TaskId>,
    /// idk
    _failed_tasks: HashSet<TaskId>,
    /// number of buckets left to receive at this wide rdd partition
    bucket_aggr_tracker: HashMap<RddPartitionId, usize>,
    bucket_aggr_ids: HashMap<RddPartitionId, HashSet<usize>>,
    /// task -> all the wide rdd partitions(inputs) that are used in this tasks
    task_deps: HashMap<TaskId, Vec<RddPartitionId>>,
    /// wide rdd partition -> all the tasks that depend on it
    childs: HashMap<RddPartitionId, Vec<TaskId>>,
    /// wide rdd partition -> is cached
    cached: HashMap<RddPartitionId, bool>,
    /// wide rdd id -> all of its tasks
    stage_tasks: HashMap<RddId, Vec<TaskId>>,

    /// dsu groups
    groups: HashMap<RddPartitionId, GroupId>,
    /// groups -> worker_id where all the partitions in this group should be materialized
    worker_assignment: HashMap<GroupId, usize>,
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
            tasks: Default::default(),
            waiting_tasks: Default::default(),
            running_tasks: Default::default(),
            _failed_tasks: Default::default(),
            bucket_aggr_tracker: Default::default(),
            childs: Default::default(),
            cached: Default::default(),
            groups: Default::default(),
            worker_assignment: Default::default(),
            task_deps: Default::default(),
            task_id_ctr: Default::default(),
            stage_tasks: Default::default(),
            bucket_aggr_ids: Default::default(),
        }
    }

    fn new_task_id(&mut self) -> TaskId {
        self.task_id_ctr += 1;
        TaskId(self.task_id_ctr)
    }

    fn get_missing_deps(&self, rpid: &TaskId) -> Vec<RddPartitionId> {
        self.task_deps
            .get(rpid)
            .unwrap()
            .iter()
            .filter(|dep_rpid| *self.cached.get(dep_rpid).unwrap_or(&false))
            .cloned()
            .collect()
    }

    fn is_all_deps_cached(&self, rpid: &TaskId) -> bool {
        !self.get_missing_deps(rpid).is_empty()
    }

    fn get_assigned_worker(&mut self, group: GroupId) -> usize {
        *self.worker_assignment.entry(group).or_insert_with(|| {
            let rng = rand::thread_rng();
            rng.gen()
        })
    }

    fn fill_worker_information(&mut self, task: &mut Task) {
        match &mut task.kind {
            TaskKind::ResultTask(result_task) => {
                task.worker_id = self
                    .get_assigned_worker(*self.groups.get(&result_task.rdd_partition_id).unwrap());
            }
            TaskKind::WideTask(wide_task) => {
                task.worker_id = self.get_assigned_worker(
                    *self
                        .groups
                        .get(&RddPartitionId {
                            rdd_id: wide_task.narrow_rdd_id,
                            partition_id: wide_task.narrow_partition_id,
                        })
                        .unwrap(),
                );
                wide_task.target_workers.iter_mut().enumerate().for_each(
                    |(wide_partition_id, worker)| {
                        let rpid = RddPartitionId {
                            rdd_id: wide_task.wide_rdd_id,
                            partition_id: wide_partition_id,
                        };
                        *worker = self.get_assigned_worker(*self.groups.get(&rpid).unwrap());
                    },
                )
            }
        };
    }

    async fn try_submit_task(&mut self, task_id: TaskId) {
        let already_submitted =
            self.waiting_tasks.contains(&task_id) || self.running_tasks.contains(&task_id);
        if !already_submitted {
            let todo_deps = self.get_missing_deps(&task_id);
            if todo_deps.is_empty() {
                // All dependecies are ready, commence task execution!
                let mut task = self.tasks.get(&task_id).unwrap().clone();
                self.fill_worker_information(&mut task);
                self.running_tasks.insert(task.id);
                self.task_sender.send(DagMessage::SubmitTask(task));
            } else {
                // Try to run dependecies, put task on waiting queue
                self.waiting_tasks.insert(task_id);
                for dep_rpid in todo_deps {
                    for task_id in self.stage_tasks.get(&dep_rpid.rdd_id).unwrap().clone() {
                        self.try_submit_task(task_id);
                    }
                }
            }
        }
    }

    /// stores taska and creates parent->child child->parent links
    fn store_task(&mut self, task_id: TaskId, task: Task, wide_dependecies: Vec<RddPartitionId>) {
        self.tasks.insert(task_id, task);
        wide_dependecies.iter().for_each(|rpid| {
            self.childs
                .entry(*rpid)
                .or_insert_with(|| Vec::new())
                .push(task_id)
        });
        self.task_deps.insert(task_id, wide_dependecies);
    }

    fn create_stage_tasks(&mut self, graph: &Graph, rdd_id: RddId) {
        let rdd = graph.get_rdd(rdd_id).unwrap();
        match rdd.rdd_dependency() {
            Dependency::Narrow(dep_rdd_id) => self.create_stage_tasks(graph, dep_rdd_id),
            Dependency::Wide(dep_rdd_id) => {
                self.create_stage_tasks(graph, rdd_id);
                let stage_tasks = Vec::new();
                let prev_rdd = graph.get_rdd(dep_rdd_id).unwrap();
                for narrow_partition_id in 0..prev_rdd.partitions_num() {
                    let task_id = self.new_task_id();
                    let wide_task = WideTask {
                        wide_rdd_id: rdd_id,
                        narrow_rdd_id: dep_rdd_id,
                        narrow_partition_id,
                        target_workers: vec![0; rdd.partitions_num()],
                    };
                    let task = Task {
                        id: task_id,
                        worker_id: 0,
                        kind: TaskKind::WideTask(wide_task),
                    };
                    let task_wide_dependecies = self.get_direct_dependencies(
                        graph,
                        RddPartitionId {
                            rdd_id: dep_rdd_id,
                            partition_id: narrow_partition_id,
                        },
                    );
                    self.store_task(task_id, task, task_wide_dependecies);
                    stage_tasks.push(task_id);
                }
                for wide_partition_id in 0..rdd.partitions_num() {
                    self.bucket_aggr_tracker.insert(
                        RddPartitionId {
                            rdd_id,
                            partition_id: wide_partition_id,
                        },
                        prev_rdd.partitions_num(),
                    );
                }
                self.stage_tasks.insert(rdd_id, stage_tasks);
            }
            Dependency::No => {}
        }
    }

    // Returns direct wide partition dependecies
    fn get_direct_dependencies(
        &mut self,
        graph: &Graph,
        node: RddPartitionId,
    ) -> Vec<RddPartitionId> {
        let rdd = graph.get_rdd(node.rdd_id).unwrap();
        match rdd.rdd_dependency() {
            Dependency::Narrow(dep_rdd_id) => self.get_direct_dependencies(
                graph,
                RddPartitionId {
                    rdd_id: dep_rdd_id,
                    partition_id: node.partition_id,
                },
            ),
            Dependency::Wide(dep_rdd_id) => {
                vec![node]
            }
            Dependency::No => Vec::new(),
        }
    }

    async fn process_bucket_receive_event(&mut self, e: BucketReceivedEvent) {
        let received_bucket_set = self
            .bucket_aggr_ids
            .entry(e.wide_partition)
            .or_insert_with(|| HashSet::default());
        if received_bucket_set.contains(&e.narrow_partition_id) {
            println!("Received duplicate bucket id: {:?}", e);
            return;
        }
        received_bucket_set.insert(e.narrow_partition_id);
        let buckets_left = self.bucket_aggr_tracker.get_mut(&e.wide_partition).unwrap();
        *buckets_left -= 1;
        if *buckets_left == 0 {
            self.cached.insert(e.wide_partition, true);
            for task_id in self.childs.get(&e.wide_partition).unwrap().clone() {
                // try to schedule this task if all the deps have freed up
                self.waiting_tasks.remove(&task_id);
                self.try_submit_task(task_id).await;
            }
        }
    }

    pub async fn start(mut self) {
        println!("Dag scheduler is running!");
        while let Some(job) = self.job_receiver.recv().await {
            println!("new job received target_rdd_id={:?}", job.target_rdd_id);

            let target_rdd = job.graph.get_rdd(job.target_rdd_id).expect("rdd not found");

            let groups = find_groups(&job.graph, job.target_rdd_id);
            self.groups = groups;
            self.create_stage_tasks(&job.graph, job.target_rdd_id);

            let mut result_stage_tasks = Vec::new();
            for partition_id in 0..target_rdd.partitions_num() {
                let rpid = RddPartitionId {
                    rdd_id: job.target_rdd_id,
                    partition_id,
                };
                let result_task = ResultTask {
                    rdd_partition_id: rpid,
                };
                let task_id = self.new_task_id();
                let task = Task {
                    id: task_id,
                    worker_id: 0,
                    kind: TaskKind::ResultTask(result_task),
                };
                let deps = self.get_direct_dependencies(&job.graph, rpid);
                self.store_task(task_id, task, deps);
                result_stage_tasks.push(task_id);
            }

            {
                // send over graph to task scheduler
                self.task_sender
                    .send(DagMessage::NewGraph(job.graph.clone()))
                    .await
                    .expect("can't send graph to task scheduler");
            }
            // TODO: make sure graph is delivered to all workers

            // mby avoid clone
            for task_id in self.stage_tasks.get(&job.target_rdd_id).unwrap().clone() {
                self.try_submit_task(task_id);
            }

            let mut result_v: Vec<Option<Box<dyn Any + Send>>> = Vec::new();
            for _ in 0..target_rdd.partitions_num() {
                result_v.push(None);
            }
            let mut num_received = 0;
            while let Some(result) = self.event_receiver.recv().await {
                match result {
                    WorkerEvent::Success(task, serialized_rdd_data) => {
                        if let TaskKind::ResultTask(result_task) = task.kind {
                            let materialized_partition =
                                target_rdd.deserialize_raw_data(serialized_rdd_data);
                            assert!(result_v[result_task.rdd_partition_id.partition_id].is_none());
                            result_v[result_task.rdd_partition_id.partition_id] =
                                Some(materialized_partition);
                            num_received += 1;
                            if num_received == target_rdd.partitions_num() {
                                let final_results =
                                    result_v.into_iter().map(|v| v.unwrap()).collect();
                                job.materialized_data_channel
                                    .send(final_results)
                                    .expect("can't returned materialied result to spark");
                                break;
                            }
                        } else {
                            assert_eq!(serialized_rdd_data.len(), 0);
                        }
                    }
                    WorkerEvent::Fail(_) => panic!("Task somehow failed?"),
                    WorkerEvent::BucketReceived(e) => self.process_bucket_receive_event(e).await,
                }
            }
        }
    }
}
