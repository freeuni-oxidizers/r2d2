pub mod rdd {

    use std::{
        any::Any,
        marker::PhantomData,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use serde::{de::DeserializeOwned, Deserialize, Serialize};

    use super::cache::ResultCache;

    // TODO: maybe add uuid so that we can't pass one rdd index to another context
    #[derive(Serialize, Deserialize)]
    pub struct RddIndex<T> {
        pub id: RddId,
        #[serde(skip)]
        _data: PhantomData<T>,
    }

    impl<T> RddIndex<T> {
        pub fn new(id: RddId) -> Self {
            RddIndex {
                id,
                _data: PhantomData::default(),
            }
        }
    }

    impl<T> Clone for RddIndex<T> {
        fn clone(&self) -> RddIndex<T> {
            RddIndex::new(self.id)
        }
    }

    impl<T> Copy for RddIndex<T> {}

    #[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
    pub struct RddId(usize);
    impl RddId {
        pub fn new() -> RddId {
            static COUNTER: AtomicUsize = AtomicUsize::new(1);
            RddId(COUNTER.fetch_add(1, Ordering::Relaxed))
        }
    }

    #[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
    pub struct RddPartitionId {
        pub rdd_id: RddId,
        pub partition_id: usize,
    }

    pub trait Data: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}

    impl<T> Data for T where T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}

    // trait DataFetcher {
    //     fn fetch_owned<T>(&mut self, idx: RddIndex<T>, partition_id: usize) -> Vec<T>;
    //     fn fetch_borrowed<T>(&self, idx: RddIndex<T>, partition_id: usize) -> &[T];
    //     fn post_result<T>(&mut self, idx: RddIndex<T>, partition_id: usize, data: Vec<T>);
    // }

    pub enum RddType {
        Narrow,
        Wide,
    }

    pub trait RddSerde {
        /// serialize data returned by this rdd in a form which can be sent over the network
        fn serialize_raw_data(&self, raw_data: &(dyn Any + Send)) -> Vec<u8>;
        /// deserialize data in a way which can be ingested into following rdds
        fn deserialize_raw_data(&self, serialized_data: Vec<u8>) -> Box<dyn Any + Send>;
    }

    pub trait RddWork {
        /// This expects that all the deps have already been put inside the cache
        /// Ownership of results is passed back to context!
        // TODO: Is being generic over DataFetcher here fine???
        // maybe do enum_dispatch
        fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any + Send>;
    }

    /// methods independent of `Item` type
    pub trait RddBase:
        RddBaseClone
        + Send
        + Sync
        + RddWork
        + RddSerde
        + serde_traitobject::Serialize
        + serde_traitobject::Deserialize
    {
        /// Fetch unique id for this rdd
        fn id(&self) -> RddId;

        /// rdd dependencies
        fn deps(&self) -> Vec<RddId>;

        fn rdd_type(&self) -> RddType;

        fn partitions_num(&self) -> usize;
    }

    /// Magic incantaions to make `dyn RddBase` `Clone`
    pub trait RddBaseClone {
        fn clone_box(&self) -> Box<dyn RddBase>;
    }

    /// Magic incantaions to make `dyn RddBase` `Clone`
    impl<T> RddBaseClone for T
    where
        T: 'static + RddBase + Clone,
    {
        fn clone_box(&self) -> Box<dyn RddBase> {
            Box::new(self.clone())
        }
    }

    /// Magic incantaions to make `dyn RddBase` `Clone`
    impl Clone for Box<dyn RddBase> {
        fn clone(&self) -> Box<dyn RddBase> {
            self.clone_box()
        }
    }

    /// methods for Rdd which are dependent on `Item` type
    trait TypedRdd {
        type Item: Data;

        fn work(&self, cache: &ResultCache, partition_id: usize) -> Vec<Self::Item>;
    }

    impl<T> RddSerde for T
    where
        T: TypedRdd,
    {
        fn serialize_raw_data(&self, raw_data: &(dyn Any + Send)) -> Vec<u8> {
            let data: &Vec<T::Item> = raw_data.downcast_ref().unwrap();
            serde_json::to_vec(data).unwrap()
        }

        fn deserialize_raw_data(&self, serialized_data: Vec<u8>) -> Box<dyn Any + Send> {
            let data: Vec<T::Item> = serde_json::from_slice(&serialized_data).unwrap();
            Box::new(data)
        }
    }

    impl<T> RddWork for T
    where
        T: TypedRdd,
    {
        fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any + Send> {
            Box::new(Self::work(&self, cache, partition_id))
        }
    }

    pub mod data_rdd {

        use serde::{Deserialize, Serialize};

        use crate::rdd::cache::ResultCache;

        use super::{Data, RddBase, RddId, RddType, TypedRdd};

        #[derive(Clone, Serialize, Deserialize)]
        pub struct DataRdd<T> {
            pub id: RddId,
            pub partitions_num: usize,
            pub data: Vec<Vec<T>>,
        }

        impl<T> TypedRdd for DataRdd<T>
        where
            T: Data,
        {
            type Item = T;

            fn work(&self, _cache: &ResultCache, partition_id: usize) -> Vec<Self::Item> {
                self.data[partition_id].clone()
            }
        }

        impl<T> RddBase for DataRdd<T>
        where
            T: Data + Clone,
        {
            fn id(&self) -> RddId {
                self.id
            }

            fn deps(&self) -> Vec<RddId> {
                vec![]
            }

            fn rdd_type(&self) -> RddType {
                RddType::Narrow
            }

            fn partitions_num(&self) -> usize {
                self.partitions_num
            }
        }
    }

    pub mod map_rdd {

        use serde::{Deserialize, Serialize};

        use crate::rdd::cache::ResultCache;

        use super::{Data, RddBase, RddId, RddIndex, RddType, TypedRdd};

        // TODO: maybe no pub?
        #[derive(Clone, Serialize, Deserialize)]
        pub struct MapRdd<T, U> {
            pub id: RddId,
            pub prev: RddIndex<T>,
            pub partitions_num: usize,
            #[serde(with = "serde_fp")]
            pub map_fn: fn(&T) -> U,
        }

        impl<T, U> TypedRdd for MapRdd<T, U>
        where
            T: Data,
            U: Data,
        {
            type Item = U;

            fn work(&self, cache: &ResultCache, partition_id: usize) -> Vec<Self::Item> {
                let v = cache.get_as(self.prev, partition_id).unwrap();
                let g: Vec<U> = v.iter().map(self.map_fn).collect();
                g
            }
        }

        impl<T, U> RddBase for MapRdd<T, U>
        where
            T: Data,
            U: Data,
        {
            fn id(&self) -> RddId {
                self.id
            }

            fn deps(&self) -> Vec<RddId> {
                vec![self.prev.id]
            }

            fn rdd_type(&self) -> super::RddType {
                RddType::Narrow
            }

            fn partitions_num(&self) -> usize {
                self.partitions_num
            }
        }
    }

    pub mod filter_rdd {
        use serde::{Deserialize, Serialize};

        use super::{Data, RddBase, RddId, RddIndex, RddType, TypedRdd};

        #[derive(Clone, Serialize, Deserialize)]
        pub struct FilterRdd<T> {
            pub id: RddId,
            pub partitions_num: usize,
            pub prev: RddIndex<T>,
            #[serde(with = "serde_fp")]
            pub filter_fn: fn(&T) -> bool,
        }

        impl<T> TypedRdd for FilterRdd<T>
        where
            T: Data,
        {
            type Item = T;

            fn work(
                &self,
                cache: &crate::rdd::cache::ResultCache,
                partition_id: usize,
            ) -> Vec<Self::Item> {
                let v = cache.get_as(self.prev, partition_id).unwrap();
                let g: Vec<T> = v.to_vec().into_iter().filter(self.filter_fn).collect();
                g
            }
        }

        impl<T> RddBase for FilterRdd<T>
        where
            T: Data,
        {
            fn id(&self) -> RddId {
                self.id
            }

            fn deps(&self) -> Vec<RddId> {
                vec![self.prev.id]
            }

            fn rdd_type(&self) -> RddType {
                RddType::Narrow
            }

            fn partitions_num(&self) -> usize {
                self.partitions_num
            }
        }
    }
}

mod cache {
    use std::{any::Any, collections::HashMap};

    use super::rdd::{Data, RddId, RddIndex, RddPartitionId};

    #[derive(Default)]
    pub struct ResultCache {
        data: HashMap<RddPartitionId, Box<dyn Any + Send>>,
    }

    impl ResultCache {
        pub fn has(&self, id: RddPartitionId) -> bool {
            self.data.contains_key(&id)
        }

        pub fn put(&mut self, id: RddPartitionId, data: Box<dyn Any + Send>) {
            self.data.insert(id, data);
        }

        pub fn get_as<T: Data>(&self, rdd: RddIndex<T>, partition_id: usize) -> Option<&[T]> {
            self.data
                .get(&RddPartitionId {
                    rdd_id: rdd.id,
                    partition_id,
                })
                .map(|b| b.downcast_ref::<Vec<T>>().unwrap().as_slice())
        }

        pub fn get_as_any(&self, rdd_id: RddId, partition_id: usize) -> Option<&(dyn Any + Send)> {
            self.data
                .get(&RddPartitionId {
                    rdd_id,
                    partition_id,
                })
                .map(|b| b.as_ref())
        }
    }
}

pub mod graph {
    use std::{collections::HashMap, fmt::Debug};

    use serde::{Deserialize, Serialize};

    use super::rdd::{RddBase, RddId};

    // TODO: can't easily have custom deserialization for hashmap value sadge :(
    #[derive(Clone, Serialize, Deserialize)]
    struct RddHolder(#[serde(with = "serde_traitobject")] Box<dyn RddBase>);

    #[derive(Clone, Default, Serialize, Deserialize)]
    pub struct Graph {
        /// All the rdd's are stored in the context in here
        rdds: HashMap<RddId, RddHolder>,
        // #[serde(skip)]
        // cache: ResultCache,
    }

    impl Debug for Graph {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Graph")
                .field("n_nodes", &self.rdds.len())
                .finish()
        }
    }

    impl Graph {
        pub fn store_new_rdd<R: RddBase + 'static>(&mut self, rdd: R) {
            self.rdds.insert(rdd.id(), RddHolder(Box::new(rdd)));
        }

        pub fn get_rdd(&self, id: RddId) -> Option<&dyn RddBase> {
            self.rdds.get(&id).map(|x| x.0.as_ref())
        }

        pub fn contains(&self, id: RddId) -> bool {
            self.rdds.contains_key(&id)
        }
    }
}

pub mod context {
    use super::rdd::{Data, RddIndex};

    /// All these methods use interior mutability to keep state
    pub trait Context {
        // fn resolve<T: Data>(&mut self, rdd: RddIndex<T>);
        // fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> Vec<T>;
        fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> U) -> RddIndex<U>;
        fn filter<T: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> bool) -> RddIndex<T>;
        fn new_from_list<T: Data + Clone>(&mut self, data: Vec<Vec<T>>) -> RddIndex<T>;

        // fn store_rdd<T: RddBase + 'static>(&self, rdd: T) -> Rc<T>;
        // fn receive_serialized(&self, id: RddId, serialized_data: String);
    }
}

pub mod executor {
    use crate::rdd::rdd::RddType;

    use super::{cache::ResultCache, graph::Graph, rdd::RddPartitionId};

    pub struct Executor {
        /// Cache which stores full partitions ready for next rdd
        // this field is basically storing Vec<T>s where T can be different for each id we are not
        // doing Vec<Any> for perf reasons. downcasting is not free
        // This should not be serialized because all workers have this is just a cache
        pub cache: ResultCache,
        /// Cache which is used to store partial results of shuffle operations
        // TODO: buckets
        pub takeout: ResultCache,
    }

    impl Executor {
        pub fn new() -> Self {
            Self {
                cache: ResultCache::default(),
                takeout: ResultCache::default(),
            }
        }
        pub fn resolve(&mut self, graph: &Graph, id: RddPartitionId) {
            assert!(graph.contains(id.rdd_id), "id not found in context");
            if self.cache.has(id) {
                return;
            }

            let rdd_type = graph.get_rdd(id.rdd_id).unwrap().rdd_type();
            // First resolve all the deps
            for dep in graph.get_rdd(id.rdd_id).unwrap().deps() {
                match rdd_type {
                    RddType::Narrow => {
                        self.resolve(
                            graph,
                            RddPartitionId {
                                rdd_id: dbg!(dep),
                                partition_id: dbg!(id.partition_id),
                            },
                        );
                        // print!("{}", id.partition_id)
                    }
                    RddType::Wide => {
                        let dep_partitions_num = graph.get_rdd(id.rdd_id).unwrap().partitions_num();
                        for partition_id in 0..dep_partitions_num {
                            self.resolve(
                                graph,
                                RddPartitionId {
                                    rdd_id: dep,
                                    partition_id,
                                },
                            );
                        }
                    }
                }
                let res = graph
                    .get_rdd(id.rdd_id)
                    .unwrap()
                    .work(&self.cache, id.partition_id);
                self.cache.put(id, res);
            }
            let res = graph
                .get_rdd(id.rdd_id)
                .unwrap()
                .work(&self.cache, id.partition_id);
            self.cache.put(id, res);
        }
    }
}

mod dag_scheduler {
    use std::any::Any;

    use tokio::sync::{mpsc, oneshot};

    use crate::rdd::task_scheduler::{Task, TaskSet};

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
                for partition_id in 0..target_rdd.partitions_num() {
                    task_set.tasks.push(Task {
                        final_rdd: job.target_rdd_id,
                        partition_id,
                        num_partitions: target_rdd.partitions_num(),
                        preffered_worker_id: partition_id % self.n_workers,
                    })
                }
                let graph = job.graph.clone();
                self.task_sender
                    .send(DagMessage::NewGraph(graph))
                    .await
                    .expect("can't send graph to task scheduler");

                self.task_sender
                    .send(DagMessage::SubmitTaskSet(task_set))
                    .await
                    .expect("can't send graph to task scheduler");

                let mut result_v: Vec<Option<Box<dyn Any + Send>>> = Vec::new();
                for _ in 0..target_rdd.partitions_num() {
                    result_v.push(None);
                }
                let mut num_received = 0;
                while let Some(result) = self.event_receiver.recv().await {
                    match result {
                        WorkerEvent::Success(task, serialized_rdd_data) => {
                            let materialized_partition =
                                target_rdd.deserialize_raw_data(serialized_rdd_data);
                            assert!(result_v[task.partition_id].is_none());
                            result_v[task.partition_id] = Some(materialized_partition);
                            num_received += 1;
                            if num_received == target_rdd.partitions_num() {
                                let final_results =
                                    result_v.into_iter().map(|v| v.unwrap()).collect();
                                job.materialized_data_channel
                                    .send(final_results)
                                    .expect("can't returned materialied result to spark");
                                break;
                            }
                        }
                        WorkerEvent::Fail(_) => panic!("Task somehow failed?"),
                    }
                }
            }
        }
    }
}

pub mod task_scheduler {

    use std::fmt::Debug;

    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc;

    use super::{graph::Graph, rdd::RddId};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum FailReason {
        All,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum WorkerEvent {
        Success(Task, Vec<u8>),
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
        pub final_rdd: RddId,
        pub partition_id: usize,
        pub num_partitions: usize,
        pub preffered_worker_id: usize,
    }
}

pub mod spark {
    use std::{fmt::Debug, process::exit};

    use tokio::sync::{mpsc, oneshot};

    use crate::{master::MasterService, worker, Config};

    use super::{
        context::Context,
        dag_scheduler::{DagScheduler, Job},
        graph::Graph,
        rdd::{data_rdd::DataRdd, filter_rdd::FilterRdd, map_rdd::MapRdd, Data, RddId, RddIndex},
        task_scheduler::{DagMessage, TaskScheduler, WorkerEvent, WorkerMessage},
    };

    // TODO(zvikinoza): extract this to sep file
    // and use SparkContext as lib from rdd-simple
    pub struct Spark {
        /// Resposible for storing current graph build up by user
        graph: Graph,
        job_channel: mpsc::Sender<Job>,
    }

    impl Debug for Job {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Job")
                // .field("graph", &self.graph)
                // .field("target_rdd", &self.target_rdd)
                // .field("materialized_receiver", &self.materialized_receiver)
                .finish()
        }
    }

    impl Spark {
        pub async fn new(config: Config) -> Self {
            // start DagScheduler
            // start TaskScheduler
            // start RpcServer

            // let (tx, _) = broadcast::channel(16);
            // let mut sc = Self {
            //     sc: SparkContext::new(),
            //     tx,
            //     wait_handle: None,
            // };
            if !config.master {
                let jh = tokio::spawn(async move {
                    worker::start(config.id).await; // this fn call never returns
                });
                jh.await.unwrap();
                exit(0);
            }
            // sc

            let (job_tx, job_rx) = mpsc::channel::<Job>(32);
            let (dag_msg_tx, dag_msg_rx) = mpsc::channel::<DagMessage>(32);
            let (dag_evt_tx, dag_evt_rx) = mpsc::channel::<WorkerEvent>(32);

            // task scheduler  <-------------> rpc server
            //                 ---Message---->
            //                 <----Event----
            let (wrk_txs, wrk_rxs): (Vec<_>, Vec<_>) = (0..config.n_workers)
                .map(|_| mpsc::channel::<WorkerMessage>(32))
                .unzip();
            let (wrk_evt_tx, wrk_evt_rx) = mpsc::channel::<WorkerEvent>(32);

            let dag_scheduler = DagScheduler::new(job_rx, dag_msg_tx, dag_evt_rx, config.n_workers);
            println!("dag scheduler should start running soon!");
            tokio::spawn(async move { dag_scheduler.start().await });
            let task_scheduler = TaskScheduler::new(dag_msg_rx, dag_evt_tx, wrk_txs, wrk_evt_rx);
            tokio::spawn(async move { task_scheduler.start().await });
            let rpc_server = MasterService::new(wrk_rxs, wrk_evt_tx);
            // TODO: Maybe aggregate some way to shutdown all the schedulers and rpc server
            // together and keep handle on that in `Spark`
            tokio::spawn(async move { rpc_server.start().await });

            Spark {
                graph: Graph::default(),
                job_channel: job_tx,
            }
        }
    }

    impl Spark {
        pub async fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> Vec<T> {
            let (mat_tx, mat_rx) = oneshot::channel();
            let job = Job {
                graph: self.graph.clone(),
                target_rdd_id: rdd.id,
                materialized_data_channel: mat_tx,
            };
            self.job_channel.send(job).await.unwrap();
            let v = mat_rx.await.expect("couldn't get result");
            return v
                .into_iter()
                .map(|vany| (*vany.downcast::<Vec<T>>().unwrap()))
                .flatten()
                .collect();
        }
    }

    impl Context for Spark {
        fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> U) -> RddIndex<U> {
            let id = RddId::new();
            let partitions_num = self.graph.get_rdd(rdd.id).unwrap().partitions_num();
            self.graph.store_new_rdd(MapRdd {
                id,
                partitions_num,
                prev: rdd,
                map_fn: f,
            });
            RddIndex::new(id)
        }

        fn filter<T: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> bool) -> RddIndex<T> {
            let id = RddId::new();
            let partitions_num = self.graph.get_rdd(rdd.id).unwrap().partitions_num();
            self.graph.store_new_rdd(FilterRdd {
                id,
                partitions_num,
                prev: rdd,
                filter_fn: f,
            });
            RddIndex::new(id)
        }

        fn new_from_list<T: Data + Clone>(&mut self, data: Vec<Vec<T>>) -> RddIndex<T> {
            let id = RddId::new();
            self.graph.store_new_rdd(DataRdd {
                id,
                partitions_num: data.len(),
                data,
            });
            RddIndex::new(id)
        }
    }
}
