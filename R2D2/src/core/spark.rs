use std::{fmt::Debug, process::exit};

use tokio::sync::{mpsc, oneshot};

use crate::{master::MasterService, worker, Args, Config};

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
    pub async fn new(args: Args) -> Self {
        // start DagScheduler
        // start TaskScheduler
        // start RpcServer

        // let (tx, _) = broadcast::channel(16);
        // let mut sc = Self {
        //     sc: SparkContext::new(),
        //     tx,
        //     wait_handle: None,
        // };

        let config: Config =
            toml::from_str(&std::fs::read_to_string("spark.toml").unwrap()).unwrap();

        if !args.master {
            let jh = tokio::spawn(async move {
                worker::start(args.id, args.port, config.master_addr.clone()).await; // this fn call never returns
            });
            jh.await.unwrap();
            exit(0);
        }
        // sc
        // read worker ports from config
        let (job_tx, job_rx) = mpsc::channel::<Job>(32);
        let (dag_msg_tx, dag_msg_rx) = mpsc::channel::<DagMessage>(32);
        let (dag_evt_tx, dag_evt_rx) = mpsc::channel::<WorkerEvent>(32);

        // task scheduler  <-------------> rpc server
        //                 ---Message---->
        //                 <----Event----
        let n_workers = config.worker_addrs.len();
        let (wrk_txs, wrk_rxs): (Vec<_>, Vec<_>) = (0..n_workers)
            .map(|_| mpsc::channel::<WorkerMessage>(32))
            .unzip();
        let (wrk_evt_tx, wrk_evt_rx) = mpsc::channel::<WorkerEvent>(32);

        let dag_scheduler = DagScheduler::new(job_rx, dag_msg_tx, dag_evt_rx, n_workers);
        println!("dag scheduler should start running soon!");
        tokio::spawn(async move { dag_scheduler.start().await });
        let task_scheduler = TaskScheduler::new(dag_msg_rx, dag_evt_tx, wrk_txs, wrk_evt_rx);
        tokio::spawn(async move { task_scheduler.start().await });
        let rpc_server = MasterService::new(wrk_rxs, wrk_evt_tx);
        // TODO: Maybe aggregate some way to shutdown all the schedulers and rpc server
        // together and keep handle on that in `Spark`
        tokio::spawn(async move { rpc_server.start(args.port).await });

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
        v.into_iter()
            .flat_map(|vany| (*vany.downcast::<Vec<T>>().unwrap()))
            .collect()
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
