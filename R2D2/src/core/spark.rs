use std::{fmt::Debug, ops::Add, process::exit};

use tokio::sync::{mpsc, oneshot};

use crate::{core::rdd::shuffle_rdd::Aggregator, master::MasterService, worker, Args, Config};

use self::{
    group_by::GroupByAggregator,
    partition_by::{DummyPartitioner, FinishingFlatten, MapUsingPartitioner},
    sum_by_key::SumByKeyAggregator,
};

use super::{
    context::Context,
    dag_scheduler::{DagScheduler, Job},
    graph::Graph,
    rdd::{
        data_rdd::DataRdd,
        filter_rdd::{FilterRdd, FnPtrFilterer},
        flat_map_rdd::{FlatMapRdd, FnPtrFlatMapper},
        map_rdd::{FnPtrMapper, MapRdd, Mapper},
        shuffle_rdd::{Partitioner, ShuffleRdd},
        Data, RddId, RddIndex,
    },
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
                worker::start(args.id, args.port, config.master_addr.clone()).await;
                // this fn call never returns
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
mod sum_by_key {
    use std::{marker::PhantomData, ops::Add};

    use serde::{Deserialize, Serialize};

    use crate::core::rdd::{shuffle_rdd::Aggregator, Data};

    #[derive(Clone, Serialize, Deserialize)]
    pub struct SumByKeyAggregator<V> {
        #[serde(skip)]
        _value: PhantomData<V>,
    }

    impl<V> SumByKeyAggregator<V> {
        pub fn new() -> Self {
            SumByKeyAggregator {
                _value: PhantomData,
            }
        }
    }

    impl<V> Default for SumByKeyAggregator<V> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<V: Data + Add<Output = V> + Default> Aggregator for SumByKeyAggregator<V> {
        type Value = V;

        type Combiner = V;

        fn create_combiner(&self) -> Self::Combiner {
            V::default()
        }

        fn merge_value(&self, value: Self::Value, combiner: Self::Combiner) -> Self::Combiner {
            combiner + value
        }

        fn merge_combiners(
            &self,
            combiner1: Self::Combiner,
            combiner2: Self::Combiner,
        ) -> Self::Combiner {
            combiner1 + combiner2
        }
    }
}

mod group_by {
    use std::marker::PhantomData;

    use serde::{Deserialize, Serialize};

    use crate::core::rdd::{shuffle_rdd::Aggregator, Data};

    #[derive(Clone, Serialize, Deserialize)]
    pub struct GroupByAggregator<V> {
        #[serde(skip)]
        _value: PhantomData<V>,
    }

    impl<V> GroupByAggregator<V> {
        pub fn new() -> Self {
            GroupByAggregator {
                _value: PhantomData,
            }
        }
    }

    impl<V> Default for GroupByAggregator<V> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<V: Data> Aggregator for GroupByAggregator<V> {
        type Value = V;

        type Combiner = Vec<V>;

        fn create_combiner(&self) -> Self::Combiner {
            Vec::new()
        }

        fn merge_value(&self, value: Self::Value, mut combiner: Self::Combiner) -> Self::Combiner {
            combiner.push(value);
            combiner
        }

        fn merge_combiners(
            &self,
            mut combiner1: Self::Combiner,
            combiner2: Self::Combiner,
        ) -> Self::Combiner {
            combiner1.extend(combiner2);
            combiner1
        }
    }
}

impl Context for Spark {
    fn shuffle<K, V, C, P, A>(
        &mut self,
        rdd: RddIndex<(K, V)>,
        partitioner: P,
        aggregator: A,
    ) -> RddIndex<(K, C)>
    where
        K: Data + Eq + std::hash::Hash,
        V: Data,
        C: Data,
        P: Partitioner<Key = K>,
        A: Aggregator<Value = V, Combiner = C>,
    {
        let id = RddId::new();
        let idx = RddIndex::new(id);
        self.graph.store_new_rdd(ShuffleRdd {
            idx,
            prev: rdd,
            partitioner,
            aggregator,
        });
        idx
    }

    // (K, V) -> (K, Vec<V>)
    fn group_by<K, V, P>(&mut self, rdd: RddIndex<(K, V)>, partitioner: P) -> RddIndex<(K, Vec<V>)>
    where
        K: Data + Eq + std::hash::Hash,
        V: Data,
        P: Partitioner<Key = K>,
    {
        self.shuffle(rdd, partitioner, GroupByAggregator::new())
    }

    // Rdd<T> -> Rdd<T>
    // <T> -> <(partition_num, T)> -> <(partition_num, Vec<T>)> -> <T>
    fn partition_by<T: Data, P>(&mut self, rdd: RddIndex<T>, partitioner: P) -> RddIndex<T>
    where
        P: Partitioner<Key = T>,
    {
        let rdd = self.map_with_state(rdd, MapUsingPartitioner::new(partitioner.clone()));
        let rdd = self.group_by(rdd, DummyPartitioner(partitioner.partitions_num()));
        self.flat_map_with_state(rdd, FinishingFlatten::new())
    }

    // (K, Add) -> (K, Add)
    fn sum_by_key<K, V, P>(&mut self, rdd: RddIndex<(K, V)>, partitioner: P) -> RddIndex<(K, V)>
    where
        K: Data + Eq + std::hash::Hash,
        V: Data + Add<Output = V> + Default,
        P: Partitioner<Key = K>,
    {
        self.shuffle(rdd, partitioner, SumByKeyAggregator::new())
    }

    fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(T) -> U) -> RddIndex<U> {
        let idx = RddIndex::new(RddId::new());
        let partitions_num = self.graph.get_rdd(rdd.id).unwrap().partitions_num();
        self.graph.store_new_rdd(MapRdd {
            idx,
            partitions_num,
            prev: rdd,
            mapper: FnPtrMapper(f),
        });
        idx
    }

    fn map_with_state<T: Data, U: Data, M: Mapper<In = T, Out = U>>(
        &mut self,
        rdd: RddIndex<T>,
        mapper: M,
    ) -> RddIndex<U> {
        let idx = RddIndex::new(RddId::new());
        let partitions_num = self.graph.get_rdd(rdd.id).unwrap().partitions_num();
        self.graph.store_new_rdd(MapRdd {
            idx,
            partitions_num,
            prev: rdd,
            mapper,
        });
        idx
    }

    fn flat_map<T: Data, U: Data, I: IntoIterator<Item = U> + Data>(
        &mut self,
        rdd: RddIndex<T>,
        f: fn(T) -> I,
    ) -> RddIndex<U> {
        self.flat_map_with_state(rdd, FnPtrFlatMapper(f))
    }

    fn flat_map_with_state<
        T: Data,
        U: Data,
        I: IntoIterator<Item = U>,
        F: super::rdd::flat_map_rdd::FlatMapper<In = T, OutIterable = I>,
    >(
        &mut self,
        rdd: RddIndex<T>,
        flat_mapper: F,
    ) -> RddIndex<U> {
        let idx = RddIndex::new(RddId::new());
        let partitions_num = self.graph.get_rdd(rdd.id).unwrap().partitions_num();
        self.graph.store_new_rdd(FlatMapRdd {
            idx,
            partitions_num,
            prev: rdd,
            flat_mapper,
        });
        idx
    }

    fn filter<T: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> bool) -> RddIndex<T> {
        let idx = RddIndex::new(RddId::new());
        let partitions_num = self.graph.get_rdd(rdd.id).unwrap().partitions_num();
        self.graph.store_new_rdd(FilterRdd {
            idx,
            partitions_num,
            prev: rdd,
            filterer: FnPtrFilterer(f),
        });
        idx
    }

    fn new_from_list<T: Data + Clone>(&mut self, data: Vec<Vec<T>>) -> RddIndex<T> {
        let idx = RddIndex::new(RddId::new());
        self.graph.store_new_rdd(DataRdd {
            idx,
            partitions_num: data.len(),
            data,
        });
        idx
    }
}

mod partition_by {
    use std::marker::PhantomData;

    use serde::{Deserialize, Serialize};

    use crate::core::rdd::{
        flat_map_rdd::FlatMapper, map_rdd::Mapper, shuffle_rdd::Partitioner, Data,
    };

    #[derive(Serialize, Deserialize, Clone)]
    pub struct MapUsingPartitioner<T, P> {
        partitioner: P,
        #[serde(skip)]
        _value: PhantomData<T>,
    }

    impl<T, P> MapUsingPartitioner<T, P> {
        pub fn new(partitioner: P) -> Self {
            Self {
                partitioner,
                _value: PhantomData,
            }
        }
    }

    impl<T, P> Mapper for MapUsingPartitioner<T, P>
    where
        T: Data,
        P: Partitioner<Key = T>,
    {
        type In = T;

        type Out = (usize, T);

        fn map(&self, v: Self::In) -> Self::Out {
            (self.partitioner.partititon_by(&v), v)
        }
    }

    #[derive(Serialize, Deserialize, Clone)]
    pub struct DummyPartitioner(pub usize);

    impl Partitioner for DummyPartitioner {
        type Key = usize;

        fn partitions_num(&self) -> usize {
            self.0
        }

        fn partititon_by(&self, key: &Self::Key) -> usize {
            *key
        }
    }

    #[derive(Serialize, Deserialize, Clone)]
    pub struct FinishingFlatten<T> {
        #[serde(skip)]
        _value: PhantomData<T>,
    }

    impl<T> FinishingFlatten<T> {
        pub fn new() -> Self {
            Self {
                _value: PhantomData,
            }
        }
    }

    impl<T: Data> FlatMapper for FinishingFlatten<T> {
        type In = (usize, Vec<T>);

        type OutIterable = Vec<T>;

        fn map(&self, v: Self::In) -> Self::OutIterable {
            v.1
        }
    }
}
