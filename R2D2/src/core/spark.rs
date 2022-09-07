use std::{fmt::Debug, ops::Add, path::PathBuf, process::exit};

use tokio::sync::{mpsc, oneshot};

use crate::{core::rdd::shuffle_rdd::Aggregator, master::MasterService, worker, Args, Config};
use crate::core::rdd::union_rdd::UnionRdd;

use self::{
    file_writer::FileWriter,
    group_by::GroupByAggregator,
    partition_by::{DummyPartitioner, FinishingFlatten, MapUsingPartitioner},
    sample_partitioner::SamplePartitioner,
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
        map_partitions::{FnPtrPartitionMapper, MapPartitionsRdd, PartitionMapper},
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
                worker::start(
                    args.id,
                    args.port,
                    config.master_addr.clone(),
                    args.fs_root,
                    config,
                )
                .await;
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
    // input: (path, content)
    // pub async fn save(&mut self, rdd: RddIndex<(PathBuf,)>, path: PathBuf) {
    //     self.map_partitions_with_state(rdd, FileWriter::new(path));
    // }

    // spark.save(rdd, |partition: Vec<T>|-> Vec<u8>, directory).await;
    pub async fn save<T: Data>(
        &mut self,
        rdd: RddIndex<T>,
        serializer: fn(Vec<T>) -> Vec<u8>,
        path: PathBuf,
    ) {
        let rdd = self.map_partitions_with_state(rdd, FileWriter::new(path, serializer));
        self.collect(rdd).await;
    }

    pub async fn read_partitions_from(
        &mut self,
        path: PathBuf,
        num_partitions: usize,
    ) -> RddIndex<(PathBuf, Vec<u8>)> {
        let data: Vec<Vec<PathBuf>> = (0..num_partitions).map(|i| {
            vec![path.join(i.to_string())]
        }).collect();
        let rdd = self.new_from_list(data);
        self.map(rdd, |path| {
            let data = std::fs::read(&path).expect("Error: while reading partition file");
            (path, data) 
        })
    }

    pub async fn sort<T>(&mut self, rdd: RddIndex<T>, num_partitions: usize) -> RddIndex<T>
    where
        T: Data + std::cmp::Ord + Debug,
    {
        let sample_rdd = self.sample(rdd, num_partitions);
        let mut sample = self.collect(sample_rdd).await;
        sample.sort();

        let mut dividers = Vec::new();
        for i in 1..num_partitions {
            dividers.push(sample[i * sample.len() / num_partitions].clone())
        }

        let partitioner = SamplePartitioner::new(dividers);
        let rdd = self.partition_by(rdd, partitioner);
        self.map_partitions(rdd, |mut v: Vec<T>, _| {
            v.sort();
            v
        })
    }
}

mod file_writer {
    use crate::core::rdd::{map_partitions::PartitionMapper, Data};
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;

    #[derive(Serialize, Deserialize, Clone)]
    pub struct FileWriter<T: Data> {
        path: PathBuf,
        #[serde(with = "serde_fp")]
        serializer: fn(Vec<T>) -> Vec<u8>,
    }

    impl<T: Data> FileWriter<T> {
        pub fn new(path: PathBuf, serializer: fn(Vec<T>) -> Vec<u8>) -> Self {
            Self { path, serializer }
        }
    }
    impl<T> PartitionMapper for FileWriter<T>
    where
        T: Data,
    {
        type In = T;
        type Out = ();

        // Rdd<T> -> <./, Vec<T> -> Vec<u8>>
        // Rdd<(K, V)> K -> V
        fn map_partitions(&self, v: Vec<Self::In>, _partitition_id: usize) -> Vec<Self::Out> {
            // TODO: make it deterministic?
            let serialized_partition = (self.serializer)(v);
            std::fs::write(
                self.path.join(_partitition_id.to_string()),
                serialized_partition,
            )
            .unwrap();
            Vec::new()
        }
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

    fn sample<T>(&mut self, rdd: RddIndex<T>, amount: usize) -> RddIndex<T>
    where
        T: Data,
    {
        self.map_partitions_with_state(rdd, sampler::Sampler::new(amount))
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

    fn map_partitions<T: Data, U: Data>(
        &mut self,
        rdd: RddIndex<T>,
        f: fn(Vec<T>, usize) -> Vec<U>,
    ) -> RddIndex<U> {
        let idx = RddIndex::new(RddId::new());
        let partitions_num = self.graph.get_rdd(rdd.id).unwrap().partitions_num();
        self.graph.store_new_rdd(MapPartitionsRdd {
            idx,
            partitions_num,
            prev: rdd,
            map_partitioner: FnPtrPartitionMapper(f),
        });
        idx
    }

    fn map_partitions_with_state<T: Data, U: Data, M: PartitionMapper<In = T, Out = U>>(
        &mut self,
        rdd: RddIndex<T>,
        map_partitioner: M,
    ) -> RddIndex<U> {
        let idx = RddIndex::new(RddId::new());
        let partitions_num = self.graph.get_rdd(rdd.id).unwrap().partitions_num();
        self.graph.store_new_rdd(MapPartitionsRdd {
            idx,
            partitions_num,
            prev: rdd,
            map_partitioner,
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

    fn union<T: Data>(&mut self, deps: &[RddIndex<T>]) -> RddIndex<T> {
        let idx = RddIndex::new(RddId::new());
        let deps: Vec<(RddId,usize)> = deps.iter().map(|rdd_idx| (rdd_idx.id, self.graph.get_rdd(rdd_idx.id).unwrap().partitions_num())).collect();
        let partitions_num = deps.iter().map(|(_, np)| np).sum::<usize>();
        self.graph.store_new_rdd(UnionRdd {
            idx, 
            deps,
            partitions_num,
        });
        idx
    }

    fn cogroup<K, V, W, P>(&mut self, left: RddIndex<(K, V)>, right: RddIndex<(K, W)>, partitioner: P) -> RddIndex<(K, (Vec<V>, Vec<W>))> 
    where 
        K: Data + Eq + std::hash::Hash,
        V: Data, 
        W: Data, 
        P: Partitioner<Key = K>,
    {
        // TODO: don't map w fn ptr
        
        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        enum VW<V, W> {
            Left(V),
            Right(W),
        }
        let left = self.map(left, |(k, v)| (k, VW::Left(v)));
        let right = self.map(right, |(k, w)| (k, VW::Right(w)));
        let all = self.union(&[left, right]);
        let grouped = self.group_by(all, partitioner);
        self.map(grouped, |(k, values)| {
            let (mut left, mut right) = (Vec::new(), Vec::new());
            for vw in values {
                match vw {
                    VW::Left(v) => left.push(v),
                    VW::Right(w) => right.push(w),
                }
            }
            (k, (left, right))
        })
    }

    fn join<K, V, W, P>(&mut self, left: RddIndex<(K, V)>, right: RddIndex<(K, W)>, partitioner: P) -> RddIndex<(K, (V, W))> 
    where
        K: Data + Eq + std::hash::Hash,
        V: Data,
        W: Data,
        P: Partitioner<Key = K>,
    {
        //let left = self.group_by(left, partitioner);
        //let right = self.group_by(right, partitioner);
        let all = self.cogroup(left, right, partitioner);
        self.flat_map(all, |(k, (vec_v, vec_w))| {
            let mut res = Vec::new();
            for v in vec_v {
                for w in vec_w.clone() {
                    res.push((k.clone(), (v.clone(), w)));
                }
            }
            res
            // vec_v.into_iter().flat_map(|v| {
            //     vec_w.clone().into_iter().map(|w| {
            //         (k.clone(), (v.clone(), w))
            //     }).collect::<Vec<_>>()
            // }).collect::<Vec<_>>()
        })
    }
}

mod sampler {
    use crate::core::rdd::{map_partitions::PartitionMapper, Data};
    use rand::{seq::IteratorRandom, thread_rng};
    use serde::{Deserialize, Serialize};
    use std::marker::PhantomData;

    #[derive(Serialize, Deserialize, Clone)]
    pub struct Sampler<T> {
        amount: usize,
        #[serde(skip)]
        _value: PhantomData<T>,
    }

    impl<T> Sampler<T> {
        pub fn new(amount: usize) -> Self {
            Self {
                amount,
                _value: PhantomData,
            }
        }
    }
    impl<T> PartitionMapper for Sampler<T>
    where
        T: Data,
    {
        type In = T;

        type Out = T;

        fn map_partitions(&self, v: Vec<Self::In>, _partitition_id: usize) -> Vec<Self::Out> {
            // TODO: make it deterministic?
            v.into_iter()
                .choose_multiple(&mut thread_rng(), self.amount)
        }
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

pub mod hash_partitioner {

    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        marker::PhantomData,
    };

    use serde::{Deserialize, Serialize};

    use crate::core::rdd::{shuffle_rdd::Partitioner, Data};

    #[derive(Clone, Serialize, Deserialize)]
    pub struct HashPartitioner<T> {
        num_partitions: usize,
        #[serde(skip)]
        _value: PhantomData<T>,
    }

    impl<T> HashPartitioner<T> {
        pub fn new(num_partitions: usize) -> Self {
            Self {
                num_partitions,
                _value: PhantomData,
            }
        }
    }

    impl<T: Data + Hash> Partitioner for HashPartitioner<T> {
        type Key = T;

        fn partitions_num(&self) -> usize {
            self.num_partitions
        }

        fn partititon_by(&self, key: &Self::Key) -> usize {
            let mut s = DefaultHasher::new();
            key.hash(&mut s);
            s.finish() as usize % self.num_partitions
        }
    }
}

pub mod sample_partitioner {

    use std::marker::PhantomData;

    use serde::{Deserialize, Serialize};

    use crate::core::rdd::{shuffle_rdd::Partitioner, Data};

    #[derive(Clone, Serialize, Deserialize)]
    pub struct SamplePartitioner<T> {
        sample: Vec<T>,
        #[serde(skip)]
        _value: PhantomData<T>,
    }

    impl<T> SamplePartitioner<T> {
        pub fn new(sample: Vec<T>) -> Self {
            Self {
                sample,
                _value: PhantomData,
            }
        }
    }

    impl<T: Data + Ord> Partitioner for SamplePartitioner<T> {
        type Key = T;

        fn partitions_num(&self) -> usize {
            // `n` divisors create `n+1` buckets
            self.sample.len() + 1
        }

        fn partititon_by(&self, key: &Self::Key) -> usize {
            self.sample.partition_point(|x| *x < *key)
        }
    }
}
