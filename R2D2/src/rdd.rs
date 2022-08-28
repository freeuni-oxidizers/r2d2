use std::sync::atomic::{AtomicUsize, Ordering};

use crate::Config;
use crate::{master, worker};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::r2d2::{Task, TaskAction};
use tokio::sync::broadcast;

use std::{any::Any, collections::HashMap, marker::PhantomData};

use self::rdd::{Data, RddBase, RddId, RddIndex};

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

    pub trait Data: Serialize + DeserializeOwned + Clone + 'static {}

    impl<T> Data for T where T: Serialize + DeserializeOwned + Clone + 'static {}

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
        fn serialize_raw_data(&self, raw_data: &dyn Any) -> Vec<u8>;
        /// deserialize data in a way which can be ingested into following rdds
        fn deserialize_raw_data(&self, serialized_data: Vec<u8>) -> Box<dyn Any>;
    }

    pub trait RddWork {
        /// This expects that all the deps have already been put inside the cache
        /// Ownership of results is passed back to context!
        // TODO: Is being generic over DataFetcher here fine???
        // maybe do enum_dispatch
        fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any>;
    }

    /// methods independent of `Item` type
    pub trait RddBase:
        RddWork + RddSerde + serde_traitobject::Serialize + serde_traitobject::Deserialize
    {
        /// Fetch unique id for this rdd
        fn id(&self) -> RddId;

        /// rdd dependencies
        fn deps(&self) -> Vec<RddId>;

        fn rdd_type(&self) -> RddType;

        fn partitions_num(&self) -> usize;
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
        fn serialize_raw_data(&self, raw_data: &dyn Any) -> Vec<u8> {
            let data: &Vec<T::Item> = raw_data.downcast_ref().unwrap();
            serde_json::to_vec(data).unwrap()
        }

        fn deserialize_raw_data(&self, serialized_data: Vec<u8>) -> Box<dyn Any> {
            let data: Vec<T::Item> = serde_json::from_slice(&serialized_data).unwrap();
            Box::new(data)
        }
    }

    impl<T> RddWork for T
    where
        T: TypedRdd,
    {
        fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any> {
            Box::new(Self::work(&self, cache, partition_id))
        }
    }

    pub mod data_rdd {

        use serde::{Deserialize, Serialize};

        use crate::rdd::cache::ResultCache;

        use super::{Data, RddBase, RddId, RddType, TypedRdd};

        #[derive(Serialize, Deserialize)]
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

            fn work(&self, cache: &ResultCache, partition_id: usize) -> Vec<Self::Item> {
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
        #[derive(Serialize, Deserialize)]
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

        #[derive(Serialize, Deserialize)]
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
        data: HashMap<RddPartitionId, Box<dyn Any>>,
    }

    impl ResultCache {
        pub fn has(&self, id: RddPartitionId) -> bool {
            self.data.contains_key(&id)
        }

        pub fn put(&mut self, id: RddPartitionId, data: Box<dyn Any>) {
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

        pub fn get_as_any(&self, rdd_id: RddId, partition_id: usize) -> Option<&dyn Any> {
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
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    use super::rdd::{RddBase, RddId};

    // TODO: can't easily have custom deserialization for hashmap value sadge :(
    #[derive(Serialize, Deserialize)]
    struct RddHolder(#[serde(with = "serde_traitobject")] Box<dyn RddBase>);

    #[derive(Default, Serialize, Deserialize)]
    pub struct Graph {
        /// All the rdd's are stored in the context in here
        rdds: HashMap<RddId, RddHolder>,
        // #[serde(skip)]
        // cache: ResultCache,
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
        fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> Vec<T>;
        fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> U) -> RddIndex<U>;
        fn filter<T: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> bool) -> RddIndex<T>;
        fn new_from_list<T: Data + Clone>(&mut self, data: Vec<Vec<T>>) -> RddIndex<T>;

        // fn store_rdd<T: RddBase + 'static>(&self, rdd: T) -> Rc<T>;
        // fn receive_serialized(&self, id: RddId, serialized_data: String);
    }
}

// // TODO(zvikinoza): extract appropriate fn s to Spark
// impl SparkContext {
//     pub fn new() -> Self {
//         Self::default()
//     }
//
//     /// TODO: proper error handling
//     fn resolve(&mut self, id: RddId) {
//         assert!(self.rdds.contains_key(&id), "id not found in context");
//         if self.cache.has(id) {
//             return;
//         }
//         // First resolve all the deps
//         for dep in self.rdds.get(&id).unwrap().0.deps() {
//             self.resolve(dep);
//         }
//         let res = self.rdds.get(&id).unwrap().0.work(&self.cache);
//         self.cache.put(id, res);
//     }
//
//     fn store_new_rdd<R: RddBase + 'static>(&mut self, rdd: R) {
//         self.rdds.insert(rdd.id(), RddHolder(Box::new(rdd)));
//     }
// }
//
// impl Context for SparkContext {
//     fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> &[T] {
//         self.resolve(rdd.id);
//         self.cache.get_as(rdd).unwrap()
//     }
//
//     fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> U) -> RddIndex<U> {
//         let id = RddId::new();
//         self.store_new_rdd(MapRdd {
//             id,
//             prev: rdd,
//             map_fn: f,
//         });
//         RddIndex {
//             id,
//             _data: PhantomData::default(),
//         }
//     }
//
//     fn new_from_list<T: Data + Clone>(&mut self, data: Vec<T>) -> RddIndex<T> {
//         let id = RddId::new();
//         self.store_new_rdd(DataRdd { id, data });
//         RddIndex {
//             id,
//             _data: PhantomData::default(),
//         }
//     }
// }

struct RddDag {}

pub mod executor {
    use crate::rdd::rdd::RddType;

    use super::{cache::ResultCache, graph::Graph, rdd::RddPartitionId};

    pub struct Executor {
        // /// Graph for the current Execution
        // pub graph: Graph,
        /// Cache which stores full partitions ready for next rdd
        pub cache: ResultCache,
        /// Cache which is used to store partial results of shuffle operations
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
    pub struct DagScheduler;
}

mod task_scheduler {
    pub struct TaskScheduler;
}

pub mod spark {
    use crate::{rdd::rdd::RddType, Config};

    use super::{
        cache::ResultCache,
        context::Context,
        dag_scheduler::DagScheduler,
        executor::Executor,
        graph::Graph,
        rdd::{
            data_rdd::DataRdd, filter_rdd::FilterRdd, map_rdd::MapRdd, Data, RddId, RddIndex,
            RddPartitionId,
        },
        task_scheduler::TaskScheduler,
    };

    // TODO(zvikinoza): extract this to sep file
    // and use SparkContext as lib from rdd-simple
    pub struct Spark {
        /// Resposible for storing current graph build up by user
        graph: Graph,
        /// Resposible for splitting up dag into tasks
        dag_scheduler: DagScheduler,
        /// resposible for scheduling tasks
        /// This is the guy who sets tasks to execute for workers
        task_scheduler: TaskScheduler,
        // tx: broadcast::Sender<Task>,
        // wait_handle: Option<tokio::task::JoinHandle<()>>,
    }

    impl Spark {
        pub fn new(config: Config) -> Self {
            // let (tx, _) = broadcast::channel(16);
            // let mut sc = Self {
            //     sc: SparkContext::new(),
            //     tx,
            //     wait_handle: None,
            // };
            // if config.master {
            //     sc.wait_handle = Some(master::start(config.n_workers, &sc.tx).await);
            // } else {
            //     // better if we tokio::block of await? (to make Self::new sync)
            //     worker::start(config.id).await; // this fn call never returns
            // }
            // sc
            Spark {
                graph: Graph::default(),
                dag_scheduler: DagScheduler,
                task_scheduler: TaskScheduler,
                // this field is basically storing Vec<T>s where T can be different for each id we are not
                // doing Vec<Any> for perf reasons. downcasting is not free
                // This should not be serialized because all workers have this is just a cache
            }
        }

        // // TODO(zvikinoza): port this to fn drop
        // // problem is Drop can't be async and await must be async
        // pub async fn termiante(&mut self) {
        //     match self.wait_handle {
        //         Some(ref mut wh) => wh.await.unwrap(),
        //         None => panic!("worker shouldn't be here"),
        //     };
        //     println!("\n\nmaster shutting down\n\n");
        // }
    }

    impl Context for Spark {
        fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> Vec<T> {
            let mut result: Vec<T> = vec![];
            let rdd_info = &self.graph.get_rdd(rdd.id).unwrap();
            let rdd_id = rdd_info.id();
            let partitions_num = rdd_info.partitions_num();
            let mut executor = Executor::new();
            for partition_id in 0..partitions_num {
                let id = RddPartitionId {
                    rdd_id,
                    partition_id,
                };
                executor.resolve(&self.graph, id);
                result.extend(
                    executor
                        .cache
                        .get_as(rdd, id.partition_id)
                        .unwrap()
                        .to_vec(),
                );
            }
            result
        }

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
