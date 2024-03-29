use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::core::rdd::{Dependency, RddWorkFns};

use super::{cache::ResultCache, graph::Graph, rdd::RddPartitionId, task_scheduler::WideTask};

/// Cloning this should still refer to same cache
#[derive(Clone, Debug)]
pub struct Executor {
    /// Cache which stores full partitions ready for next rdd
    // this field is basically storing Vec<T>s where T can be different for each id we are not
    // doing Vec<Any> for perf reasons. downcasting is not free
    // This should not be serialized because all workers have this is just a cache
    pub worker_id: usize,
    pub cache: ResultCache,
    pub received_buckets: Arc<Mutex<HashMap<(RddPartitionId, usize), Vec<u8>>>>,
}

impl Executor {
    pub fn new(worker_id: usize) -> Self {
        Self {
            worker_id,
            cache: ResultCache::default(),
            received_buckets: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    // pub fn resolve
    pub fn resolve(&mut self, graph: &Graph, id: RddPartitionId) -> Box<dyn Any + Send> {
        assert!(graph.contains(id.rdd_id), "id not found in context");
        if let Some(res) = self.cache.take_as_any(
            id.rdd_id,
            id.partition_id,
            graph.get_rdd(id.rdd_id).unwrap(),
        ) {
            return res;
        }

        match graph.get_rdd(id.rdd_id).unwrap().work_fns() {
            RddWorkFns::Narrow(narrow_work) => {
                let rdd = graph.get_rdd(id.rdd_id).unwrap();
                let res = match rdd.rdd_dependency() {
                    Dependency::Narrow(prev_rdd_id) => {
                        let materialized_prev = self.resolve(
                            graph,
                            RddPartitionId {
                                rdd_id: prev_rdd_id,
                                partition_id: id.partition_id,
                            },
                        );

                        narrow_work.work(Some(materialized_prev), id.partition_id)
                    }
                    Dependency::No => narrow_work.work(None, id.partition_id),
                    Dependency::Wide(_) => unreachable!(),
                    Dependency::Union(_) => unreachable!(),
                };
                return res;
                // self.cache.put(id, res);
            }
            RddWorkFns::Wide(aggr_fns) => {
                // TODO: error handling - if bucket is missing (is None)
                let rdd = graph.get_rdd(id.rdd_id).unwrap();

                if let Dependency::Wide(depp) = graph.get_rdd(id.rdd_id).unwrap().rdd_dependency() {
                    let narrow_partition_nums = graph.get_rdd(depp).unwrap().partitions_num();
                    println!("need to fetch buckets for {:?}, #{}", id, self.worker_id);
                    let mut received_buckets = { self.received_buckets.lock().unwrap() };
                    if self.cache.has(id) {
                        return self.cache.take_as_any(id.rdd_id, id.partition_id, rdd).unwrap();
                    }
                    let buckets: Vec<_> = (0..narrow_partition_nums)
                        .map(|narrow_partition_id| {
                            rdd.deserialize_raw_data(
                                received_buckets.remove(&(id, narrow_partition_id)).unwrap(),
                            )
                        })
                        .collect();

                    let wides_result = aggr_fns.aggregate_buckets(buckets);
                    self.cache.put(id, wides_result);
                    // we keep stage results
                    self.cache.keep_when_taken(id);
                    println!("saved wide result for {:?}, #{}", id, self.worker_id);
                    return self.cache.take_as_any(id.rdd_id, id.partition_id, rdd).unwrap();
                };
                unreachable!();
            }
            RddWorkFns::Union => {
                if let Dependency::Union(union_deps) =
                    graph.get_rdd(id.rdd_id).unwrap().rdd_dependency()
                {
                    let dep_partition = union_deps.get_partition_depp(id.partition_id);
                    let data = self.resolve(graph, dep_partition);
                    // let data = self
                    //     .cache
                    //     .take_as_any(
                    //         dep_partition.rdd_id,
                    //         dep_partition.partition_id,
                    //         graph.get_rdd(dep_partition.rdd_id).unwrap(),
                    //     )
                    //     .unwrap();
                    return data;
                }
                unreachable!();
            }
        };
    }

    pub fn resolve_task(&mut self, graph: &Graph, task: &WideTask) -> Vec<Vec<u8>> {
        assert!(graph.contains(task.wide_rdd_id), "id not found in context");
        // TODO: check if buckets are already on the disk and return without futher computations

        // if final task is not wide, it is ResultTask type, which is not handled here (not my fn's prob).
        if let Dependency::Wide(dep_id) = graph.get_rdd(task.wide_rdd_id).unwrap().rdd_dependency()
        {
            let res = self.resolve(
                graph,
                RddPartitionId {
                    rdd_id: dep_id,
                    partition_id: task.narrow_partition_id,
                },
            );
            // perform bucketisation of final data
            let rdd = graph.get_rdd(task.wide_rdd_id).unwrap();
            if let RddWorkFns::Wide(work) = rdd.work_fns() {
                let buckets = work.partition_data(
                    res,
                );

                let aggred_buckets: Vec<_> = buckets
                    .into_iter()
                    .map(|bucket| work.aggregate_inside_bucket(bucket))
                    .map(|aggred_bucket| rdd.serialize_raw_data(&*aggred_bucket))
                    .collect();

                return aggred_buckets;
            }
        };
        unreachable!();
    }
}

// 1. at the end of task run first two steps of shuffle. +++++++ done Tamta
//  * might need some changes to `Task` and `resolve` +++++++
// 2. When `resolve` reaches final task from some previous Rdd, its result must be calculated from
//    received buckets. so, we need to run third step of shuffle, when we reach that kind of rdd.
//  * will need some changes in `resolve` and might need to indtrocude "bucket cache" in
//  executor/worker +++++++ done Tamta
// 3. Send buckets at the end of task to the target workers. +++++++ done Tamta
// 4. Receiver of buckets on worker. +++++++ done Tamta Zviki
// 5. dag scheduler: +++++++ done Khokho
//    * find stages and their dependecies
//    * track events and mark dependecies as completed
//    * schedule stages which have all dependecies completed
//    * randomize target workers which will store shuffle rdd partitions
//    * optional: assign tasks to workers which have partitions cached

// Tasks after report:
// 1. union -> cogroup -> join
// 2. sort
// 3. scheduler and executor side of preserving certain rdd outputs in cache:
//        * user fn: cache
//        * if rdd hase 2 children
// 4. persists - user fn
