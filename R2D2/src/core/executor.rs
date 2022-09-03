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
    pub cache: ResultCache,
    pub received_buckets: Arc<Mutex<HashMap<(RddPartitionId, usize), Vec<u8>>>>,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Self {
        Self {
            cache: ResultCache::default(),
            received_buckets: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    // pub fn resolve
    pub fn resolve(&mut self, graph: &Graph, id: RddPartitionId) {
        assert!(graph.contains(id.rdd_id), "id not found in context");
        if self.cache.has(id) {
            return;
        }

        let rdd_type = graph.get_rdd(id.rdd_id).unwrap().rdd_dependency();
        if let Dependency::Narrow(dep_rdd) = rdd_type {
            self.resolve(
                graph,
                RddPartitionId {
                    rdd_id: dep_rdd,
                    partition_id: id.partition_id,
                },
            )
        };
        // First resolve all the deps

        match graph.get_rdd(id.rdd_id).unwrap().work_fns() {
            RddWorkFns::Narrow(narrow_work) => {
                let res = narrow_work.work(&self.cache, id.partition_id);
                self.cache.put(id, res);
            }
            RddWorkFns::Wide(aggr_fns) => {
                // TODO: error handling - if bucket is missing (is None)
                let rdd = graph.get_rdd(id.rdd_id).unwrap();

                if let Dependency::Wide(depp) = graph.get_rdd(id.rdd_id).unwrap().rdd_dependency() {
                    let narrow_partition_nums = graph.get_rdd(depp).unwrap().partitions_num();
                    let mut received_buckets = { self.received_buckets.lock().unwrap() };
                    let buckets: Vec<_> = (0..narrow_partition_nums)
                        .map(|narrow_partition_id| {
                            rdd.deserialize_raw_data(
                                received_buckets.remove(&(id, narrow_partition_id)).unwrap(),
                            )
                        })
                        .collect();

                    let wides_result = aggr_fns.aggregate_buckets(buckets);
                    self.cache.put(id, wides_result);
                };
            }
        };
    }

    pub fn resolve_task(&mut self, graph: &Graph, task: &WideTask) -> Vec<Vec<u8>> {
        assert!(graph.contains(task.wide_rdd_id), "id not found in context");
        // TODO: check if buckets are already on the disk and return without futher computations

        // if final task is not wide, it is ResultTask type, which is not handled here (not my fn's prob).
        if let Dependency::Wide(dep_id) = graph.get_rdd(task.wide_rdd_id).unwrap().rdd_dependency()
        {
            self.resolve(
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
                    self.cache
                        .take_as_any(dep_id, task.narrow_partition_id)
                        .unwrap(),
                );

                let aggred_buckets: Vec<_> = buckets
                    .into_iter()
                    .map(|bucket| work.aggregate_inside_bucket(bucket))
                    .map(|aggred_bucket| rdd.serialize_raw_data(aggred_bucket))
                    .collect();

                return aggred_buckets;
            }
        };
        unreachable!();
    }
}

// 1. at the end of task run first two steps of shuffle.
//  * might need some changes to `Task` and `resolve`
// 2. When `resolve` reaches final task from some previous Rdd, its result must be calculated from
//    received buckets. so, we need to run third step of shuffle, when we reach that kind of rdd.
//  * will need some changes in `resolve` and might need to indtrocude "bucket cache" in
//  executor/worker
// 3. Send buckets at the end of task to the target workers.
// 4. Receiver of buckets on worker.
// 5. dag scheduler:
//    * find stages and their dependecies
//    * track events and mark dependecies as completed
//    * schedule stages which have all dependecies completed
//    * randomize target workers which will store shuffle rdd partitions
//    * optional: assign tasks to workers which have partitions cached

// sc.cache(rdd);

// 5. join
