use std::any::Any;

use crate::core::rdd::{shuffle_rdd::Aggregator, Dependency, RddWideWork, RddWorkFns};

use super::{cache::ResultCache, graph::Graph, rdd::RddPartitionId, task_scheduler::Task};

pub struct Executor {
    /// Cache which stores full partitions ready for next rdd
    // this field is basically storing Vec<T>s where T can be different for each id we are not
    // doing Vec<Any> for perf reasons. downcasting is not free
    // This should not be serialized because all workers have this is just a cache
    pub cache: ResultCache,
    /// Cache which is used to store partial results of shuffle operations
    // TODO: buckets
    pub takeout: ResultCache,
    pub received_buckets: ResultCache,
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
            takeout: ResultCache::default(),
            received_buckets: ResultCache::default(),
        }
    }
    // pub fn resolve
    pub fn resolve(&mut self, graph: &Graph, id: RddPartitionId) {
        assert!(graph.contains(id.rdd_id), "id not found in context");
        if self.cache.has(id) {
            return;
        }

        let rdd_type = graph.get_rdd(id.rdd_id).unwrap().rdd_dependency();
        match rdd_type {
            Dependency::Narrow(dep_rdd) => self.resolve(
                graph,
                RddPartitionId {
                    rdd_id: dep_rdd,
                    partition_id: id.partition_id,
                },
            ),
            _ => {}
        };
        // First resolve all the deps
        match graph.get_rdd(id.rdd_id).unwrap().work_fns() {
            RddWorkFns::Narrow(narrow_work) => {
                let res = narrow_work.work(&self.cache, id.partition_id);
                self.cache.put(id, res);
            }
            RddWorkFns::Wide(aggr_fns) => {
                let partitions_num = graph.get_rdd(id.rdd_id).unwrap().partitions_num();
                // TODO: error handling - if bucket is missing (is None)
                let buckets: Vec<_> = (0..partitions_num)
                    .map(|i| self.received_buckets.take_as_any(id.rdd_id, i).unwrap())
                    .collect();
                let wides_result = aggr_fns.aggregate_buckets(buckets);
                self.cache.put(id, wides_result);
            }
        };
    }

    fn resolve_task(&mut self, graph: &Graph, task: Task) -> Vec<Box<dyn Any + Send>> {
        assert!(graph.contains(task.final_rdd), "id not found in context");
        // TODO: check if buckets are already on the disk and return without futher computations

        // if final task is not wide, it is ResultTask type, which is not handled here (not my fn's prob).
        if let Dependency::Wide(dep_id) = graph.get_rdd(task.final_rdd).unwrap().rdd_dependency() {
            self.resolve(
                graph,
                RddPartitionId {
                    rdd_id: dep_id,
                    partition_id: task.partition_id,
                },
            );
            // perform bucketisation of final data
            if let RddWorkFns::Wide(work) = graph.get_rdd(task.final_rdd).unwrap().work_fns() {
                let buckets =
                    work.partition_data(self.cache.take_as_any(dep_id, task.partition_id).unwrap());

                let aggred_buckets: Vec<_> = buckets
                    .into_iter()
                    .map(|bucket| work.aggregate_inside_bucket(bucket))
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
