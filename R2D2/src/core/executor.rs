use crate::core::rdd::{RddType, RddWorkFns};

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
            self.resolve(
                graph,
                RddPartitionId {
                    rdd_id: dep,
                    partition_id: id.partition_id,
                },
            );
        }
        match graph.get_rdd(id.rdd_id).unwrap().work_fns() {
            RddWorkFns::Narrow(narrow_work) => {
                let res = narrow_work.work(&self.cache, id.partition_id);
                self.cache.put(id, res);
            }
            RddWorkFns::Wide(_) => todo!(),
        };
    }
}
