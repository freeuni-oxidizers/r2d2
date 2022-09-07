use std::collections::{HashMap, HashSet};

use crate::core::{
    graph::Graph,
    rdd::{RddId, RddPartitionId},
};

#[derive(Default, Debug)]
pub struct PartitionDsu {
    parent: HashMap<RddPartitionId, RddPartitionId>,
    visited: HashSet<RddPartitionId>,
    ids: HashMap<RddPartitionId, usize>,
}

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct GroupId(usize);

impl PartitionDsu {
    pub fn consume_new_graph(
        &mut self,
        graph: &Graph,
        target_rdd: RddId,
    ) -> HashMap<RddPartitionId, GroupId> {
        self.run(graph, target_rdd);
        let mut cur_id = 0;
        let mut res: HashMap<_, _> = HashMap::default();
        for (rpid, _) in self.parent.clone().into_iter() {
            let p = self.par(rpid);
            let id = match self.ids.get(&p) {
                Some(id) => *id,
                None => {
                    cur_id += 1;
                    self.ids.insert(p, cur_id);
                    cur_id
                }
            };
            res.insert(rpid, GroupId(id));
        }
        res
    }

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
        // hacky: this makes sure that node is recorded
        self.par(rpid);
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
            crate::core::rdd::Dependency::Union(ud) => {
                let parent_rpid = ud.get_partition_depp(rpid.partition_id);
                self.join(rpid, parent_rpid);
                self.dfs(graph, parent_rpid)
            }
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
