use serde::{Deserialize, Serialize};

use super::{Data, Dependency, RddBase, RddId, RddIndex, RddWorkFns, TypedRdd, UnionDependency};

// TODO: maybe no pub?
#[derive(Clone, Serialize, Deserialize)]
pub struct UnionRdd<T> {
    pub idx: RddIndex<T>,
    pub deps: Vec<(RddId, usize)>,
    pub partitions_num: usize,
}

impl<T> TypedRdd for UnionRdd<T>
where
    T: Data,
{
    type Item = T;
}

impl<T> RddBase for UnionRdd<T>
where
    T: Data,
{
    fn id(&self) -> RddId {
        self.idx.id
    }

    fn rdd_dependency(&self) -> super::Dependency {
        Dependency::Union(UnionDependency::new(&self.deps))
    }

    fn partitions_num(&self) -> usize {
        self.partitions_num
    }

    fn work_fns(&self) -> RddWorkFns {
        RddWorkFns::Union
    }
}
