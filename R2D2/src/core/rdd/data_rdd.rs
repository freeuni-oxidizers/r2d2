use serde::{Deserialize, Serialize};

use crate::core::cache::ResultCache;

use super::{Data, RddBase, RddId, RddType, RddWorkFns, TypedRdd, TypedNarrowRddWork};

#[derive(Clone, Serialize, Deserialize)]
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
}

impl<T> TypedNarrowRddWork for DataRdd<T>
where
    T: Data,
{
    type Item = T;

    fn work(&self, _cache: &ResultCache, partition_id: usize) -> Vec<Self::Item> {
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

    fn work_fns(&self) -> RddWorkFns {
        RddWorkFns::Narrow(self)
    }
}
