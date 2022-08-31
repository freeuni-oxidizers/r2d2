use serde::{Deserialize, Serialize};

use crate::core::cache::ResultCache;

use super::{Data, RddBase, RddId, RddIndex, RddType, RddWorkFns, TypedNarrowRddWork, TypedRdd};

// TODO: maybe no pub?
#[derive(Clone, Serialize, Deserialize)]
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
}

impl<T, U> TypedNarrowRddWork for MapRdd<T, U>
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

    fn work_fns(&self) -> RddWorkFns {
        RddWorkFns::Narrow(self)
    }
}
