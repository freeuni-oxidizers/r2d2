use serde::{Deserialize, Serialize};

use super::{Data, RddBase, RddId, RddIndex, RddType, TypedRdd};

#[derive(Clone, Serialize, Deserialize)]
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
        cache: &crate::core::cache::ResultCache,
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
