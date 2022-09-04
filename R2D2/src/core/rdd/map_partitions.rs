use serde::{Deserialize, Serialize};

use crate::core::cache::ResultCache;

use super::{Data, Dependency, RddBase, RddId, RddIndex, RddWorkFns, TypedNarrowRddWork, TypedRdd};

pub trait PartitionMapper: Data {
    type In: Data;
    type Out: Data;

    fn map_partitions(&self, v: Vec<Self::In>) -> Vec<Self::Out>;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FnPtrPartitionMapper<T, U>(#[serde(with = "serde_fp")] pub fn(Vec<T>) -> Vec<U>);


impl<T, U> PartitionMapper for FnPtrPartitionMapper<T, U>
where
    T: Data, // ll be partition
    U: Data,
{
    type In = T;

    type Out = U;

    fn map_partitions(&self, v: Vec<Self::In>) -> Vec<Self::Out> {
        self.0(v)
    }
}

// TODO: maybe no pub?
#[derive(Clone, Serialize, Deserialize)]
pub struct MapPartitionsRdd<T, U, M> {
    pub idx: RddIndex<U>,
    pub prev: RddIndex<T>,
    pub partitions_num: usize,
    pub map_partitioner: M,
}

impl<T, U, M> TypedRdd for MapPartitionsRdd<T, U, M>
where
    T: Data,
    U: Data,
    M: PartitionMapper<In = T, Out = U>,
{
    type Item = U;
}

impl<T, U, M> TypedNarrowRddWork for MapPartitionsRdd<T, U, M>
where
    T: Data,
    U: Data,
    M: PartitionMapper<In = T, Out = U>,
{
    type Item = U;

    fn work(&self, cache: &ResultCache, partition_id: usize) -> Vec<Self::Item> {
        // TODO: pass dependency in don't just take
        let v = cache.take_as(self.prev, partition_id).unwrap();
        self.map_partitioner.map_partitions(v)
    }
}

impl<T, U, M> RddBase for MapPartitionsRdd<T, U, M>
where
    T: Data,
    U: Data,
    M: PartitionMapper<In = T, Out = U>,
{
    fn id(&self) -> RddId {
        self.idx.id
    }

    fn rdd_dependency(&self) -> super::Dependency {
        Dependency::Narrow(self.prev.id)
    }

    fn partitions_num(&self) -> usize {
        self.partitions_num
    }

    fn work_fns(&self) -> RddWorkFns {
        RddWorkFns::Narrow(self)
    }
}
