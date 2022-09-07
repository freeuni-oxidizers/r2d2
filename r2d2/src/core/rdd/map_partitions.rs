use serde::{Deserialize, Serialize};

use super::{Data, Dependency, RddBase, RddId, RddIndex, RddWorkFns, TypedNarrowRddWork, TypedRdd};

pub trait PartitionMapper: Data {
    type In: Data;
    type Out: Data;

    fn map_partitions(&self, v: Vec<Self::In>, partition_id: usize) -> Vec<Self::Out>;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FnPtrPartitionMapper<T, U>(#[serde(with = "serde_fp")] pub fn(Vec<T>, usize) -> Vec<U>);

impl<T, U> PartitionMapper for FnPtrPartitionMapper<T, U>
where
    T: Data, // ll be partition
    U: Data,
{
    type In = T;

    type Out = U;

    fn map_partitions(&self, v: Vec<Self::In>, partition_id: usize) -> Vec<Self::Out> {
        self.0(v, partition_id)
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
    type InputItem = T;

    type OutputItem = U;

    fn work(
        &self,
        input_partition: Option<Vec<Self::InputItem>>,
        partition_id: usize,
    ) -> Vec<Self::OutputItem> {
        self.map_partitioner
            .map_partitions(input_partition.unwrap(), partition_id)
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
