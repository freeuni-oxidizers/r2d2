use serde::{Deserialize, Serialize};

use crate::core::cache::ResultCache;

use super::{Data, RddBase, RddId, RddIndex, RddType, TypedRdd, TypedRddWideWork};

#[derive(Clone, Serialize, Deserialize)]
pub struct ShuffleRdd<K, V, C, P, A> {
    pub idx: RddIndex<(K, C)>,
    pub prev: RddIndex<(K, V)>,
    pub partitions_num: usize,
    pub partitioner: P,
    pub aggregator: Option<A>,
}

pub trait Partitioner: Data {
    type Key: Data;
    fn partititon_by(&self, key: Self::Key) -> usize;
}

// (V) -> Acc
// (V, Acc) -> Acc
// (Acc, Acc) -> Acc
pub trait Aggregator: Data {
    type Value: Data;
    type Combiner: Data;

    fn create_combiner(&self) -> Self::Combiner;
    fn merge_value(&self, value: Self::Value, combiner: Self::Combiner) -> Self::Combiner;
    fn merge_combiners(
        &self,
        combiner1: Self::Combiner,
        combiner2: Self::Combiner,
    ) -> Self::Combiner;
}

impl<K, V, C, P, A> TypedRdd for ShuffleRdd<K, V, C, P, A>
where
    K: Data,
    V: Data,
    C: Data,
    P: Partitioner,
    A: Aggregator,
{
    type Item = (K, C);

    fn work(&self, cache: &ResultCache, partition_id: usize) -> Vec<Self::Item> {
        unreachable!("ShuffleRdd is a wide transformation");
    }
}

impl<K, V, C, P, A> TypedRddWideWork for ShuffleRdd<K, V, C, P, A>
where
    K: Data,
    V: Data,
    C: Data,
    P: Partitioner,
    A: Aggregator,
{
}

impl<K, V, C, P, A> RddBase for ShuffleRdd<K, V, C, P, A>
where
    K: Data,
    V: Data,
    C: Data,
    P: Partitioner,
    A: Aggregator,
{
    fn id(&self) -> RddId {
        self.idx.id
    }

    fn deps(&self) -> Vec<RddId> {
        vec![self.prev.id]
    }

    fn rdd_type(&self) -> super::RddType {
        RddType::Wide
    }

    fn partitions_num(&self) -> usize {
        self.partitions_num
    }
}
