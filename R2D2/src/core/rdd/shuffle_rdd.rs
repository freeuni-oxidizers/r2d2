use std::{collections::HashMap, hash::Hash};

use serde::{Deserialize, Serialize};

use super::{Data, RddBase, RddId, RddIndex, RddType, RddWorkFns, TypedRdd, TypedRddWideWork};

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
    fn partititon_by(&self, key: &Self::Key) -> usize;
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
}

impl<K, V, C, P, A> TypedRddWideWork for ShuffleRdd<K, V, C, P, A>
where
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
    P: Partitioner<Key = K>,
    A: Aggregator<Value = V, Combiner = C>,
{
    type K = K;
    type V = V;
    type C = C;

    fn partition_data(
        &self,
        input_partition: Vec<(Self::K, Self::V)>,
    ) -> Vec<Vec<(Self::K, Self::V)>> {
        let mut result = Vec::new();
        for _ in 0..self.partitions_num {
            result.push(Vec::new());
        }

        for elem in input_partition.into_iter() {
            let partition_idx = self.partitioner.partititon_by(&elem.0);
            result[partition_idx].push(elem);
        }

        result
    }

    fn aggregate_inside_bucket(
        &self,
        bucket_data: Vec<(Self::K, Self::V)>,
    ) -> Vec<(Self::K, Self::C)> {
        // TODO: handle aggreagte = Null case (repartition e. g.)
        let aggr = self.aggregator.as_ref().unwrap();

        let mut bucket_by_keys = HashMap::new();
        for (k, v) in bucket_data.into_iter() {
            bucket_by_keys.entry(k).or_insert_with(Vec::new).push(v)
        }

        bucket_by_keys
            .into_iter()
            .map(|x| {
                (
                    x.0,
                    x.1.into_iter()
                        .fold(aggr.create_combiner(), |acc, y| aggr.merge_value(y, acc)),
                )
            })
            .collect()
    }

    fn aggregate_buckets(
        &self,
        _buckets_aggr_data: Vec<Vec<(Self::K, Self::C)>>,
    ) -> Vec<(Self::K, Self::C)> {
        let aggr = self.aggregator.as_ref().unwrap();

        let mut combiners_by_keys = HashMap::new();
        for bucket_combiners in buckets_aggr_data.into_iter() {
            for (k, c) in bucket_combiners.into_iter() {
                combiners_by_keys.entry(k).or_insert_with(Vec::new).push(c)
            }
        }
        combiners_by_keys
            .into_iter()
            .map(|x| {
                (
                    x.0,
                    x.1.into_iter().fold(aggr.create_combiner(), |acc1, acc2| {
                        aggr.merge_combiners(acc1, acc2)
                    }),
                )
            })
            .collect()
    }
}

impl<K, V, C, P, A> RddBase for ShuffleRdd<K, V, C, P, A>
where
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
    P: Partitioner<Key = K>,
    A: Aggregator<Value = V, Combiner = C>,
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

    fn work_fns(&self) -> RddWorkFns {
        RddWorkFns::Wide(self)
    }
}
