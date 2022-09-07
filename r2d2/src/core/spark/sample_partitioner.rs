use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::core::rdd::{shuffle_rdd::Partitioner, Data};

#[derive(Clone, Serialize, Deserialize)]
pub struct SamplePartitioner<T> {
    sample: Vec<T>,
    #[serde(skip)]
    _value: PhantomData<T>,
}

impl<T> SamplePartitioner<T> {
    pub fn new(sample: Vec<T>) -> Self {
        Self {
            sample,
            _value: PhantomData,
        }
    }
}

impl<T: Data + Ord> Partitioner for SamplePartitioner<T> {
    type Key = T;

    fn partitions_num(&self) -> usize {
        // `n` divisors create `n+1` buckets
        self.sample.len() + 1
    }

    fn partititon_by(&self, key: &Self::Key) -> usize {
        self.sample.partition_point(|x| *x < *key)
    }
}
