use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use serde::{Deserialize, Serialize};

use crate::core::rdd::{shuffle_rdd::Partitioner, Data};

#[derive(Clone, Serialize, Deserialize)]
pub struct HashPartitioner<T> {
    num_partitions: usize,
    #[serde(skip)]
    _value: PhantomData<T>,
}

impl<T> HashPartitioner<T> {
    pub fn new(num_partitions: usize) -> Self {
        Self {
            num_partitions,
            _value: PhantomData,
        }
    }
}

impl<T: Data + Hash> Partitioner for HashPartitioner<T> {
    type Key = T;

    fn partitions_num(&self) -> usize {
        self.num_partitions
    }

    fn partititon_by(&self, key: &Self::Key) -> usize {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish() as usize % self.num_partitions
    }
}
