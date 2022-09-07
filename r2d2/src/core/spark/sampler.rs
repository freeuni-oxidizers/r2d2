use crate::core::rdd::{map_partitions::PartitionMapper, Data};
use rand::{seq::IteratorRandom, thread_rng};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Serialize, Deserialize, Clone)]
pub struct Sampler<T> {
    amount: usize,
    #[serde(skip)]
    _value: PhantomData<T>,
}

impl<T> Sampler<T> {
    pub fn new(amount: usize) -> Self {
        Self {
            amount,
            _value: PhantomData,
        }
    }
}
impl<T> PartitionMapper for Sampler<T>
where
    T: Data,
{
    type In = T;

    type Out = T;

    fn map_partitions(&self, v: Vec<Self::In>, _partitition_id: usize) -> Vec<Self::Out> {
        // TODO: make it deterministic?
        v.into_iter()
            .choose_multiple(&mut thread_rng(), self.amount)
    }
}
