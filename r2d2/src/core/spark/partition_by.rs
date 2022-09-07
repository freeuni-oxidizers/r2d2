use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::core::rdd::{flat_map_rdd::FlatMapper, map_rdd::Mapper, shuffle_rdd::Partitioner, Data};

#[derive(Serialize, Deserialize, Clone)]
pub struct MapUsingPartitioner<T, P> {
    partitioner: P,
    #[serde(skip)]
    _value: PhantomData<T>,
}

impl<T, P> MapUsingPartitioner<T, P> {
    pub fn new(partitioner: P) -> Self {
        Self {
            partitioner,
            _value: PhantomData,
        }
    }
}

impl<T, P> Mapper for MapUsingPartitioner<T, P>
where
    T: Data,
    P: Partitioner<Key = T>,
{
    type In = T;

    type Out = (usize, T);

    fn map(&self, v: Self::In) -> Self::Out {
        (self.partitioner.partititon_by(&v), v)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DummyPartitioner(pub usize);

impl Partitioner for DummyPartitioner {
    type Key = usize;

    fn partitions_num(&self) -> usize {
        self.0
    }

    fn partititon_by(&self, key: &Self::Key) -> usize {
        *key
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FinishingFlatten<T> {
    #[serde(skip)]
    _value: PhantomData<T>,
}

impl<T> Default for FinishingFlatten<T> {
    fn default() -> Self {
        Self { _value: PhantomData::default() }
    }
}

impl<T: Data> FlatMapper for FinishingFlatten<T> {
    type In = (usize, Vec<T>);

    type OutIterable = Vec<T>;

    fn map(&self, v: Self::In) -> Self::OutIterable {
        v.1
    }
}
