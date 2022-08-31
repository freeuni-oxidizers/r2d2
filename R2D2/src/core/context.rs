use std::{hash::Hash, ops::Add};

use super::rdd::{
    map_rdd::Mapper,
    shuffle_rdd::{Aggregator, Partitioner},
    Data, RddIndex, flat_map_rdd::FlatMapper,
};

/// All these methods use interior mutability to keep state
pub trait Context {
    fn shuffle<K: Data, V: Data, C: Data, P, A>(
        &mut self,
        rdd: RddIndex<(K, V)>,
        partitioner: P,
        aggregator: A,
    ) -> RddIndex<(K, C)>
    where
        K: Eq + Hash,
        P: Partitioner<Key = K>,
        A: Aggregator<Value = V, Combiner = C>;
    fn group_by<K: Data, V: Data, P>(
        &mut self,
        rdd: RddIndex<(K, V)>,
        partitioner: P,
    ) -> RddIndex<(K, Vec<V>)>
    where
        K: Eq + Hash,
        P: Partitioner<Key = K>;
    fn partition_by<T: Data, P>(&mut self, rdd: RddIndex<T>, partitioner: P) -> RddIndex<T>
    where
        P: Partitioner<Key = T>;
    fn sum_by_key<K: Data, V: Data, P>(
        &mut self,
        rdd: RddIndex<(K, V)>,
        partitioner: P,
    ) -> RddIndex<(K, V)>
    where
        K: Eq + Hash,
        V: Add<Output = V> + Default,
        P: Partitioner<Key = K>;

    fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(T) -> U) -> RddIndex<U>;
    fn map_with_state<T: Data, U: Data, M: Mapper<In = T, Out = U>>(
        &mut self,
        rdd: RddIndex<T>,
        mapper: M,
    ) -> RddIndex<U>;

    fn flat_map<T: Data, U: Data, I: IntoIterator<Item = U> + Data>(
        &mut self,
        rdd: RddIndex<T>,
        f: fn(T) -> I,
    ) -> RddIndex<U>;

    fn flat_map_with_state<T: Data, U: Data, I: IntoIterator<Item = U>, F: FlatMapper<In=T, OutIterable = I>>(
        &mut self,
        rdd: RddIndex<T>,
        flat_mapper: F,
    ) -> RddIndex<U>;

    fn filter<T: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> bool) -> RddIndex<T>;
    fn new_from_list<T: Data + Clone>(&mut self, data: Vec<Vec<T>>) -> RddIndex<T>;
}
