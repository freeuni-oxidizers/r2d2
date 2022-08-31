use std::ops::Add;

use super::rdd::{
    shuffle_rdd::{Aggregator, Partitioner},
    Data, RddIndex, map_rdd::Mapper,
};

/// All these methods use interior mutability to keep state
pub trait Context {
    fn shuffle<K, V, C, P, A>(
        &mut self,
        rdd: RddIndex<(K, V)>,
        partitioner: P,
        aggregator: A,
    ) -> RddIndex<(K, C)>
    where
        K: Data + Eq + std::hash::Hash,
        V: Data,
        C: Data,
        P: Partitioner<Key = K>,
        A: Aggregator<Value = V, Combiner = C>;
    fn group_by<K, V, P>(&mut self, rdd: RddIndex<(K, V)>, partitioner: P) -> RddIndex<(K, Vec<V>)>
    where
        K: Data + Eq + std::hash::Hash,
        V: Data,
        P: Partitioner<Key = K>;
    // fn partition_by<T, P>(&mut self, rdd: RddIndex<T>, partitioner: P) -> RddIndex<T>
    // where
    //     T: Data,
    //     P: Partitioner<Key = T>;
    fn sum_by_key<K, V, P>(&mut self, rdd: RddIndex<(K, V)>, partitioner: P) -> RddIndex<(K, V)>
    where
        K: Data + Eq + std::hash::Hash,
        V: Data + Add<Output = V> + Default,
        P: Partitioner<Key = K>;
    fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(T) -> U) -> RddIndex<U>;
    fn map_with_state<T: Data, U: Data, M: Mapper<In=T, Out=U>>(&mut self, rdd: RddIndex<T>, mapper: M) -> RddIndex<U>;
    fn filter<T: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> bool) -> RddIndex<T>;
    fn new_from_list<T: Data + Clone>(&mut self, data: Vec<Vec<T>>) -> RddIndex<T>;
}
