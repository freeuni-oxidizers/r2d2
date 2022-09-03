use serde::{Deserialize, Serialize};

use super::{Data, Dependency, RddBase, RddId, RddIndex, RddWorkFns, TypedNarrowRddWork, TypedRdd};

/// Imagine you have Rdd<Vec<T>> and want to get Rdd<T>. In this case each map would take element
/// which is Vec<T> from previous Rdd, so In=Vec<T>, and would output something that is iterable
/// that's why we have `OutIterable`. In this case OutIterable would be Vec<T> as well which would
/// be flattened by `flat_map` itself. I'm following api from rust's iterators `flat_map`.
pub trait FlatMapper: Data {
    type In: Data;
    // TODO: idk is this the best we can do? :)
    type OutIterable: IntoIterator;

    fn map(&self, v: Self::In) -> Self::OutIterable;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FnPtrFlatMapper<T, I>(#[serde(with = "serde_fp")] pub fn(T) -> I);

impl<T, U, I> FlatMapper for FnPtrFlatMapper<T, I>
where
    T: Data,
    U: Data,
    I: IntoIterator<Item = U> + Data,
{
    type In = T;

    type OutIterable = I;

    fn map(&self, v: Self::In) -> Self::OutIterable {
        self.0(v)
    }
}

// TODO: maybe no pub?
#[derive(Clone, Serialize, Deserialize)]
pub struct FlatMapRdd<T, U, M> {
    pub idx: RddIndex<U>,
    pub prev: RddIndex<T>,
    pub partitions_num: usize,
    pub flat_mapper: M,
}

impl<T, U, M, I> TypedRdd for FlatMapRdd<T, U, M>
where
    T: Data,
    U: Data,
    I: IntoIterator<Item = U>,
    M: FlatMapper<In = T, OutIterable = I>,
{
    type Item = U;
}

impl<T, U, M, I> TypedNarrowRddWork for FlatMapRdd<T, U, M>
where
    T: Data,
    U: Data,
    I: IntoIterator<Item = U>,
    M: FlatMapper<In = T, OutIterable = I>,
{
    type InputItem = T;
    type OutputItem = U;

    fn work(
        &self,
        input_partition: Option<Vec<Self::InputItem>>,
        _partition_id: usize,
    ) -> Vec<Self::OutputItem> {
        let g: Vec<U> = input_partition
            .unwrap()
            .into_iter()
            .flat_map(|v| self.flat_mapper.map(v))
            .collect();
        g
    }
}

impl<T, U, M, I> FlatMapRdd<T, U, M>
where
    T: Data,
    U: Data,
    I: IntoIterator<Item = U>,
    M: FlatMapper<In = T, OutIterable = I>,
{
    pub fn work(&self, v: Vec<T>) -> Vec<U> {
        let g: Vec<U> = v
            .into_iter()
            .flat_map(|v| self.flat_mapper.map(v))
            .collect();
        g
    }
}

impl<T, U, M, I> RddBase for FlatMapRdd<T, U, M>
where
    T: Data,
    U: Data,
    I: IntoIterator<Item = U>,
    M: FlatMapper<In = T, OutIterable = I>,
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

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[derive(Clone, Serialize, Deserialize)]
    struct FlattenVec;

    impl FlatMapper for FlattenVec {
        type In = Vec<usize>;

        type OutIterable = Vec<usize>;

        fn map(&self, v: Self::In) -> Self::OutIterable {
            v
        }
    }

    #[test]
    fn test_flat_mapper_basic() {
        let rdd = FlatMapRdd {
            idx: RddIndex::new(RddId::new()),
            prev: RddIndex::new(RddId::new()),
            partitions_num: 10,
            flat_mapper: FlattenVec,
        };
        assert_eq!(
            rdd.work(vec![vec![1, 2, 3], vec![1, 2]]),
            vec![1, 2, 3, 1, 2]
        );
    }
}
