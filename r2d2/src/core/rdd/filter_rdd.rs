use serde::{Deserialize, Serialize};

use super::{Data, Dependency, RddBase, RddId, RddIndex, RddWorkFns, TypedNarrowRddWork, TypedRdd};

pub trait Filterer: Data {
    type Item: Data;

    fn predicate(&self, v: &Self::Item) -> bool;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FnPtrFilterer<T>(#[serde(with = "serde_fp")] pub fn(&T) -> bool);

impl<T> Filterer for FnPtrFilterer<T>
where
    T: Data,
{
    type Item = T;

    fn predicate(&self, v: &Self::Item) -> bool {
        self.0(v)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FilterRdd<T, F> {
    pub idx: RddIndex<T>,
    pub partitions_num: usize,
    pub prev: RddIndex<T>,
    pub filterer: F,
}

impl<T, F> TypedRdd for FilterRdd<T, F>
where
    T: Data,
    F: Filterer<Item = T>,
{
    type Item = T;
}

impl<T, F> TypedNarrowRddWork for FilterRdd<T, F>
where
    T: Data,
    F: Filterer<Item = T>,
{
    type InputItem = T;
    type OutputItem = T;

    fn work(
        &self,
        input_partition: Option<Vec<Self::InputItem>>,
        _partition_id: usize,
    ) -> Vec<Self::OutputItem> {
        let g: Vec<T> = input_partition
            .unwrap()
            .into_iter()
            .filter(|v| self.filterer.predicate(v))
            .collect();
        g
    }
}

impl<T, F> RddBase for FilterRdd<T, F>
where
    T: Data,
    F: Filterer<Item = T>,
{
    fn id(&self) -> RddId {
        self.idx.id
    }

    fn rdd_dependency(&self) -> Dependency {
        Dependency::Narrow(self.prev.id)
    }

    fn partitions_num(&self) -> usize {
        self.partitions_num
    }

    fn work_fns(&self) -> RddWorkFns {
        RddWorkFns::Narrow(self)
    }
}
