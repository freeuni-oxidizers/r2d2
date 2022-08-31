use serde::{Deserialize, Serialize};

use crate::core::cache::ResultCache;

use super::{Data, RddBase, RddId, RddIndex, RddType, RddWorkFns, TypedNarrowRddWork, TypedRdd};

pub trait Mapper: Data {
    type In: Data;
    type Out: Data;

    fn map(&self, v: Self::In) -> Self::Out;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FnPtrMapper<T, U> (#[serde(with = "serde_fp")] pub fn(T) -> U);

impl<T, U> Mapper for FnPtrMapper<T, U>
where T: Data, U: Data{
    type In=T;

    type Out=U;

    fn map(&self, v: Self::In) -> Self::Out {
        self.0(v)
    }
}


// TODO: maybe no pub?
#[derive(Clone, Serialize, Deserialize)]
pub struct MapRdd<T, U, M> {
    pub idx: RddIndex<U>,
    pub prev: RddIndex<T>,
    pub partitions_num: usize,
    pub mapper: M,
}

impl<T, U, M> TypedRdd for MapRdd<T, U, M>
where
    T: Data,
    U: Data,
    M: Mapper<In = T, Out = U>
{
    type Item = U;
}

impl<T, U, M> TypedNarrowRddWork for MapRdd<T, U, M>
where
    T: Data,
    U: Data,
    M: Mapper<In = T, Out = U>
{
    type Item = U;

    fn work(&self, cache: &ResultCache, partition_id: usize) -> Vec<Self::Item> {
        // TODO: pass dependency in don't just take
        let v = cache.take_as(self.prev, partition_id).unwrap();
        let g: Vec<U> = v.into_iter().map(|v|self.mapper.map(v)).collect();
        g
    }
}

impl<T, U, M> RddBase for MapRdd<T, U, M>
where
    T: Data,
    U: Data,
    M: Mapper<In = T, Out = U>
{
    fn id(&self) -> RddId {
        self.idx.id
    }

    fn deps(&self) -> Vec<RddId> {
        vec![self.prev.id]
    }

    fn rdd_type(&self) -> super::RddType {
        RddType::Narrow
    }

    fn partitions_num(&self) -> usize {
        self.partitions_num
    }

    fn work_fns(&self) -> RddWorkFns {
        RddWorkFns::Narrow(self)
    }
}
