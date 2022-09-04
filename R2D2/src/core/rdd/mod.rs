use std::{
    any::Any,
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::cache::ResultCache;

// TODO: maybe add uuid so that we can't pass one rdd index to another context
#[derive(Serialize, Deserialize)]
pub struct RddIndex<T> {
    pub id: RddId,
    #[serde(skip)]
    _data: PhantomData<T>,
}

impl<T> RddIndex<T> {
    pub fn new(id: RddId) -> Self {
        RddIndex {
            id,
            _data: PhantomData::default(),
        }
    }
}

impl<T> Clone for RddIndex<T> {
    fn clone(&self) -> RddIndex<T> {
        RddIndex::new(self.id)
    }
}

impl<T> Copy for RddIndex<T> {}

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct RddId(pub usize);
impl RddId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> RddId {
        static COUNTER: AtomicUsize = AtomicUsize::new(1);
        RddId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct RddPartitionId {
    pub rdd_id: RddId,
    pub partition_id: usize,
}

pub trait Data: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}

impl<T> Data for T where T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {}

// trait DataFetcher {
//     fn fetch_owned<T>(&mut self, idx: RddIndex<T>, partition_id: usize) -> Vec<T>;
//     fn fetch_borrowed<T>(&self, idx: RddIndex<T>, partition_id: usize) -> &[T];
//     fn post_result<T>(&mut self, idx: RddIndex<T>, partition_id: usize, data: Vec<T>);
// }

pub enum Dependency {
    Narrow(RddId),
    Wide(RddId),
    No,
}

pub trait RddSerde {
    /// serialize data returned by this rdd in a form which can be sent over the network
    fn serialize_raw_data(&self, raw_data: &(dyn Any + Send)) -> Vec<u8>;
    /// deserialize data in a way which can be ingested into following rdds
    fn deserialize_raw_data(&self, serialized_data: Vec<u8>) -> Box<dyn Any + Send>;
}

pub trait NarrowRddWork {
    /// This expects that all the deps have already been put inside the cache
    /// Ownership of results is passed back to context!
    // TODO: Is being generic over DataFetcher here fine???
    // maybe do enum_dispatch
    // TODO: maybe instead of ResultCache arg should be Vec<dyn Any> (materialized deps)
    fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any + Send>;
}

trait TypedRddWideWork {
    type K: Data;
    type V: Data;
    type C: Data;

    fn partition_data(
        &self,
        input_partition: Vec<(Self::K, Self::V)>,
    ) -> Vec<Vec<(Self::K, Self::V)>>;
    fn aggregate_inside_bucket(
        &self,
        bucket_data: Vec<(Self::K, Self::V)>,
    ) -> Vec<(Self::K, Self::C)>;
    fn aggregate_buckets(
        &self,
        buckets_aggr_data: Vec<Vec<(Self::K, Self::C)>>,
    ) -> Vec<(Self::K, Self::C)>;
}

// impl TypedRddWideWork {}

pub trait RddWideWork {
    /// distributes data in local buckets according to K
    fn partition_data(&self, input_partition: Box<dyn Any + Send>) -> Vec<Box<dyn Any + Send>>;

    /// aggregates data in single bucket
    fn aggregate_inside_bucket(&self, bucket_data: Box<dyn Any + Send>) -> Box<dyn Any + Send>;

    /// aggregates data from multiple buckets into single Vector
    fn aggregate_buckets(&self, buckets_aggr_data: Vec<Box<dyn Any + Send>>)
        -> Box<dyn Any + Send>;
}

/// methods independent of `Item` type
///per partition:
// 1.
// RddBase should have
// partition_data Vec<(K, V)> --> Vec<Vec<(K, V)>>
// 2. aggregate bucket
// aggregate_inside_bucket Vec<(K, V)> --> Vec<(K, C)>
// 3.
// aggregate_buckets Vec<Vec<(K, C)>> --> Vec<(K, C)>

impl<T> RddWideWork for T
where
    T: TypedRddWideWork,
{
    fn partition_data(&self, input_partition: Box<dyn Any + Send>) -> Vec<Box<dyn Any + Send>> {
        Self::partition_data(self, *input_partition.downcast().unwrap())
            .into_iter()
            .map(|x| -> Box<dyn Any + Send> { Box::new(x) })
            .collect()
    }
    fn aggregate_inside_bucket(&self, bucket_data: Box<dyn Any + Send>) -> Box<dyn Any + Send> {
        Box::new(Self::aggregate_inside_bucket(
            self,
            *bucket_data.downcast().unwrap(),
        ))
    }
    fn aggregate_buckets(
        &self,
        buckets_aggr_data: Vec<Box<dyn Any + Send>>,
    ) -> Box<dyn Any + Send> {
        Box::new(Self::aggregate_buckets(
            self,
            buckets_aggr_data
                .into_iter()
                .map(|x| *x.downcast().unwrap())
                .collect(),
        ))
    }
}

pub enum RddWorkFns<'a> {
    Narrow(&'a dyn NarrowRddWork),
    Wide(&'a dyn RddWideWork),
}

pub trait RddBase:
    RddBaseClone
    + Send
    + Sync
    + RddSerde
    + serde_traitobject::Serialize
    + serde_traitobject::Deserialize
{
    /// Fetch unique id for this rdd
    fn id(&self) -> RddId;

    /// rdd dependencies
    fn rdd_dependency(&self) -> Dependency;

    fn partitions_num(&self) -> usize;

    fn work_fns(&self) -> RddWorkFns;
}

/// Magic incantaions to make `dyn RddBase` `Clone`
pub trait RddBaseClone {
    fn clone_box(&self) -> Box<dyn RddBase>;
}

/// Magic incantaions to make `dyn RddBase` `Clone`
impl<T> RddBaseClone for T
where
    T: 'static + RddBase + Clone,
{
    fn clone_box(&self) -> Box<dyn RddBase> {
        Box::new(self.clone())
    }
}

/// Magic incantaions to make `dyn RddBase` `Clone`
impl Clone for Box<dyn RddBase> {
    fn clone(&self) -> Box<dyn RddBase> {
        self.clone_box()
    }
}

trait TypedNarrowRddWork {
    type Item: Data;

    fn work(&self, cache: &ResultCache, partition_id: usize) -> Vec<Self::Item>;
}

/// methods for Rdd which are dependent on `Item` type
trait TypedRdd {
    type Item: Data;
}

impl<T> RddSerde for T
where
    T: TypedRdd,
{
    fn serialize_raw_data(&self, raw_data: &(dyn Any + Send)) -> Vec<u8> {
        let data: &Vec<T::Item> = raw_data.downcast_ref().unwrap();
        serde_json::to_vec(data).unwrap()
    }

    fn deserialize_raw_data(&self, serialized_data: Vec<u8>) -> Box<dyn Any + Send> {
        let data: Vec<T::Item> = serde_json::from_slice(&serialized_data).unwrap();
        Box::new(data)
    }
}

impl<T> NarrowRddWork for T
where
    T: TypedNarrowRddWork,
{
    fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any + Send> {
        Box::new(Self::work(self, cache, partition_id))
    }
}

pub mod data_rdd;

pub mod map_rdd;

pub mod flat_map_rdd;

pub mod filter_rdd;

pub mod shuffle_rdd;

pub mod map_partitions;

pub mod sample_rdd;

// crate::core::rdd::RddBase;
// crate::core::rdd::map_rdd::MapRdd;
// ``
