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
pub struct RddId(usize);
impl RddId {
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

pub enum RddType {
    Narrow,
    Wide,
}

pub trait RddSerde {
    /// serialize data returned by this rdd in a form which can be sent over the network
    fn serialize_raw_data(&self, raw_data: &(dyn Any + Send)) -> Vec<u8>;
    /// deserialize data in a way which can be ingested into following rdds
    fn deserialize_raw_data(&self, serialized_data: Vec<u8>) -> Box<dyn Any + Send>;
}

pub trait RddWork {
    /// This expects that all the deps have already been put inside the cache
    /// Ownership of results is passed back to context!
    // TODO: Is being generic over DataFetcher here fine???
    // maybe do enum_dispatch
    fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any + Send>;
}

/// methods independent of `Item` type
pub trait RddBase:
    RddBaseClone
    + Send
    + Sync
    + RddWork
    + RddSerde
    + serde_traitobject::Serialize
    + serde_traitobject::Deserialize
{
    /// Fetch unique id for this rdd
    fn id(&self) -> RddId;

    /// rdd dependencies
    fn deps(&self) -> Vec<RddId>;

    fn rdd_type(&self) -> RddType;

    fn partitions_num(&self) -> usize;
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

/// methods for Rdd which are dependent on `Item` type
trait TypedRdd {
    type Item: Data;

    fn work(&self, cache: &ResultCache, partition_id: usize) -> Vec<Self::Item>;
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

impl<T> RddWork for T
where
    T: TypedRdd,
{
    fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any + Send> {
        Box::new(Self::work(&self, cache, partition_id))
    }
}

pub mod data_rdd;

pub mod map_rdd;

pub mod filter_rdd;

// crate::core::rdd::RddBase;
// crate::core::rdd::map_rdd::MapRdd;

