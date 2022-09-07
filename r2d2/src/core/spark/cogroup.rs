#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum VW<V, W> {
    Left(V),
    Right(W),
}

use serde::{Serialize, Deserialize};
use super::super:: rdd::{map_rdd::Mapper, Data}; 
use std::marker::PhantomData;

#[derive(Clone, Serialize, Deserialize)]
pub struct Unzip<K, V, W> {
    #[serde(skip)]
    _data: PhantomData<(K, V, W)>
}

impl<K, V, W> Default for Unzip<K, V, W> {
    fn default() -> Self {
        Self { _data: PhantomData::default() }
    }
}

impl<K, V, W> Mapper for Unzip<K, V, W>
where
    K: Data + Eq + std::hash::Hash,
    V: Data,
    W: Data,
{
    type In = (K, Vec<VW<V, W>>);
    type Out = (K, (Vec<V>, Vec<W>));

    fn map(&self, v: Self::In) -> Self::Out {
        let (k, values) = v;
        let (mut left, mut right) = (Vec::new(), Vec::new());
        for vw in values {
            match vw {
                VW::Left(v) => left.push(v),
                VW::Right(w) => right.push(w),
            }
        }
        (k, (left, right))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Lefter<K, V, W> {
    #[serde(skip)]
    _data: PhantomData<(K, V, W)>
}

impl<K, V, W> Default for Lefter<K, V, W> {
    fn default() -> Self {
        Self { _data: PhantomData::default() }
    }
}

impl<K, V, W> Mapper for Lefter<K, V, W>
where
    K: Data + Eq + std::hash::Hash,
    V: Data,
    W: Data,
{
    type In = (K, V);
    type Out = (K, VW<V, W>);

    fn map(&self, v: Self::In) -> Self::Out {
        let (k, v) = v;
        (k, VW::Left(v))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Righter<K, V, W> {
    #[serde(skip)]
    _data: PhantomData<(K, V, W)>
}

impl<K, V, W> Default for Righter<K, V, W> {
    fn default() -> Self {
        Self { _data: PhantomData::default() }
    }
}

impl<K, V, W> Mapper for Righter<K, V, W>
where
    K: Data + Eq + std::hash::Hash,
    V: Data,
    W: Data,
{
    type In = (K, W);
    type Out = (K, VW<V, W>);

    fn map(&self, v: Self::In) -> Self::Out {
        let (k, v) = v;
        (k, VW::Right(v))
    }
}
