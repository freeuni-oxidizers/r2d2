use serde::{Serialize, Deserialize};
use super::super:: rdd::{flat_map_rdd::FlatMapper, Data}; 
use std::marker::PhantomData;

#[derive(Clone, Serialize, Deserialize)]
pub struct Cartesian<K, V, W> {
    #[serde(skip)]
    _data: PhantomData<(K, V, W)>
}

impl<K, V, W> Default for Cartesian<K, V, W> {
    fn default() -> Self {
        Self { _data: PhantomData::default() }
    }
}

impl<K, V, W> FlatMapper for Cartesian<K, V, W>
where
    K: Data + Eq + std::hash::Hash,
    V: Data,
    W: Data,
{
    type In = (K, (Vec<V>, Vec<W>));
    type OutIterable = Vec<(K, (V, W))>;

    fn map(&self, v: Self::In) -> Self::OutIterable {
        let (k, (vec_v, vec_w)) = v;
        let mut res = Vec::new();
        for v in vec_v {
            for w in vec_w.clone() {
                res.push((k.clone(), (v.clone(), w)));
            }
        }
        res
        // vec_v.into_iter().flat_map(|v| {
        //     vec_w.clone().into_iter().map(|w| {
        //         (k.clone(), (v.clone(), w))
        //     }).collect::<Vec<_>>()
        // }).collect::<Vec<_>>()
    }
}
