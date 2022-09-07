use std::{marker::PhantomData, ops::Add};

use serde::{Deserialize, Serialize};

use crate::core::rdd::{shuffle_rdd::Aggregator, Data};

#[derive(Clone, Serialize, Deserialize)]
pub struct SumByKeyAggregator<V> {
    #[serde(skip)]
    _value: PhantomData<V>,
}

impl<V> SumByKeyAggregator<V> {
    pub fn new() -> Self {
        SumByKeyAggregator {
            _value: PhantomData,
        }
    }
}

impl<V> Default for SumByKeyAggregator<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Data + Add<Output = V> + Default> Aggregator for SumByKeyAggregator<V> {
    type Value = V;

    type Combiner = V;

    fn create_combiner(&self) -> Self::Combiner {
        V::default()
    }

    fn merge_value(&self, value: Self::Value, combiner: Self::Combiner) -> Self::Combiner {
        combiner + value
    }

    fn merge_combiners(
        &self,
        combiner1: Self::Combiner,
        combiner2: Self::Combiner,
    ) -> Self::Combiner {
        combiner1 + combiner2
    }
}
