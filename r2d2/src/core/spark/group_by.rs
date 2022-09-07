use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::core::rdd::{shuffle_rdd::Aggregator, Data};

#[derive(Clone, Serialize, Deserialize)]
pub struct GroupByAggregator<V> {
    #[serde(skip)]
    _value: PhantomData<V>,
}

impl<V> GroupByAggregator<V> {
    pub fn new() -> Self {
        GroupByAggregator {
            _value: PhantomData,
        }
    }
}

impl<V> Default for GroupByAggregator<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Data> Aggregator for GroupByAggregator<V> {
    type Value = V;

    type Combiner = Vec<V>;

    fn create_combiner(&self) -> Self::Combiner {
        Vec::new()
    }

    fn merge_value(&self, value: Self::Value, mut combiner: Self::Combiner) -> Self::Combiner {
        combiner.push(value);
        combiner
    }

    fn merge_combiners(
        &self,
        mut combiner1: Self::Combiner,
        combiner2: Self::Combiner,
    ) -> Self::Combiner {
        combiner1.extend(combiner2);
        combiner1
    }
}
