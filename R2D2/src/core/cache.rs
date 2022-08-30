use std::{any::Any, collections::HashMap};

use super::rdd::{Data, RddId, RddIndex, RddPartitionId};

#[derive(Default, Debug)]
pub struct ResultCache {
    data: HashMap<RddPartitionId, Box<dyn Any + Send>>,
}

impl ResultCache {
    pub fn has(&self, id: RddPartitionId) -> bool {
        self.data.contains_key(&id)
    }

    pub fn put(&mut self, id: RddPartitionId, data: Box<dyn Any + Send>) {
        self.data.insert(id, data);
    }

    pub fn get_as<T: Data>(&self, rdd: RddIndex<T>, partition_id: usize) -> Option<&[T]> {
        self.data
            .get(&RddPartitionId {
                rdd_id: rdd.id,
                partition_id,
            })
            .map(|b| b.downcast_ref::<Vec<T>>().unwrap().as_slice())
    }

    pub fn get_as_any(&self, rdd_id: RddId, partition_id: usize) -> Option<&(dyn Any + Send)> {
        self.data
            .get(&RddPartitionId {
                rdd_id,
                partition_id,
            })
            .map(|b| b.as_ref())
    }
}
