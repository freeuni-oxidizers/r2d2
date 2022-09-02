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

    // FIXME: this needs &mut self
    pub fn take_as<T: Data>(&self, rdd: RddIndex<T>, partition_id: usize) -> Option<Vec<T>> {
        // self.data
        //     .remove(&RddPartitionId {
        //         rdd_id: rdd.id,
        //         partition_id,
        //     })
        //     .map(|b| *b.downcast::<Vec<T>>().unwrap())
        self.get_as(rdd, partition_id).map(|v| v.to_vec())
    }

    pub fn get_as_any(&self, rdd_id: RddId, partition_id: usize) -> Option<&(dyn Any + Send)> {
        self.data
            .get(&RddPartitionId {
                rdd_id,
                partition_id,
            })
            .map(|b| b.as_ref())
    }

    pub fn take_as_any(
        &mut self,
        rdd_id: RddId,
        partition_id: usize,
    ) -> Option<Box<dyn Any + Send>> {
        self.data.remove(&RddPartitionId {
            rdd_id,
            partition_id,
        })
    }
}
