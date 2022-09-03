use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::rdd::{Data, RddId, RddIndex, RddPartitionId};

#[derive(Clone, Default, Debug)]
pub struct ResultCache {
    data: Arc<Mutex<HashMap<RddPartitionId, Box<dyn Any + Send>>>>,
}

impl ResultCache {
    pub fn has(&self, id: RddPartitionId) -> bool {
        self.data.lock().unwrap().contains_key(&id)
    }

    pub fn put(&mut self, id: RddPartitionId, data: Box<dyn Any + Send>) {
        self.data.lock().unwrap().insert(id, data);
    }

    // pub fn get_as<T: Data>(&self, rdd: RddIndex<T>, partition_id: usize) -> Option<&[T]> {
    //     self.data
    //         .get(&RddPartitionId {
    //             rdd_id: rdd.id,
    //             partition_id,
    //         })
    //         .map(|b| b.downcast_ref::<Vec<T>>().unwrap().as_slice())
    // }

    pub fn take_as<T: Data>(
        &self,
        rdd: RddIndex<T>,
        partition_id: usize,
        cache: bool,
    ) -> Option<Vec<T>> {
        let mut map = self.data.lock().unwrap();
        let rpid = RddPartitionId {
            rdd_id: rdd.id,
            partition_id,
        };
        if cache {
            map.get(&rpid)
                .map(|b| b.downcast_ref::<Vec<T>>().unwrap().clone())
        } else {
            map.remove(&rpid).map(|b| *b.downcast().unwrap())
        }
    }

    // pub fn get_as_any(&self, rdd_id: RddId, partition_id: usize) -> Option<&(dyn Any + Send)> {
    //     self.data
    //         .get(&RddPartitionId {
    //             rdd_id,
    //             partition_id,
    //         })
    //         .map(|b| b.as_ref())
    // }

    pub fn take_as_any(
        &mut self,
        rdd_id: RddId,
        partition_id: usize,
    ) -> Option<Box<dyn Any + Send>> {

        let mut map = self.data.lock().unwrap();
        let rpid = RddPartitionId {
            rdd_id,
            partition_id,
        };
        map.remove(&rpid)
    }
}
