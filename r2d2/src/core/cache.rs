use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use super::rdd::{Data, RddBase, RddId, RddIndex, RddPartitionId};

#[derive(Default, Debug)]
pub struct CacheInner {
    data: HashMap<RddPartitionId, Box<dyn Any + Send>>,
    should_keep: HashSet<RddPartitionId>,
}

#[derive(Clone, Default, Debug)]
pub struct ResultCache {
    inner: Arc<Mutex<CacheInner>>,
}

impl ResultCache {
    pub fn has(&self, id: RddPartitionId) -> bool {
        self.inner.lock().unwrap().data.contains_key(&id)
    }

    pub fn put(&mut self, id: RddPartitionId, data: Box<dyn Any + Send>) {
        self.inner.lock().unwrap().data.insert(id, data);
    }

    pub fn keep_when_taken(&mut self, id: RddPartitionId) {
        self.inner.lock().unwrap().should_keep.insert(id);
    }

    // pub fn get_as<T: Data>(&self, rdd: RddIndex<T>, partition_id: usize) -> Option<&[T]> {
    //     self.data
    //         .get(&RddPartitionId {
    //             rdd_id: rdd.id,
    //             partition_id,
    //         })
    //         .map(|b| b.downcast_ref::<Vec<T>>().unwrap().as_slice())
    // }

    pub fn take_as<T: Data>(&self, rdd: RddIndex<T>, partition_id: usize) -> Option<Vec<T>> {
        let mut inner = self.inner.lock().unwrap();
        let rpid = RddPartitionId {
            rdd_id: rdd.id,
            partition_id,
        };
        let cache = inner.should_keep.contains(&rpid);
        if cache {
            inner
                .data
                .get(&rpid)
                .map(|b| b.downcast_ref::<Vec<T>>().unwrap().clone())
        } else {
            inner.data.remove(&rpid).map(|b| *b.downcast().unwrap())
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
        rdd: &dyn RddBase,
    ) -> Option<Box<dyn Any + Send>> {
        let mut inner = self.inner.lock().unwrap();
        let rpid = RddPartitionId {
            rdd_id,
            partition_id,
        };
        let cache = inner.should_keep.contains(&rpid);
        if cache {
            inner.data.get(&rpid).map(|v| rdd.clone_any(v.as_ref()))
        } else {
            inner.data.remove(&rpid)
        }
    }
}
