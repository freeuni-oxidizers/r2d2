use serde::{Deserialize, Serialize};

use super::{Data, Dependency, RddBase, RddId, RddIndex, RddWorkFns, TypedNarrowRddWork, TypedRdd};

#[derive(Clone, Serialize, Deserialize)]
pub struct DataRdd<T> {
    pub idx: RddIndex<T>,
    pub partitions_num: usize,
    pub data: Vec<Vec<T>>,
}

impl<T> TypedRdd for DataRdd<T>
where
    T: Data,
{
    type Item = T;
}

impl<T> TypedNarrowRddWork for DataRdd<T>
where
    T: Data,
{
    type InputItem = T;
    type OutputItem = T;

    fn work(
        &self,
        _input_partition: Option<Vec<Self::InputItem>>,
        partition_id: usize,
    ) -> Vec<Self::OutputItem> {
        self.data[partition_id].clone()
    }
}

impl<T> RddBase for DataRdd<T>
where
    T: Data + Clone,
{
    fn id(&self) -> RddId {
        self.idx.id
    }

    fn rdd_dependency(&self) -> Dependency {
        Dependency::No
    }

    fn partitions_num(&self) -> usize {
        self.partitions_num
    }

    fn work_fns(&self) -> RddWorkFns {
        RddWorkFns::Narrow(self)
    }
}
