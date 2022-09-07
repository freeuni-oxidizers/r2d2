use crate::core::rdd::{map_partitions::PartitionMapper, Data};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone)]
pub struct FileWriter<T: Data> {
    path: PathBuf,
    #[serde(with = "serde_fp")]
    serializer: fn(Vec<T>) -> Vec<u8>,
}

impl<T: Data> FileWriter<T> {
    pub fn new(path: PathBuf, serializer: fn(Vec<T>) -> Vec<u8>) -> Self {
        Self { path, serializer }
    }
}
impl<T> PartitionMapper for FileWriter<T>
where
    T: Data,
{
    type In = T;
    type Out = ();

    // Rdd<T> -> <./, Vec<T> -> Vec<u8>>
    // Rdd<(K, V)> K -> V
    fn map_partitions(&self, v: Vec<Self::In>, _partitition_id: usize) -> Vec<Self::Out> {
        // TODO: make it deterministic?
        let serialized_partition = (self.serializer)(v);
        std::fs::write(
            self.path.join(_partitition_id.to_string()),
            serialized_partition,
        )
        .unwrap();
        Vec::new()
    }
}
