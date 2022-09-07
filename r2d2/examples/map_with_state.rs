use clap::Parser;
use r2d2::{
    core::{
        context::Context,
        rdd::{flat_map_rdd::FlatMapper, map_partitions::PartitionMapper, map_rdd::Mapper},
        spark::Spark,
    },
    Args,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct ExpandNumber;

impl FlatMapper for ExpandNumber {
    type In = usize;

    type OutIterable = Vec<usize>;

    fn map(&self, v: Self::In) -> Self::OutIterable {
        vec![v; v]
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct MakeTuple;

impl Mapper for MakeTuple {
    type In = usize;

    type Out = (usize, usize);

    fn map(&self, v: Self::In) -> Self::Out {
        (1, v * 2)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct MulByThree;

impl Mapper for MulByThree {
    type In = usize;

    type Out = usize;

    fn map(&self, v: Self::In) -> Self::Out {
        0
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct MulByFour;

impl Mapper for MulByFour {
    type In = usize;

    type Out = usize;

    fn map(&self, v: Self::In) -> Self::Out {
        v * 4
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct MulByFive;

impl Mapper for MulByFive {
    type In = usize;

    type Out = usize;

    fn map(&self, v: Self::In) -> Self::Out {
        v * 5
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Summer;

impl PartitionMapper for Summer {
    type In = usize;

    type Out = usize;

    fn map_partitions(&self, v: Vec<Self::In>, _: usize) -> Vec<Self::Out> {
        vec![v.into_iter().sum::<usize>()]
    }
}

#[tokio::main]
async fn main() {
    let config = Args::parse();
    let mut spark = Spark::new(config).await;

    let data = vec![
        vec![1023_123_12, 1023_123_12, 1023_123_12],
        vec![2023_123_12, 2023_123_12, 2023_123_12],
        vec![4023_123_12, 4023_123_12, 4023_123_12],
        vec![5023_123_12, 5023_123_12, 5023_123_12],
    ];
    let rdd = spark.new_from_list(data);
    let rdd = spark.flat_map_with_state(rdd, ExpandNumber);
    let rdd = spark.map_with_state(rdd, MulByThree);
    let rdd = spark.map_with_state(rdd, MulByFour);
    let rdd = spark.map_with_state(rdd, MulByFive);
    let rdd = spark.map_partitions_with_state(rdd, Summer);
    let result = spark.collect(rdd).await;
    println!("client code received result = {result:?}");
}
