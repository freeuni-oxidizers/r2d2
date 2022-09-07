use clap::Parser;
use r2d2::{
    core::{context::Context, spark::Spark, rdd::{flat_map_rdd::FlatMapper, map_rdd::Mapper}},
    Args,
};
use serde::{Serialize, Deserialize};

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
struct MulByTwo;

impl Mapper for MulByTwo {
    type In = usize;

    type Out = usize;

    fn map(&self, v: Self::In) -> Self::Out {
        v*2
    }
}


#[tokio::main]
async fn main() {
    let config = Args::parse();
    let mut spark = Spark::new(config).await;

    let data = vec![
        vec![
            123_123_123,
            123_123_124,
            123_123_125,
        ],
        vec![
            223_123_123,
            223_123_124,
            223_123_125,
        ],
        vec![
            423_123_123,
            423_123_124,
            423_123_125,
        ],
        vec![
            523_123_123,
            523_123_124,
            523_123_125,
        ],
    ];
    let rdd = spark.new_from_list(data);
    let rdd = spark.flat_map_with_state(rdd, ExpandNumber);
    let rdd = spark.map_with_state(rdd, MulByTwo);
    let result = spark.collect(rdd).await;
    println!("client code received result = {result:?}");
}
