use clap::Parser;
use r2d2::{
    core::{
        context::Context,
        spark::Spark,
    },
    Args,
};

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
    let rdd = spark.flat_map(rdd, |x| vec![x; x]);
    let rdd = spark.map(rdd, |x| 0);
    let rdd = spark.map(rdd, |x| 4 * x);
    let rdd = spark.map(rdd, |x| 5 * x);
    let rdd = spark.map_partitions(rdd, |x, _| vec![x.into_iter().sum::<usize>()]);
    let result = spark.collect(rdd).await;
    println!("client code received result = {result:?}");
}
