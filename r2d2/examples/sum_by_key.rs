use clap::Parser;
use r2d2::{
    core::{context::Context, spark::hash_partitioner::HashPartitioner, spark::Spark},
    Args,
};

#[tokio::main]
async fn main() {
    let config = Args::parse();
    let mut spark = Spark::new(config).await;

    let data = vec![
        vec![
            ("wow".to_string(), 1),
            ("ok".to_string(), 100),
            ("ok".to_string(), 200),
        ],
        vec![("wow".to_string(), 2), ("x".to_string(), 102)],
    ];
    let rdd = spark.new_from_list(data);
    let rdd = spark.sum_by_key(rdd, HashPartitioner::new(8));
    let result = spark.collect(rdd).await;
    println!("client code received result = {result:?}");
}
