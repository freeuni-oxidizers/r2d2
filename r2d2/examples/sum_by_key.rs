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
            ("cougar".to_string(), 2),
            ("bear".to_string(), 3),
            ("bear".to_string(), 2),
            ("squirrel".to_string(), 1),
            ("turtle".to_string(), 6),
        ],
        vec![
            ("snake".to_string(), 2),
            ("turtle".to_string(), 5),
            ("bear".to_string(), 2),
            ("turtle".to_string(), 2),
            ("squirrel".to_string(), 2),
        ],
    ];
    let rdd = spark.new_from_list(data);
    let rdd = spark.sum_by_key(rdd, HashPartitioner::new(3));
    let result = spark.collect(rdd).await;
    println!("client code received result = {result:?}");
}
