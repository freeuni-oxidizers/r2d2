use clap::Parser;
use r2d2::{
    core::{context::Context, spark::Spark},
    Args,
};

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
    let rdd = spark.flat_map(rdd, |x|vec![x; x]);
    let rdd = spark.map(rdd, |x|2*x);
    let result = spark.collect(rdd).await;
    println!("client code received result = {result:?}");
}
