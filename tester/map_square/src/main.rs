use clap::Parser;
use R2D2::{
    core::{context::Context, spark::Spark},
    Args,
};

#[tokio::main]
async fn main() {
    let config = Args::parse();
    let mut spark = Spark::new(config).await;

    let data = vec![
        vec![1, 2, 3, 4],
        vec![1, 2],
        vec![1, 2, 3, 4],
        vec![1, 2, 3, 4],
    ];
    let rdd = spark.new_from_list(data);
    let rdd = spark.filter(rdd, |x| x % 2 == 0);
    let rdd = spark.map(rdd, |x| vec![x; x as usize]);
    let rdd = spark.flat_map(rdd, |x| x);
    let result = spark.collect(rdd).await;
    println!("client code received result = {:?}", result);
}
