use clap::Parser;
use R2D2::{Config, rdd::{spark::Spark, context::Context}};

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let mut spark = Spark::new(config);

    let data = vec![vec![1,2,3,4], vec![1,2]];
    let rdd1 = spark.new_from_list(data);
    let rdd2 = spark.map(rdd1, |x| (x * x) as i32);
    let rdd2 = spark.filter(rdd2, |x| x%2 == 0);
    let result = spark.collect(rdd2).await;
    println!("client code received result = {:?}", result);

    // spark.termiante().await;
}
