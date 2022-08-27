use clap::Parser;
use R2D2::rdd::Spark;
use R2D2::rdd::*;
use R2D2::Config;

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let mut spark = Spark::new(config).await;

    let data = vec![1,2,3,4];
    let rdd1 = spark.new_from_list(data);
    let rdd2 = spark.map(rdd1, |x| (x * x) as i32);
    let result = spark.collect(rdd2);
    println!("client code received result = {:?}", result);

    spark.termiante().await;
}
