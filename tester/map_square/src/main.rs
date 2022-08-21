use clap::Parser;
use R2D2::rdd::Spark;
use R2D2::rdd::*;
use R2D2::Config;

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let input = config.intput_path.clone();
    // let output = config.output_path.clone();
    let input = std::fs::read_to_string(input).unwrap();
    let data: Vec<i32> = serde_json::from_str(input.as_str()).unwrap();

    let mut spark = Spark::new(config).await;
    let rdd1 = spark.new_from_list(data);
    let rdd2 = spark.map(rdd1, |x| x * x);
    spark.collect(rdd2);

    spark.termiante().await;
}
