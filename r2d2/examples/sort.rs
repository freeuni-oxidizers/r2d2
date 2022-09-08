use clap::Parser;
use r2d2::{
    core::{context::Context, spark::Spark},
    Args,
};

#[tokio::main]
async fn main() {
    let config = Args::parse();
    let mut spark = Spark::new(config).await;

    let rdd = spark.read_partitions_from("in", 10);


    let rdd = spark.flat_map(rdd, |(_, raw_data)| {
        raw_data.split(|b| *b == b'\n')
            .map(|num| String::from_utf8(num.to_vec()).unwrap().parse().unwrap())
            .collect::<Vec<usize>>()
    });
    
    let rdd = spark.sort(rdd, 10).await;

    spark.save(rdd, |nums| {
        let mut result = Vec::new();
        for n in nums {
            result.extend(format!("{}\n", n).as_bytes());
        }
        result
    },"out",).await;
}
