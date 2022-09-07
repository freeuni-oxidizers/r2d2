use clap::Parser;
use r2d2::{
    core::{context::Context, spark::hash_partitioner::HashPartitioner, spark::Spark},
    Args,
};

#[tokio::main]
async fn main() {
    let config = Args::parse();
    let mut spark = Spark::new(config).await;

    let rdd_in = spark.read_partitions_from("./in", 10);
    let lines = spark.flat_map(rdd_in, |(_path, text)| {
        text.split(|b| *b == b'\n')
            .map(|ln| ln.to_vec())
            .collect::<Vec<_>>()
    });
    let words = spark.flat_map(lines, |text| {
        text.split(|b| *b == b' ')
            .map(|ln| ln.to_vec())
            .collect::<Vec<_>>()
    });
    let pairs = spark.map(words, |word| (word, 1));
    let summed = spark.sum_by_key(pairs, HashPartitioner::new(10));
    spark
        .save(
            summed,
            |partition| {
                partition
                    .into_iter()
                    .flat_map(|(w, c)| {
                        format!("{}:{c}\n", String::from_utf8(w).unwrap())
                            .bytes()
                            .collect::<Vec<_>>()
                    })
                    .collect()
            },
            "./out",
        )
        .await;
}

