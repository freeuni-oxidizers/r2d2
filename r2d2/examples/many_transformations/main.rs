use clap::Parser;
use r2d2::{
    core::{context::Context, spark::Spark},
    Args,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct Row {
    id: usize,
    name: String,
    surname: String,
    age: usize,
    city: String,
}

#[tokio::main]
async fn main() {
    let config = Args::parse();
    let mut spark = Spark::new(config).await;

    let data = vec![
        vec![10000000, 10000000, 10000000],
        vec![10000000, 10000000, 10000000],
        vec![10000000, 10000000, 10000000],
        vec![10000000, 10000000, 10000000],
    ];

    let rdd = spark.new_from_list(data);
    let mut a_init = spark.flat_map(rdd, |x| vec![x as u64; x]);

    let data = vec![
        vec![5000000, 5000000, 5000000],
        vec![5000000, 5000000, 5000000],
        vec![5000000, 5000000, 5000000],
        vec![5000000, 5000000, 5000000],
        vec![5000000, 5000000, 5000000],
        vec![5000000, 5000000, 5000000],
    ];

    let rdd = spark.new_from_list(data);
    let mut b_init = spark.flat_map(rdd, |x| vec![x as u64; x]);

    for _ in 0..10 {
        let a = spark.map(a_init, |x| x * 2);
        let b = spark.map(b_init, |x| x * 20);

        let a = spark.map(a, |x| (x as f64).sin());
        let b = spark.map(b, |x| (x as f64).cos());

        let a = spark.filter(a, |x| *x < 0.5);
        let b = spark.filter(b, |x| *x >= 0.5);

        let a = spark.map(a, |x| ((x * 1000.0).floor() as u64, x));
        let b = spark.map(b, |x| ((x * 1000.0).floor() as u64, x));

        let all = spark.union(&[a, b]);

        let a = spark.filter(all, |(k, _)| *k >= 500);
        let b = spark.filter(all, |(k, _)| *k < 500);

        a_init = spark.map(a, |(k, _)| k);
        b_init = spark.map(b, |(k, _)| k);
    }
    // let a_init = spark.map(a_init, |_| ());
    // let b_init = spark.map(b_init, |_| ());

    spark.collect(a_init).await;
    // spark.collect(b_init).await;
}
