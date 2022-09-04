use clap::Parser;
use R2D2::{
    core::{context::Context, spark::Spark},
    Args,
};

#[tokio::main]
async fn main() {
    let config = Args::parse();
    let mut spark = Spark::new(config).await;

    // let data = vec![
    //     vec![1, 2, 3, 4],
    //     vec![1, 2],
    //     vec![1, 2, 3, 4],
    //     vec![1, 2, 3, 4],
    // ];
    // let rdd = spark.new_from_list(data);
    // let rdd = spark.filter(rdd, |x| x % 2 == 0);
    // let rdd = spark.flat_map(rdd, |x| x);
    // let rdd = spark.map(rdd, |x| (x, 1));
    let data = vec![
        vec![2000000],
        vec![10000],
        vec![3],
        vec![1000000],
        vec![3000000],
        vec![3000000],
    ];
    let rdd = spark.new_from_list(data);
    let rdd = spark.sort(rdd).await;
    let result = spark.collect(rdd).await;
    println!("client code received result = {:?}", result);

    // let rdd = spark.map(rdd, |x| vec![x; x]);
    // let rdd = spark.flat_map(rdd, |x| x);
    // let rdd = spark.map(rdd, |x| (x, 1));
    // // let rdd = spark.group_by(rdd, HashPartitioner::new(2));
    // let rdd = spark.sum_by_key(rdd, HashPartitioner::new(1));
    // // let rdd = spark.sum_by_key(rdd, HashPartitioner::new(3));
    // // let rdd = spark.sum_by_key(rdd, HashPartitioner::new(8));
    // let result = spark.collect(rdd).await;
    // println!("client code received result = {:?}", result);
    // let result = spark.collect(rdd).await;
    // println!("client code received result = {:?}", result);
}
