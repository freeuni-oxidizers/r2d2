use clap::Parser;
use r2d2::{
    core::{
        context::Context, spark::hash_partitioner::HashPartitioner,
        spark::Spark,
    },
    Args,
};

#[tokio::main]
async fn main() {
    let config = Args::parse();
    let mut spark = Spark::new(config).await;

    let a = vec![vec![(1, 4)], vec![(1, 2)]];
    let b = vec![vec![(1, "wow".to_string())], vec![(2, "bla".to_string())]];
    let a = spark.new_from_list(a);
    let b = spark.new_from_list(b);

    let partitioner = HashPartitioner::new(2);
    let ab = spark.join(a, b, partitioner);
    let v = spark.collect(ab).await;
    println!("{:?}", v);
    // let rdd = spark.new_from_list(data);
    // let rdd = spark.filter(rdd, |x| x % 2 == 0);
    // let rdd = spark.flat_map(rdd, |x| x);
    // let rdd = spark.map(rdd, |x| (x, 1));
    // let mut input = [0; 100];
    // for i in 0..100 {
    //     input[i] = rand::thread_rng().gen();
    // }
    // dbg!(input);

    // let data = vec![input.to_vec()];

    // let rdd = spark.new_from_list(data);
    // let rdd = spark.sort(rdd, 10).await;
    // let rdd = spark.map_partitions(rdd, |partition, partition_id| {
    //     let file_name = format!("out_{partition_id}");
    //     let file_data = format!("{:?}", partition);
    //     vec![(file_name, file_data.as_bytes().to_owned())]
    // });
    // Rdd<T>

    // let sorted = spark.collect(rdd).await;
    // input.sort();
    // assert_eq!(sorted, input);

    //    spark
    //        .save(
    //            rdd,
    //            |partition| format!("{:?}", partition).into_bytes(),
    //            PathBuf::from("spark_job_output"),
    //        )
    //        .await;

    // Rdd<(path, data)>
    // let result = spark.collect(rdd).await;
    // println!("client code received result = {:?}", result);

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
