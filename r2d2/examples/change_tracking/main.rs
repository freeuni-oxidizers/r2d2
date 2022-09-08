use std::{os::raw, path::PathBuf};

use clap::Parser;
use r2d2::{
    core::{context::Context, spark::hash_partitioner::HashPartitioner, spark::Spark},
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

    let rdd_in_1 = spark.read_partitions_from("people_jan", 10);
    let rdd_in_2 = spark.read_partitions_from("people_feb", 10);

    let raw_to_row_data: fn((PathBuf, Vec<u8>)) -> Vec<Row> = |(_, raw_data)| {
        let mut slc = raw_data.as_slice();
        let mut rdr = csv::Reader::from_reader(&mut slc);
        let mut data = Vec::new();
        for row in rdr.deserialize() {
            let record: Row = row.unwrap();
            data.push(record);
        }
        data
    };

    let row_rdd_1 = spark.flat_map(rdd_in_1, raw_to_row_data);
    let row_rdd_2 = spark.flat_map(rdd_in_2, raw_to_row_data);

    let id_row_1 = spark.map(row_rdd_1, |row| (row.id, row));
    let id_row_2 = spark.map(row_rdd_2, |row| (row.id, row));

    let joined_rdds = spark.join(id_row_1, id_row_2, HashPartitioner::new(10));

    let _unified_files = spark
        .save(
            joined_rdds,
            |x| {
                let mut writer = csv::Writer::from_writer(vec![]);
                for (_, (row_1, row_2)) in x {
                    if row_1.city != row_2.city {
                        writer.serialize(row_1).unwrap();
                        writer.serialize(row_2).unwrap();
                    } else {
                        writer.serialize(row_1).unwrap();
                    }
                }
                writer.into_inner().unwrap()
            },
            "out",
        )
        .await;
}
