use std::{os::raw, path::PathBuf};

use clap::Parser;
use r2d2::{
    core::{context::Context, spark::hash_partitioner::HashPartitioner, spark::Spark},
    Args,
};
use serde::{Deserialize, Serialize};
use serde_traitobject::Any;

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
        raw_data
            .split(|b| *b == b'\n')
            .filter(|ln|ln.len()>0)
            .map(|ln| ln.split(|b| *b == b',').collect())
            .map(|row: Vec<_>| Row {
                id: String::from_utf8(row[0].to_vec()).unwrap().parse().unwrap(),
                name: String::from_utf8(row[1].to_vec()).unwrap(),
                surname: String::from_utf8(row[2].to_vec()).unwrap(),
                age: String::from_utf8(row[3].to_vec()).unwrap().parse().unwrap(),
                city: String::from_utf8(row[4].to_vec()).unwrap(),
            })
            .collect()
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
                let format_row = |row: &Row| {
                    format!(
                        "{},{},{},{},{}\n",
                        row.id, row.name, row.surname, row.age, row.city
                    )
                };
                let mut result = Vec::new();
                for (_, (row_1, row_2)) in x {
                    result.extend(format_row(&row_1).as_bytes());
                    if row_1.city != row_2.city {
                        result.extend(format_row(&row_2).as_bytes());
                    }
                }
                result
            },
            "out",
        )
        .await;
}
