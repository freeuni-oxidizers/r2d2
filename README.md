# r2d2

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## About 

Research project around the possibility of more efficient implementation 
of Spark RDDs without compromising on user friendly API.

Tested on Linux using the Rust nightly.

## Running Locally

In one terminal run:
```bash
cd r2d2
cargo run --example sum_by_key -- --id 123 --master --port 6969
```

In another terminal run:
```bash
cd r2d2
./run_workers.sh sum_by_key debug
```

Example user codes can be found in `r2d2/examples` directory. Some examples 
might need specific folder setup with generated data.

## Running Remotely

You'll need to look inside `run_workers.sh` and `spark.toml` files first. To 
run remotely you need to compile binary with either debug or release profile, 
distribute the binary to all nodes, and run them on ports specified 
in `spark.toml`
