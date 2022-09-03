use crate::core::executor::Executor;
use crate::core::graph::Graph;
use crate::core::rdd::RddPartitionId;
use crate::core::task_scheduler::{WorkerEvent, WorkerMessage};
use crate::r2d2::master_client::MasterClient;
use crate::r2d2::{GetTaskRequest, TaskResultRequest};
use crate::worker::bucket_receiver::receiver_loop;
use crate::Config;
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tonic::transport::Channel;
use tonic::Request;

async fn get_master_client(
    master_addr: String,
) -> Result<MasterClient<Channel>, tonic::transport::Error> {
    let mut retries = 3;
    let mut wait = 100;
    loop {
        match MasterClient::connect(master_addr.clone()).await {
            Err(_) if retries > 0 => {
                retries -= 1;
                sleep(Duration::from_millis(wait)).await;
                wait *= 2;
            }
            master_conn => break master_conn,
        }
    }
}

pub(crate) mod bucket_receiver {
    use core::fmt;
    use std::path::{Path, PathBuf};

    use tokio::{
        fs,
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
    };
    use tonic::{transport::Channel, Request};

    use crate::{
        core::{
            rdd::{RddId, RddPartitionId},
            task_scheduler::WorkerEvent,
        },
        r2d2::{master_client::MasterClient, TaskResultRequest},
    };

    #[derive(Debug)]
    pub(crate) enum Error {
        FsError,
        NetworkError,
        AlreadyExists,
    }

    impl std::error::Error for Error {}

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Error::FsError => write!(f, "FS error when creating bucket file"),
                Error::NetworkError => write!(f, "Error when receiving bucket from socket"),
                Error::AlreadyExists => write!(
                    f,
                    "Couldn't create new bucket file. Probably already exists"
                ),
            }
        }
    }

    /// Receive a bucket from socket
    /// BSP(Bucket Send Protocol) format:
    /// rdd_id: u32
    /// partition_id: u32
    /// data_len: u64
    /// data: [u8; data_len]
    /// EOF
    async fn receive_bucket_from_socket(
        worker_id: usize,
        mut socket: TcpStream,
        root_dir: &Path,
    ) -> Result<RddPartitionId, Error> {
        let rdd_id = socket.read_u32().await.map_err(|_| Error::NetworkError)?;
        let partition_id = socket.read_u32().await.map_err(|_| Error::NetworkError)?;
        let data_len = socket.read_u64().await.map_err(|_| Error::NetworkError)?;
        let rdd_dir = root_dir
            .join(format!("worker_{worker_id}"))
            .join(format!("rdd_{rdd_id}"));
        fs::create_dir_all(&rdd_dir)
            .await
            .map_err(|_| Error::FsError)?;
        let partition_path = rdd_dir.join(format!("partition_{partition_id}"));
        if partition_path.exists() {
            return Err(Error::AlreadyExists);
        }
        let mut file = fs::File::create(partition_path)
            .await
            .map_err(|_| Error::FsError)?;
        let bytes_copied = tokio::io::copy(&mut socket, &mut file)
            .await
            .map_err(|_| Error::NetworkError)?;
        if bytes_copied != data_len {
            return Err(Error::NetworkError)?;
        }
        Ok(RddPartitionId {
            rdd_id: RddId(rdd_id as usize),
            partition_id: partition_id as usize,
        })
    }

    pub(crate) async fn receiver_loop(
        worker_id: usize,
        port: usize,
        fs_root: PathBuf,
        master_client: MasterClient<Channel>,
    ) -> Result<(), Error> {
        let addr = format!("0.0.0.0:{port}");
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|_| Error::NetworkError)?;
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                let fs_root = fs_root.clone();
                let mut master_client = master_client.clone();
                tokio::spawn(async move {
                    // TODO: ping master about received bucket
                    match receive_bucket_from_socket(worker_id, socket, &fs_root).await {
                        Ok(part_id) => {
                            let worker_event = WorkerEvent::BucketReceived(worker_id, part_id);
                            let result = TaskResultRequest {
                                serialized_task_result: serde_json::to_vec(&worker_event).unwrap(),
                            };
                            master_client
                                .post_task_result(Request::new(result))
                                .await
                                .unwrap();
                        }
                        Err(e) => match e {
                            Error::FsError => {
                                panic!("File system error occured when receiving bucket")
                            }
                            _ => {}
                        },
                    };
                });
            }
        }
    }
}

pub async fn start(id: usize, port: usize, master_addr: String, fs_root: PathBuf, config: Config) {
    // let cache: Cache = Arc::new(Mutex::new(HashMap::new()));
    // let cachecp = cache.clone();

    let mut master_conn = get_master_client(format!("http://{}", master_addr))
        .await
        .expect("Worker couldn't connect to master");
    {
        let master_conn = master_conn.clone();

        tokio::spawn(async move {
            receiver_loop(id, port, fs_root, master_conn)
                .await
                .expect("Failed to start bucket receiver loop");
        });
    }

    // master may not be running yet
    println!("Worker #{id} starting");
    let mut graph: Graph = Graph::default();
    // Here response.action must be TaskAction::Work
    let mut executor = Executor::new();
    loop {
        let get_task_response = match master_conn
            .get_task(Request::new(GetTaskRequest { id: id as u32 }))
            .await
        {
            Ok(r) => r.into_inner(),
            Err(_) => break,
        };
        let serialized_task = get_task_response.serialized_task;
        let worker_message: WorkerMessage =
            serde_json::from_slice(&serialized_task).expect("bad worker message");

        println!("Worker #{id} got message={worker_message:?}");
        let task = match worker_message {
            WorkerMessage::NewGraph(g) => {
                graph = g;
                continue;
            }
            WorkerMessage::RunTask(task) => {
                let rdd_pid = RddPartitionId {
                    rdd_id: task.final_rdd,
                    partition_id: task.partition_id,
                };
                // TODO: run this in new os thread
                let serialized_buckets = executor.resolve_task(&graph, &task);

                let target_addrs: Vec<String> = task
                    .target_workers
                    .into_iter()
                    .map(|worker_id| config.worker_addrs[worker_id].clone())
                    .collect();

                let bucket_worker_pair =
                    target_addrs.into_iter().zip(serialized_buckets.into_iter());

                bucket_worker_pair.for_each(|(worker_addr, bucket)| {
                    tokio::spawn(async move {
                        send_buckets(bucket, worker_addr).await;
                    });
                });

                // tokio::spawn(async move {
                //     // config.worker_addrs[]
                //     send_buckets(serialized_buckets, String::from_str("tamta")).await;
                // });
            }
            WorkerMessage::Wait => {
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                continue;
            }
            // this is a lie
            WorkerMessage::Shutdown => {
                break;
            }
        };

        // let materialized_rdd_result = graph.get_rdd(task.final_rdd).unwrap().serialize_raw_data(
        //     executor
        //         .cache
        //         .get_as_any(task.final_rdd, task.partition_id)
        //         .unwrap(),
        // );

        // let worker_event = WorkerEvent::Success(task, materialized_rdd_result);
        // let result = TaskResultRequest {
        //     serialized_task_result: serde_json::to_vec(&worker_event).unwrap(),
        // };
        // println!("worker={} got result={:?}", id, result);
        // // cache
        // //     .lock()
        // //     .unwrap()
        // //     .insert(rdd_pid, result.serialized_task_result.clone());
        // master_conn
        //     .post_task_result(Request::new(result))
        //     .await
        //     .unwrap();
    }
    println!("\n\nWoker #{id} shutting down\n\n");
    std::process::exit(0);
}

async fn send_buckets(data: Vec<u8>, worker_addr: String) -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect(worker_addr).await?;

    stream.write_all(&data).await?;

    Ok(())
}
