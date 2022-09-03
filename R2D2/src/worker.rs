use crate::core::executor::Executor;
use crate::core::graph::Graph;
use crate::core::rdd::RddPartitionId;
use crate::core::task_scheduler::{BucketReceivedEvent, TaskKind, WorkerEvent, WorkerMessage};
use crate::r2d2::master_client::MasterClient;
use crate::r2d2::{GetTaskRequest, TaskResultRequest};
use crate::worker::bucket_receiver::receiver_loop;
use crate::Config;
use std::error::Error;
use std::path::PathBuf;
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

pub async fn send_worker_event(
    master_client: &mut MasterClient<Channel>,
    worker_event: WorkerEvent,
) {
    let result = TaskResultRequest {
        serialized_task_result: serde_json::to_vec(&worker_event).unwrap(),
    };
    master_client
        .post_task_result(Request::new(result))
        .await
        .unwrap();
}

pub(crate) mod bucket_receiver {
    use core::fmt;
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
    };
    use tonic::transport::Channel;

    use crate::{
        core::{
            rdd::{RddId, RddPartitionId},
            task_scheduler::{BucketReceivedEvent, WorkerEvent},
        },
        r2d2::master_client::MasterClient,
    };

    use super::send_worker_event;

    #[derive(Debug)]
    #[allow(dead_code)]
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
    /// wide_rdd_id: u32
    /// wide_partition_id: u32
    /// narrow_partition_id: u32
    /// data_len: u64
    /// data: [u8; data_len]
    /// EOF
    /// TODO: put in cache, notify master(maybe written, check)
    async fn receive_bucket_from_socket(
        _worker_id: usize,
        mut socket: TcpStream,
        _root_dir: &Path,
        received_buckets: Arc<std::sync::Mutex<HashMap<(RddPartitionId, usize), Vec<u8>>>>,
    ) -> Result<(RddPartitionId, usize), Error> {
        let wide_rdd_id = socket.read_u32().await.map_err(|_| Error::NetworkError)?;
        let wide_partition_id = socket.read_u32().await.map_err(|_| Error::NetworkError)?;
        let narrow_partition_id = socket.read_u32().await.map_err(|_| Error::NetworkError)?;
        let data_len = socket.read_u64().await.map_err(|_| Error::NetworkError)?;
        let mut buf = vec![0_u8; data_len as usize];
        socket
            .read_exact(&mut buf[..])
            .await
            .map_err(|_| Error::NetworkError)?;
        let mut buckets = { received_buckets.lock().unwrap() };
        let rpid = RddPartitionId {
            rdd_id: RddId(wide_rdd_id as usize),
            partition_id: wide_partition_id as usize,
        };
        buckets.insert((rpid, narrow_partition_id as usize), buf);
        Ok((rpid, narrow_partition_id as usize))
    }

    pub(crate) async fn receiver_loop(
        worker_id: usize,
        port: usize,
        fs_root: PathBuf,
        master_client: MasterClient<Channel>,
        received_buckets: Arc<std::sync::Mutex<HashMap<(RddPartitionId, usize), Vec<u8>>>>,
    ) -> Result<(), Error> {
        let addr = format!("0.0.0.0:{port}");
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|_| Error::NetworkError)?;
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                let fs_root = fs_root.clone();
                let mut master_client = master_client.clone();
                let buckets = received_buckets.clone();
                tokio::spawn(async move {
                    // TODO: ping master about received bucket
                    match receive_bucket_from_socket(worker_id, socket, &fs_root, buckets).await {
                        Ok((rpid, narrow_partition_id)) => {
                            let worker_event = WorkerEvent::BucketReceived(BucketReceivedEvent {
                                worker_id,
                                wide_partition: rpid,
                                narrow_partition_id,
                            });
                            send_worker_event(&mut master_client, worker_event).await;
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

    let mut executor = Executor::new();

    {
        let cache = executor.received_buckets.clone();

        let master_conn = master_conn.clone();

        tokio::spawn(async move {
            receiver_loop(id, port, fs_root, master_conn, cache)
                .await
                .expect("Failed to start bucket receiver loop");
        });
    }

    // master may not be running yet
    println!("Worker #{id} starting");
    let mut graph: Graph = Graph::default();

    // Here response.action must be TaskAction::Work
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
        match worker_message {
            WorkerMessage::NewGraph(g) => {
                graph = g;
                // ping graph receive event
                send_worker_event(&mut master_conn, WorkerEvent::GraphReceived(id)).await;
                continue;
            }
            WorkerMessage::RunTask(task) => {
                match task.kind {
                    TaskKind::ResultTask(ref result_task) => {
                        let rdd_pid = result_task.rdd_partition_id;
                        executor.resolve(&graph, rdd_pid);

                        let materialized_rdd_result = graph
                            .get_rdd(result_task.rdd_partition_id.rdd_id)
                            .unwrap()
                            .serialize_raw_data(
                                executor
                                    .cache
                                    .take_as_any(rdd_pid.rdd_id, rdd_pid.partition_id)
                                    .unwrap(),
                            );

                        let worker_event =
                            WorkerEvent::Success(task.clone(), materialized_rdd_result);
                        send_worker_event(&mut master_conn, worker_event).await;
                    }
                    TaskKind::WideTask(wide_task) => {
                        // TODO: run this in new os thread
                        let serialized_buckets = executor.resolve_task(&graph, &wide_task);

                        let target_addrs: Vec<_> = wide_task
                            .target_workers
                            .into_iter()
                            .map(|worker_id| (worker_id, config.worker_addrs[worker_id].clone()))
                            .collect();

                        let bucket_worker_pair =
                            target_addrs.into_iter().zip(serialized_buckets.into_iter());

                        // error: locking to loong `for_each`
                        bucket_worker_pair.enumerate().for_each(
                            |(wide_partition_id, ((worker_id, worker_addr), bucket))| {
                                if id == worker_id {
                                    let mut received_buckets =
                                        { executor.received_buckets.lock().unwrap() };
                                    let rpid = RddPartitionId {
                                        rdd_id: wide_task.wide_rdd_id,
                                        partition_id: wide_partition_id,
                                    };

                                    received_buckets
                                        .insert((rpid, wide_task.narrow_partition_id), bucket);

                                    let worker_event =
                                        WorkerEvent::BucketReceived(BucketReceivedEvent {
                                            worker_id,
                                            wide_partition: rpid,
                                            narrow_partition_id: wide_task.narrow_partition_id,
                                        });

                                    let mut master_conn = master_conn.clone();
                                    // ping master about received bucket
                                    tokio::spawn(async move {
                                        send_worker_event(&mut master_conn, worker_event).await
                                    });
                                } else {
                                    tokio::spawn(async move {
                                        send_buckets(
                                            bucket,
                                            worker_addr,
                                            wide_task.wide_rdd_id.0,
                                            wide_partition_id,
                                            wide_task.narrow_partition_id,
                                        )
                                        .await
                                        .expect("Couldn't send bucket");
                                    });
                                }
                            },
                        );
                    }
                }
            }
            WorkerMessage::Wait => {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                continue;
            }
            // this is a lie
            WorkerMessage::Shutdown => {
                break;
            }
        };
    }
    println!("\n\nWoker #{id} shutting down\n\n");
    std::process::exit(0);
}

/// let rdd_id = socket.read_u32().await.map_err(|_| Error::NetworkError)?;
// let partition_id = socket.read_u32().await.map_err(|_| Error::NetworkError)?;
// let data_len = socket.read_u64().await.map_err(|_| Error::NetworkError)?;
// TODO: if worker_id == self_id -> dont send, just update cache
/// Receive a bucket from socket
/// BSP(Bucket Send Protocol) format:
/// wide_rdd_id: u32
/// wide_partition_id: u32
/// narrow_partition_id: u32
/// data_len: u64
/// data: [u8; data_len]
/// done: send according to receive protocol ^
async fn send_buckets(
    data: Vec<u8>,
    worker_addr: String,
    wide_rdd_id: usize,
    wide_partition_id: usize,
    narrow_partition_id: usize,
) -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect(worker_addr).await?;

    stream.write_u32(wide_rdd_id as u32).await?;
    stream.write_u32(wide_partition_id as u32).await?;
    stream.write_u32(narrow_partition_id as u32).await?;
    stream.write_u64(data.len() as u64).await?;
    stream.write_all(&data).await?;

    Ok(())
}
