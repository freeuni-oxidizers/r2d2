use crate::core::executor::Executor;
use crate::core::graph::Graph;
use crate::core::rdd::{RddId, RddPartitionId};
use crate::core::task_scheduler::{WorkerEvent, WorkerMessage};
use crate::r2d2::master_client::MasterClient;
use crate::r2d2::{GetTaskRequest, TaskResultRequest};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
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
type Cache = Arc<Mutex<HashMap<RddPartitionId, Vec<u8>>>>;

async fn handle_connection(mut socket: TcpStream, cache: Cache) {
    tokio::spawn(async move {
        let rdd_id = socket.read_u32().await.expect("") as usize;
        let partition_id = socket.read_u32().await.expect("") as usize;
        let rdd_pid = RddPartitionId {
            rdd_id: RddId(rdd_id),
            partition_id,
        };
        let data = { cache.lock().unwrap().remove(&rdd_pid).unwrap() };
        // TODO: if fail sending return data to cache.
        if socket.write_all(&data).await.is_err() {
            cache.lock().unwrap().insert(rdd_pid, data);
        }
    });
}

pub async fn start(id: usize, port: usize, master_addr: String) {
    let cache: Cache = Arc::new(Mutex::new(HashMap::new()));
    let cachecp = cache.clone();
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{port}");
        let listener = TcpListener::bind(addr).await.expect("Worker couldn't bind");
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                handle_connection(socket, cachecp.clone()).await;
            }
        }
    });

    // master may not be running yet
    println!("Worker #{id} starting");
    let mut master_conn = get_master_client(format!("http://{}", master_addr))
        .await
        .expect("Worker couldn't connect to master");
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
            WorkerMessage::RunTask(task) => task,
            WorkerMessage::Wait => {
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                continue;
            }
            // this is a lie
            WorkerMessage::Shutdown => {
                break;
            }
        };
        let rdd_pid = RddPartitionId {
            rdd_id: task.final_rdd,
            partition_id: task.partition_id,
        };
        executor.resolve(&graph, rdd_pid);

        let materialized_rdd_result = graph.get_rdd(task.final_rdd).unwrap().serialize_raw_data(
            executor
                .cache
                .get_as_any(task.final_rdd, task.partition_id)
                .unwrap(),
        );

        let worker_event = WorkerEvent::Success(task, materialized_rdd_result);
        let result = TaskResultRequest {
            serialized_task_result: serde_json::to_vec(&worker_event).unwrap(),
        };
        println!("worker={} got result={:?}", id, result);
        cache
            .lock()
            .unwrap()
            .insert(rdd_pid, result.serialized_task_result.clone());
        master_conn
            .post_task_result(Request::new(result))
            .await
            .unwrap();
    }
    println!("\n\nWoker #{id} shutting down\n\n");
    std::process::exit(0);
}
