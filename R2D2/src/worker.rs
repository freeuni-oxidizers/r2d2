use crate::core::cache::ResultCache;
use crate::core::executor::Executor;
use crate::core::graph::Graph;
use crate::core::rdd::{RddId, RddPartitionId};
use crate::core::task_scheduler::{WorkerEvent, WorkerMessage};
use crate::r2d2::master_client::MasterClient;
use crate::r2d2::worker_server::{Worker, WorkerServer};
use crate::r2d2::{GetBucketRequest, GetBucketResponse};
use crate::r2d2::{GetTaskRequest, TaskResultRequest};
use crate::{MASTER_ADDR, ADDR_BASE};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tonic::transport::Channel;
use tonic::{transport::Server, Request, Response, Status};

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

#[derive(Debug)]
pub struct WorkerService {
    _id: u32,
    cache: Mutex<ResultCache>,
}

impl WorkerService {
    fn new(id: u32) -> Self {
        Self {
            _id: id,
            cache: Mutex::new(ResultCache::default()),
        }
    }
}

#[tonic::async_trait]
impl Worker for WorkerService {
    async fn get_bucket(
        &self,
        request: Request<GetBucketRequest>,
    ) -> Result<Response<GetBucketResponse>, Status> {
        let request = request.into_inner();
        let rdd_pid = RddPartitionId {
            rdd_id: RddId(request.rdd_id as usize),
            partition_id: request.partition_id as usize,
        };
        if !self.cache.lock().await.has(rdd_pid) {
            return Err(Status::new(tonic::Code::InvalidArgument, "invalid rddid"));
        }
        Ok(Response::new(GetBucketResponse {}))
    }
}

pub async fn start(id: u32, port: u32) {
    tokio::spawn(async move {
        let worker = WorkerService::new(id);
        let addr = format!("#{ADDR_BASE}:#{port}").parse().unwrap();
        Server::builder()
            .add_service(WorkerServer::new(worker))
            .serve(addr)
            .await
            .expect("Couldn't start worker service");
    });

    // master may not be running yet
    println!("Worker #{id} starting");
    let mut master_conn = get_master_client(format!("http://{}", MASTER_ADDR))
        .await
        .expect("Worker couldn't connect to master");
    let mut graph: Graph = Graph::default();
    // Here response.action must be TaskAction::Work
    let mut executor = Executor::new();
    loop {
        let get_task_response = match master_conn
            .get_task(Request::new(GetTaskRequest { id }))
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
        executor.resolve(
            &graph,
            RddPartitionId {
                rdd_id: task.final_rdd,
                partition_id: task.partition_id,
            },
        );
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
        master_conn
            .post_task_result(Request::new(result))
            .await
            .unwrap();
    }
    println!("\n\nWoker #{id} shutting down\n\n");
    std::process::exit(0);
}
