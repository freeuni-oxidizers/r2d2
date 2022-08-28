use crate::r2d2::master_client::MasterClient;
use crate::r2d2::{GetTaskRequest, Task, TaskAction, TaskFinishedRequest};
use crate::rdd::executor::Executor;
use crate::rdd::graph::Graph;
use crate::rdd::rdd::{RddId, RddPartitionId};
use crate::MASTER_ADDR;
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

/// TODO: refactor start?
/// `start` DOES NOT RETURN
///  PROCESS EXITS
pub async fn start(id: u32) {
    // master may not be running yet
    println!("Worker #{id} starting");
    let mut master_conn = get_master_client(format!("http://{}", MASTER_ADDR))
        .await
        .expect("Worker couldn't connect to master");
    loop {
        let task: Task = master_conn
            .get_task(Request::new(GetTaskRequest { id }))
            .await
            .expect("Worker didn't get rpc response from master")
            .into_inner();
        println!("Worker #{id} got task={task:?}");
        match TaskAction::from_i32(task.action) {
            Some(TaskAction::Shutdown) => {
                break;
            }
            Some(TaskAction::Wait) => {
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                continue;
            }
            Some(TaskAction::Work) => {}
            None => {
                panic!("Worker got bad `TaskAction`: {:?}", task.action);
            }
        }
        // Here response.action must be TaskAction::Work
        let graph: Graph = serde_json::from_str(&task.graph).unwrap();
        let target: RddId = serde_json::from_str(&task.rdd).unwrap();
        let mut executor = Executor::new();
        executor.resolve(&graph, RddPartitionId {
            rdd_id: target,
            partition_id: task.partition_id as usize,
        });
        let result = graph.get_rdd(target).unwrap().serialize_raw_data(
            executor
                .cache
                .get_as_any(target, task.partition_id as usize)
                .unwrap(),
        );

        let result = TaskFinishedRequest { result, id };
        println!("worker={} got result={:?}", id, result);
        let response = master_conn
            .task_finished(Request::new(result))
            .await
            .unwrap()
            .into_inner();

        if response.shutdown {
            break;
        }
    }
    println!("\n\nWoker #{id} shutting down\n\n");
    std::process::exit(0);
}

// pub async fn task_finished() -> Result<(), Box<dyn std::error::Error>> {
//     let master_addr = format!("http://{}", MASTER_ADDR);
//     let response = MasterClient::connect(master_addr)
//         .await?
//         .task_finished(Request::new(Empty {}))
//         .await?
//         .into_inner();
//     if !response.terminate {
//         unimplemented!();
//     }
//     Ok(())
// }
