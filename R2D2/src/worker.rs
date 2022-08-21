use crate::r2d2::master_client::MasterClient;
use crate::r2d2::{GetTaskRequest, Task, TaskAction, TaskFinishedRequest};
use crate::rdd::{Context, RddIndex, SparkContext, RddBase, RddId};
use crate::MASTER_ADDR;
use tokio::time::{sleep, Duration};
use tonic::Request;

impl TryFrom<i32> for TaskAction {
    type Error = i32;

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == TaskAction::Wait as i32 => Ok(TaskAction::Wait),
            x if x == TaskAction::Shutdown as i32 => Ok(TaskAction::Shutdown),
            x if x == TaskAction::Work as i32 => Ok(TaskAction::Work),
            _ => Err(v),
        }
    }
}

/// TODO: refactor start?
/// `start` DOES NOT RETURN
///  PROCESS EXITS
pub async fn start(id: u32) {
    // master may not be running yet
    // println!("worker {} starting", id);
    let mut retries = 3;
    let mut wait = 100;
    let master_conn = loop {
        let master_addr = format!("http://{}", MASTER_ADDR);
        match MasterClient::connect(master_addr).await {
            Err(_) if retries > 0 => {
                retries -= 1;
                sleep(Duration::from_millis(wait)).await;
                wait *= 2;
            }
            master_conn => break master_conn,
        }
    };
    let master_conn = &mut master_conn.expect("Error: worker couldn't connect to master");
    loop {
        let response: Task = master_conn
            .get_task(Request::new(GetTaskRequest { id }))
            .await
            .expect("Error: worker didn't get rpc response from master")
            .into_inner();
        // println!("worker={} got task={:?}", id, response);
        match response.action.try_into() {
            Ok(TaskAction::Shutdown) => {
                break;
            }
            Ok(TaskAction::Wait) => {
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                continue;
            }
            Ok(TaskAction::Work) => {}
            Err(e) => {
                panic!("Worker got weird `TaskAction`: {:?}", e);
            }
        }
        // here response.action must be TaskAction::Work
        let mut sc: SparkContext = serde_json::from_str(&response.context).unwrap();
        // TODO: how does worker know it's i32, convert to T
        // RddId -> resolve -> serialize from raw 
        let rdd: RddIndex<i32> = serde_json::from_str(&response.rdd)
            .expect("Error: couldn't parse RddIndex from master");
        let result = sc.collect(rdd);
        let result = TaskFinishedRequest {
            result: serde_json::to_string_pretty(result).unwrap(),
            id,
        };
        // println!("worker={} got result={:?}", id, result);
        let response = master_conn
            .task_finished(Request::new(result))
            .await
            .unwrap()
            .into_inner();
        if response.shutdown {
            break;
        }
    }
    println!("\n\nwoker={} shutting down\n\n", id);
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
