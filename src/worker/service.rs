//! src/worker/service.rs
use crate::worker::WorkerId;

#[derive(PartialEq, Debug, serde::Serialize, serde::Deserialize)]
pub enum WorkerStatus {
    Idle(WorkerId),
    InProgress(WorkerId),
    Completed(WorkerId),
}
#[tarpc::service]
pub trait WorkerService {
    async fn ping() -> bool;
    async fn status() -> WorkerStatus;
}
