//! src/worker/service.rs

use crate::master::{MapTask, ReduceTask};
use crate::worker::WorkerId;

#[derive(PartialEq, Debug, serde::Serialize, serde::Deserialize)]
pub enum WorkerStatus {
    Idle(WorkerId),
    InProgress(WorkerId),
    Completed(WorkerId),
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize)]
pub enum WorkerServiceError {
    #[error("Unexpected error: {0}")]
    UnexpectedError(String),
}

impl From<anyhow::Error> for WorkerServiceError {
    fn from(err: anyhow::Error) -> Self {
        WorkerServiceError::UnexpectedError(err.to_string())
    }
}

impl std::fmt::Debug for WorkerServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(f, self)
    }
}

pub fn error_chain_fmt(
    f: &mut std::fmt::Formatter<'_>,
    e: &impl std::error::Error,
) -> std::fmt::Result {
    writeln!(f, "{}\n", e)?;
    let mut current = e.source();
    while let Some(cause) = current {
        writeln!(f, "Caused by:\n\t{}", cause)?;
        current = cause.source();
    }
    Ok(())
}

#[tarpc::service]
pub trait WorkerService {
    async fn ping() -> bool;
    async fn status() -> WorkerStatus;

    async fn assign_map_task(map_task: MapTask) -> Result<bool, WorkerServiceError>;
    async fn assign_reduce_task(reduce_task: ReduceTask) -> Result<bool, WorkerServiceError>;
}
