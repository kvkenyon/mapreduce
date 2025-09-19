//! src/master/service.rs
use crate::error::error_chain_fmt;
use crate::worker::{WorkerId, WorkerInfo};
use std::net::SocketAddr;

#[derive(PartialEq, Debug, serde::Serialize, serde::Deserialize)]
pub enum MasterStatus {
    Idle,
    InProgress,
    Complete,
}

#[derive(PartialEq, Debug, serde::Serialize, serde::Deserialize)]
pub enum CallHome {
    WorkerResponse(WorkerId, SocketAddr),
}

impl CallHome {
    pub fn worker_id(&self) -> &WorkerId {
        let CallHome::WorkerResponse(worker_id, _) = self;
        worker_id
    }

    pub fn socket_address(&self) -> &SocketAddr {
        let CallHome::WorkerResponse(_, socket_address) = self;
        socket_address
    }
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize)]
pub enum MasterServiceError {
    #[error("Unexpected error: {0}")]
    UnexpectedError(String),
}

impl From<anyhow::Error> for MasterServiceError {
    fn from(err: anyhow::Error) -> Self {
        MasterServiceError::UnexpectedError(err.to_string())
    }
}

impl std::fmt::Debug for MasterServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(f, self)
    }
}

#[tarpc::service]
pub trait MasterService {
    async fn call_home(call_home: CallHome) -> Result<bool, MasterServiceError>;

    async fn update_task();

    async fn status() -> Result<MasterStatus, MasterServiceError>;

    async fn worker_info() -> Result<Vec<WorkerInfo>, MasterServiceError>;
}
