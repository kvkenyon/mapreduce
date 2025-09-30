//! src/worker/service.rs

use crate::error::error_chain_fmt;
use crate::master::{MapTask, ReduceTask};
use crate::worker::WorkerId;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

#[derive(PartialEq, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WorkerStatus {
    Idle(WorkerId),
    InProgress(WorkerId),
    Completed(WorkerId),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerInfo {
    worker_id: WorkerId,
    worker_status: WorkerStatus,
    socket_addr: SocketAddr,
    map_tasks: Vec<MapTask>,
    reduce_tasks: Vec<ReduceTask>,
}

impl WorkerInfo {
    pub fn new(
        worker_id: WorkerId,
        socket_addr: SocketAddr,
        worker_status: WorkerStatus,
        map_tasks: Vec<MapTask>,
        reduce_tasks: Vec<ReduceTask>,
    ) -> Self {
        Self {
            worker_id,
            socket_addr,
            map_tasks,
            reduce_tasks,
            worker_status,
        }
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }

    pub fn map_tasks(&self) -> &Vec<MapTask> {
        &self.map_tasks
    }

    pub fn reduce_tasks(&self) -> &Vec<ReduceTask> {
        &self.reduce_tasks
    }

    pub fn worker_status(&self) -> &WorkerStatus {
        &self.worker_status
    }
}

impl Display for WorkerInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f)?;
        writeln!(f, "-----------------")?;
        writeln!(f, "Worker Id: {}", self.worker_id)?;
        writeln!(f, "Status: {:?}", self.worker_status)?;
        writeln!(f, "Socket Address: {}", self.socket_addr)?;
        if !self.map_tasks.is_empty() {
            writeln!(f, "Map Tasks:")?;
            for map_task in self.map_tasks.iter() {
                writeln!(f, "    Task Id: {}", map_task.task_id)?;
                writeln!(f, "    Task State: {:?}", map_task.state)?;
            }
        }
        if !self.reduce_tasks.is_empty() {
            writeln!(f, "Reduce Tasks:")?;
            for reduce_task in self.reduce_tasks.iter() {
                writeln!(f, "    Task Id: {}", reduce_task.task_id)?;
                writeln!(f, "    Task State: {:?}", reduce_task.state)?;
            }
        }
        writeln!(f, "-----------------")
    }
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(f, self)
    }
}

#[tarpc::service]
pub trait WorkerService {
    async fn status() -> Result<WorkerStatus, WorkerServiceError>;

    async fn assign_map_task(map_task: MapTask) -> Result<bool, WorkerServiceError>;

    async fn assign_reduce_task(reduce_task: ReduceTask) -> Result<bool, WorkerServiceError>;

    async fn start_tasks() -> Result<(), WorkerServiceError>;

    async fn worker_info() -> Result<WorkerInfo, WorkerServiceError>;
}
