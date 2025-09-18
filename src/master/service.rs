//! src/master/service.rs
use crate::master::{MapTask, ReduceTask};
use crate::worker::WorkerId;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

#[derive(PartialEq, Debug, serde::Serialize, serde::Deserialize)]
pub enum MasterStatus {
    Idle,
    InProgress,
    Complete,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerInfo {
    worker_id: WorkerId,
    socket_addr: SocketAddr,
    map_tasks: Vec<MapTask>,
    reduce_tasks: Vec<ReduceTask>,
}

impl WorkerInfo {
    pub fn new(
        worker_id: WorkerId,
        socket_addr: SocketAddr,
        map_tasks: Vec<MapTask>,
        reduce_tasks: Vec<ReduceTask>,
    ) -> Self {
        Self {
            worker_id,
            socket_addr,
            map_tasks,
            reduce_tasks,
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
}

impl Display for WorkerInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "-------------------------------------------")?;
        writeln!(f, "Worker Id: {}", self.worker_id)?;
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
        writeln!(f, "-------------------------------------------")
    }
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

#[tarpc::service]
pub trait MasterService {
    async fn call_home(call_home: CallHome) -> bool;

    async fn update_task();

    async fn status() -> MasterStatus;

    async fn worker_info() -> Vec<WorkerInfo>;
}
