//! src/workers/lib.rs
use crate::master::{MapTask, ReduceTask};
use uuid::Uuid;

#[derive(Clone, PartialEq, Debug)]
pub struct WorkerId(Uuid);

impl WorkerId {
    pub fn new() -> Self {
        WorkerId(Uuid::new_v4())
    }

    pub fn id(&self) -> Uuid {
        self.0
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct Worker {
    id: WorkerId,
    map_tasks: Vec<MapTask>,
    reduce_tasks: Vec<ReduceTask>,
}

impl Default for Worker {
    fn default() -> Self {
        Self::new()
    }
}

impl Worker {
    pub fn new() -> Self {
        Self {
            id: WorkerId::new(),
            map_tasks: vec![],
            reduce_tasks: vec![],
        }
    }

    pub fn id(&self) -> &WorkerId {
        &self.id
    }

    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }

    pub fn assign_map(&mut self, task: MapTask) -> Result<(), anyhow::Error> {
        self.map_tasks.push(task);
        Ok(())
    }

    pub fn assign_reduce(&mut self, task: ReduceTask) -> Result<(), anyhow::Error> {
        self.reduce_tasks.push(task);
        Ok(())
    }

    pub fn has_task(&self) -> bool {
        !self.map_tasks.is_empty() || !self.reduce_tasks.is_empty()
    }
}
