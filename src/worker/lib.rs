//! src/workers/lib.rs
use crate::{
    master::{MapTask, ReduceTask, TaskState},
    registry,
};
use anyhow::Context;
use std::fs::read_to_string;
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
        for task in self.map_tasks.iter_mut() {
            //update state to InProgress
            task.state = TaskState::InProgress;

            // readinput
            let input = read_to_string(task.input_split.location())
                .context("Failed to read input split")?;
            // execute task
            let mapper = task.input_split.function();
            let mut mapper = registry::get_mapper(mapper).map_err(|e| anyhow::anyhow!(e))?;
            mapper.map("".to_string(), input);
            // Update state to Completed
            task.state = TaskState::Completed;
        }
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
