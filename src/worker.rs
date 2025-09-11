//! src/worker.rs
use crate::{
    master::{Task, TaskState},
    registry,
};
use anyhow::Context;
use std::fs::read_to_string;
use uuid::Uuid;

#[derive(Clone)]
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

#[derive(Clone)]
pub struct Worker {
    id: WorkerId,
    tasks: Vec<Task>,
}

impl Default for Worker {
    fn default() -> Self {
        Self::new()
    }
}

impl Worker {
    pub fn new() -> Self {
        Worker {
            id: WorkerId::new(),
            tasks: vec![],
        }
    }

    pub fn id(&self) -> &WorkerId {
        &self.id
    }

    pub fn assign_task(&mut self, task: Task) {
        self.tasks.push(task);
    }

    pub fn has_task(&self) -> bool {
        !self.tasks.is_empty()
    }

    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        for task in self.tasks.iter_mut() {
            //update state to InProgress
            task.state = TaskState::InProgress;

            // readinput
            let input = read_to_string(task.input_split.location())
                .context("Failed to read input split")?;
            // execute task
            let mapper = task.input_split.function();
            let mapper = registry::get_mapper(mapper).map_err(|e| anyhow::anyhow!(e))?;
            mapper.map("".to_string(), input);
            // Update state to Completed
            task.state = TaskState::Completed;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
