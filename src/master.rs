//! src/master.rs
use std::collections::{hash_map::HashMap, hash_map::Values};

use uuid::Uuid;

use crate::{
    mapreduce::InputSplit,
    worker::{Worker, WorkerId},
};

#[derive(Clone)]
pub enum TaskState {
    Idle,
    InProgress,
    Completed,
}

#[derive(Clone)]
pub struct Task {
    pub state: TaskState,
    pub worker_id: Option<WorkerId>,
    pub input_split: InputSplit,
}

pub struct Master {
    workers: Vec<Worker>,
    tasks: Vec<Task>,
    input_splits: HashMap<Uuid, InputSplit>,
}

impl Master {
    pub fn new(workers: Vec<Worker>, input_splits: HashMap<Uuid, InputSplit>) -> Self {
        let mut tasks = vec![];
        for input_split in input_splits.values() {
            let task = Task {
                state: TaskState::Idle,
                worker_id: None,
                input_split: input_split.clone(),
            };
            tasks.push(task);
        }
        let mut master = Master {
            workers,
            tasks,
            input_splits,
        };

        master.assign_tasks();
        master
    }

    fn assign_tasks(&mut self) {
        let count = self.worker_count();
        for (curr_worker, i) in (0..self.task_count()).enumerate() {
            let task = self.tasks.get(i).unwrap();
            let worker = self.workers.get_mut(curr_worker % count).unwrap();
            worker.assign_task(task.clone());
        }
    }

    pub fn workers(&self) -> &Vec<Worker> {
        &self.workers
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn input_splits(&self) -> Values<'_, Uuid, InputSplit> {
        self.input_splits.values()
    }

    pub fn tasks(&self) -> &Vec<Task> {
        &self.tasks
    }

    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        let num_workers = self.worker_count();
        for i in 0..num_workers {
            let worker = self.workers.get_mut(i).unwrap();
            worker.run()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
