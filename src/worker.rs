//! src/worker.rs
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

#[cfg(test)]
mod tests {
    use crate::test_utils::make_mr_job;
    use std::path::PathBuf;

    #[test]
    fn map_workers_emit_to_partitioned_files() {
        let mut mr_job = make_mr_job();
        let worker_id = mr_job.master_mut().workers().first().unwrap().id();
        let worker_id = worker_id.clone();
        let worker = mr_job
            .master_mut()
            .get_worker_from_id_mut(worker_id)
            .unwrap();
        worker.run().expect("Failed to run worker");
        let path = PathBuf::from("/tmp/mapreduce/output/map");
        let dir_entries = std::fs::read_dir(&path).expect("Failed to read dir");
        assert_eq!(dir_entries.count(), 2);
        std::fs::remove_dir_all(path).expect("Failed to delete test dir.");
    }
}
