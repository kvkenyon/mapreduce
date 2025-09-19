//! src/worker/executor.rs
use crate::master::MapTask;

pub struct MapExecutor {
    bucket_name: String,
    task: MapTask,
    r: usize,
}

impl MapExecutor {
    pub fn new(bucket_name: &str, task: &MapTask, r: usize) -> Self {
        Self {
            bucket_name: bucket_name.to_string(),
            task: task.clone(),
            r,
        }
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        let mapper = self.task.input_split.mapper();

        Ok(())
    }
}
