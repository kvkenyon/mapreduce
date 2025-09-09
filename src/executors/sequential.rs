//! src/executors/sequential.rs
use crate::executors::Executor;

pub struct SequentialExecutor {}

impl Executor for SequentialExecutor {
    fn run(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
