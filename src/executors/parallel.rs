//! src/executors/parallel.rs
use crate::executors::Executor;

pub struct ParallelExecutor {}

impl Executor for ParallelExecutor {
    fn run(&self) -> Result<(), anyhow::Error> {
        todo!()
    }
}
