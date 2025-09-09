//! src/executors/mod.rs
pub trait Executor {
    fn run(&self) -> Result<(), anyhow::Error>;
}

mod parallel;
pub use parallel::ParallelExecutor;

mod sequential;
pub use sequential::SequentialExecutor;
