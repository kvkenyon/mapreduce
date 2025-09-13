//! src/master/lib.rs
use crate::{mapreduce::InputSplit, spec::MapReduceOutput, worker::WorkerId};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub enum TaskState {
    Idle,
    InProgress,
    Completed,
}

#[derive(Clone, Debug)]
pub struct MapTask {
    pub task_id: Uuid,
    pub state: TaskState,
    pub worker_id: Option<WorkerId>,
    pub input_split: InputSplit,
}

#[derive(Clone, Debug)]
pub struct ReduceTask {
    pub task_id: Uuid,
    pub state: TaskState,
    pub worker_id: Option<WorkerId>,
    pub output: MapReduceOutput,
    pub input_location: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Master {}

#[allow(unused)]
impl Master {
    pub fn new() -> Self {
        Master {}
    }
}

impl Default for Master {
    fn default() -> Self {
        Self::new()
    }
}
