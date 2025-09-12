//! src/master/lib.rs
use crate::{
    mapreduce::InputSplit,
    spec::MapReduceOutput,
    worker::{Worker, WorkerId},
};
use std::collections::hash_map::{HashMap, Values};
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

#[allow(unused)]
pub struct Master {
    workers: Vec<Worker>,
    map_tasks: Vec<MapTask>,
    reduce_tasks: Vec<ReduceTask>,
    input_splits: HashMap<Uuid, InputSplit>,
    reduce_output: MapReduceOutput,
}

#[allow(unused)]
impl Master {
    pub fn new(
        workers: Vec<Worker>,
        input_splits: HashMap<Uuid, InputSplit>,
        reduce_output: MapReduceOutput,
    ) -> Self {
        let mut map_tasks = vec![];
        for input_split in input_splits.values() {
            let task = MapTask {
                task_id: Uuid::new_v4(),
                state: TaskState::Idle,
                worker_id: None,
                input_split: input_split.clone(),
            };
            map_tasks.push(task);
        }

        let mut reduce_tasks = vec![];
        for _ in 0..reduce_output.num_tasks() {
            let task = ReduceTask {
                task_id: Uuid::new_v4(),
                state: TaskState::Idle,
                worker_id: None,
                output: reduce_output.clone(),
                input_location: None,
            };

            reduce_tasks.push(task);
        }

        let mut master = Master {
            workers,
            map_tasks,
            reduce_tasks,
            input_splits,
            reduce_output,
        };

        master
            .assign_tasks()
            .expect("Failed to assign tasks. Terminating...");

        master
    }

    fn get_map_task(&mut self, task_id: Uuid) -> Option<&mut MapTask> {
        self.map_tasks
            .iter_mut()
            .find(|task| task.task_id == task_id)
    }

    fn get_reduce_task(&mut self, task_id: Uuid) -> Option<&mut ReduceTask> {
        self.reduce_tasks
            .iter_mut()
            .find(|task| task.task_id == task_id)
    }

    fn get_worker_from_id(&self, worker_id: WorkerId) -> Option<&Worker> {
        self.workers.iter().find(|w| *w.id() == worker_id)
    }

    pub fn get_worker_from_id_mut(&mut self, worker_id: WorkerId) -> Option<&mut Worker> {
        self.workers.iter_mut().find(|w| *w.id() == worker_id)
    }

    fn assign_tasks(&mut self) -> Result<(), anyhow::Error> {
        let num_workers = self.worker_count();
        for (curr_worker, task) in self.map_tasks.iter_mut().enumerate() {
            let worker = self.workers.get_mut(curr_worker % num_workers).unwrap();
            task.worker_id = Some(worker.id().clone());
            task.state = TaskState::InProgress;
            worker.assign_map(task.clone())?;
        }

        for (curr_worker, task) in self.reduce_tasks.iter_mut().enumerate() {
            let worker = self.workers.get_mut(curr_worker % num_workers).unwrap();
            task.worker_id = Some(worker.id().clone());
            task.state = TaskState::InProgress;
            worker.assign_reduce(task.clone())?;
        }

        Ok(())
    }

    pub fn workers(&self) -> &Vec<Worker> {
        &self.workers
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn task_count(&self) -> usize {
        self.map_tasks.len() + self.reduce_tasks.len()
    }

    pub fn input_splits(&self) -> Values<'_, Uuid, InputSplit> {
        self.input_splits.values()
    }

    pub fn map_tasks(&self) -> &Vec<MapTask> {
        &self.map_tasks
    }

    pub fn reduce_tasks(&self) -> &Vec<ReduceTask> {
        &self.reduce_tasks
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
mod tests {
    use crate::master::TaskState;
    use crate::test_utils::make_mr_job;
    use claims::assert_some;

    #[test]
    fn master_should_assign_all_tasks_to_available_workers() {
        let mr_job = make_mr_job();
        let num_map_tasks = mr_job.master().map_tasks.len();
        for i in 0..num_map_tasks {
            let task = mr_job.master().map_tasks.get(i).unwrap();
            assert_some!(&task.worker_id);
        }

        let num_reduce_tasks = mr_job.master().reduce_tasks.len();
        for i in 0..num_reduce_tasks {
            let task = mr_job.master().reduce_tasks.get(i).unwrap();
            assert_some!(&task.worker_id);
        }
    }

    #[test]
    fn master_should_mark_all_assigned_tasks_as_in_progress() {
        let mr_job = make_mr_job();
        let num_map_tasks = mr_job.master().map_tasks.len();
        for i in 0..num_map_tasks {
            let task = mr_job.master().map_tasks.get(i).unwrap();
            assert_eq!(task.state, TaskState::InProgress);
        }

        let num_reduce_tasks = mr_job.master().reduce_tasks.len();
        for i in 0..num_reduce_tasks {
            let task = mr_job.master().reduce_tasks.get(i).unwrap();
            assert_eq!(task.state, TaskState::InProgress);
        }
    }
    // #[test]
    // fn master_should_be_able_to_send_messages_to_workers() {
    //     todo!()
    // }
    //
    // #[test]
    // fn master_should_be_able_to_receive_messages_from_workers() {
    //     todo!()
    // }
    //
    // #[test]
    // fn master_should_send_heartbeats_to_workers_periodically() {
    //     todo!()
    // }
    //
    // #[test]
    // fn master_should_reassign_work_for_workers_that_dont_respond_to_heartbeat_before_timeout() {
    //     todo!()
    // }
    //
    // #[test]
    // fn master_should_forward_completed_map_task_intermediate_file_location_to_reduce_workers() {
    //     todo!()
    // }
}
