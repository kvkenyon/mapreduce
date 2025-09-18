//! src/master/lib.rs

use crate::configuration::Settings;
use crate::master::{MasterService, MasterStatus};
use crate::spec::MapReduceSpecification;
use crate::worker::WorkerServiceClient;
use crate::{mapreduce::InputSplit, spec::MapReduceOutput, worker::WorkerId};
use anyhow::Context;
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tarpc::{
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
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
pub struct Master {
    spec: MapReduceSpecification,
    input_splits: HashMap<String, Vec<InputSplit>>,
    number_of_map_tasks: usize,
    number_of_reduce_tasks: usize,
    map_tasks: HashMap<Uuid, MapTask>,
    reduce_tasks: HashMap<Uuid, ReduceTask>,
    worker_service_clients: Vec<WorkerServiceClient>,
}

#[allow(unused)]
impl Master {
    pub fn new(
        spec: MapReduceSpecification,
        input_splits: HashMap<String, Vec<InputSplit>>,
        worker_service_clients: Vec<WorkerServiceClient>,
    ) -> Self {
        let number_of_reduce_tasks = spec
            .output()
            .expect("No map reduce output defined. Job failing.")
            .num_tasks() as usize;

        let mut number_of_map_tasks = 0;
        let mut map_tasks = HashMap::new();
        for (_, input_splits) in input_splits.clone() {
            for input_split in input_splits {
                let task_id = Uuid::new_v4();
                let task = MapTask {
                    task_id,
                    state: TaskState::Idle,
                    worker_id: None,
                    input_split,
                };
                map_tasks.insert(task_id, task);
                number_of_map_tasks += 1;
            }
        }

        let mut reduce_tasks = HashMap::new();
        for i in 0..number_of_reduce_tasks {
            let task_id = Uuid::new_v4();
            let task = ReduceTask {
                task_id,
                state: TaskState::Idle,
                worker_id: None,
                output: spec.output().unwrap(),
                input_location: None,
            };
            reduce_tasks.insert(task_id, task);
        }

        Master {
            spec,
            input_splits,
            number_of_map_tasks,
            number_of_reduce_tasks,
            map_tasks,
            reduce_tasks,
            worker_service_clients,
        }
    }

    pub fn map_tasks(&self) -> &HashMap<Uuid, MapTask> {
        &self.map_tasks
    }
    pub fn reduce_tasks(&self) -> &HashMap<Uuid, ReduceTask> {
        &self.reduce_tasks
    }

    pub fn number_of_reduce_tasks(&self) -> usize {
        self.number_of_reduce_tasks
    }

    pub fn number_of_map_tasks(&self) -> usize {
        self.number_of_map_tasks
    }

    pub fn spec(&self) -> &MapReduceSpecification {
        &self.spec
    }

    pub fn input_splits(&self) -> &HashMap<String, Vec<InputSplit>> {
        &self.input_splits
    }
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct MasterServer {
    master: Master,
    host: String,
    port: u16,
}

impl MasterServer {
    pub async fn build(
        config: Settings,
        spec: MapReduceSpecification,
        input_splits: HashMap<String, Vec<InputSplit>>,
        worker_service_clients: Vec<WorkerServiceClient>,
    ) -> anyhow::Result<Self> {
        let master_server = MasterServer {
            host: config.rpc.host,
            port: config.rpc.port,
            master: Master::new(spec, input_splits, worker_service_clients),
        };
        Ok(master_server)
    }

    #[tracing::instrument("MasterServer start", skip_all)]
    pub async fn start(
        &self,
        shutdown_tx: &tokio::sync::broadcast::Sender<()>,
    ) -> anyhow::Result<(SocketAddr, JoinHandle<anyhow::Result<()>>)> {
        let (addr_tx, addr_rx) = oneshot::channel::<SocketAddr>();

        let socket_addr = self.get_addr().context("Failed to get address")?;

        let mut shutdown_rx = shutdown_tx.subscribe();
        let server_clone = self.clone();
        let handle = tokio::spawn(async move {
            tokio::select! {
                result = Self::run_until_stopped(&socket_addr, addr_tx, server_clone) => {
                    result
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!("Master shutting down");
                   Ok(())
                }
            }
        });
        tracing::info!("waiting for bound socket addr.");
        let socket_addr = addr_rx.await.context("Failed to receive master address")?;
        Ok((socket_addr, handle))
    }

    #[tracing::instrument("Run worker until stopped", skip_all)]
    async fn run_until_stopped(
        server_addr: &SocketAddr,
        addr_tx: oneshot::Sender<SocketAddr>,
        master_server: MasterServer,
    ) -> anyhow::Result<()> {
        let mut listener = tarpc::serde_transport::tcp::listen(server_addr, Json::default).await?;
        listener.config_mut().max_frame_length(usize::MAX);
        let socket_addr = listener.local_addr();
        let _ = addr_tx.send(socket_addr);
        listener
            // Ignore accept errors.
            .filter_map(|r| future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            // Limit channels to 1 per IP.
            .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
            // serve is generated by the service attribute. It takes as input any type implementing
            // the generated World trait.
            .map(|channel| {
                channel
                    .execute(master_server.clone().serve())
                    .for_each(spawn)
            })
            // Max 10 channels.
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;
        Ok(())
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn get_addr(&self) -> anyhow::Result<SocketAddr> {
        let addr: Ipv4Addr = self.host().parse().context("Failed to parse host")?;
        Ok(SocketAddr::new(IpAddr::V4(addr), self.port))
    }

    pub fn master(&self) -> &Master {
        &self.master
    }
}

impl MasterService for MasterServer {
    async fn call_home(self, _context: tarpc::context::Context) -> bool {
        true
    }

    async fn update_task(self, _context: tarpc::context::Context) {}

    async fn status(self, _context: tarpc::context::Context) -> MasterStatus {
        MasterStatus::Idle
    }
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[cfg(test)]
mod tests {
    use super::Master;
    use crate::mapreduce::InputSplit;
    use crate::master::TaskState;
    use crate::spec::{
        MapReduceInput, MapReduceInputFormat, MapReduceOutput, MapReduceOutputFormat,
        MapReduceSpecification,
    };
    use claims::{assert_none, assert_some};
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn should_create_idle_unassigned_map_tasks_for_each_input_split_and_reduce_tasks_from_spec() {
        let master = setup_master();
        assert_eq!(100, master.number_of_map_tasks);
        assert_eq!(100, master.map_tasks.len());

        let mut input_split_keys = vec![];
        for i in 0..100 {
            let key = format!("mr_input_0_{i}_of_99");
            input_split_keys.push(key);
        }

        for (tid, task) in master.map_tasks().iter() {
            assert_eq!(*tid, task.task_id);
            assert_none!(&task.worker_id);
            assert_eq!(TaskState::Idle, task.state);
            let idx_of_split = input_split_keys
                .iter()
                .position(|key| key == task.input_split.key());
            assert_some!(idx_of_split);
            input_split_keys.remove(idx_of_split.unwrap());
        }

        assert!(input_split_keys.is_empty());

        for (tid, task) in master.reduce_tasks() {
            assert_eq!(*tid, task.task_id);
            assert_none!(&task.worker_id);
            assert_eq!(TaskState::Idle, task.state);
            assert_eq!(task.output.reducer(), "Adder");
            assert_eq!(task.output.base_path(), "/tmp/mapreduce/out");
            assert_none!(&task.input_location);
        }
    }

    #[test]
    fn master_server_should_have_a_vector_of_worker_servers_matching_the_cluster_configuration() {}

    fn setup_master() -> Master {
        let bucket_name = Uuid::new_v4().to_string();
        let mut spec = MapReduceSpecification::new(&bucket_name, 3, 128, 128);
        spec.add_input(MapReduceInput::new(
            MapReduceInputFormat::Text,
            "input0.txt".into(),
            "WordCounter".into(),
        ));
        spec.set_output(MapReduceOutput::new(
            "/tmp/mapreduce/out".into(),
            10,
            MapReduceOutputFormat::Text,
            "Adder".into(),
            None,
        ));
        let mut input_splits = HashMap::new();
        let mut splits = Vec::new();
        for i in 0..100 {
            splits.push(InputSplit::new(
                &format!("mr_input_0_{i}_of_99"),
                "WordCounter",
            ));
        }
        input_splits.insert("input0.txt".to_string(), splits);

        Master::new(spec, input_splits, vec![])
    }
}
