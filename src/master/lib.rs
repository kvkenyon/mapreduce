//! src/master/lib.rs

use crate::configuration::Settings;
use crate::master::{CallHome, MasterService, MasterStatus, WorkerInfo};
use crate::spec::MapReduceSpecification;
use crate::worker::WorkerServiceClient;
use crate::{mapreduce::InputSplit, spec::MapReduceOutput, worker::WorkerId};
use anyhow::Context;
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tarpc::{
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};
use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid; // or use std::sync::Mutex

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TaskState {
    Idle,
    InProgress,
    Completed,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MapTask {
    pub task_id: Uuid,
    pub state: TaskState,
    pub worker_id: Option<WorkerId>,
    pub input_split: InputSplit,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
    input_splits: Arc<RwLock<HashMap<String, Vec<InputSplit>>>>,
    number_of_map_tasks: usize,
    number_of_reduce_tasks: usize,
    map_tasks: Arc<RwLock<HashMap<Uuid, MapTask>>>,
    reduce_tasks: Arc<RwLock<HashMap<Uuid, ReduceTask>>>,
    #[allow(unused)]
    worker_service_clients: Vec<WorkerServiceClient>,
    workers_addresses: Arc<RwLock<HashMap<WorkerId, SocketAddr>>>,
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
            input_splits: Arc::new(RwLock::new(input_splits)),
            number_of_map_tasks,
            number_of_reduce_tasks,
            map_tasks: Arc::new(RwLock::new(map_tasks)),
            reduce_tasks: Arc::new(RwLock::new(reduce_tasks)),
            worker_service_clients,
            workers_addresses: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn map_tasks(&self) -> &Arc<RwLock<HashMap<Uuid, MapTask>>> {
        &self.map_tasks
    }
    pub fn reduce_tasks(&self) -> &Arc<RwLock<HashMap<Uuid, ReduceTask>>> {
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

    pub fn input_splits(&self) -> &Arc<RwLock<HashMap<String, Vec<InputSplit>>>> {
        &self.input_splits
    }

    pub async fn worker_info(&self) -> Vec<WorkerInfo> {
        let mut worker_infos = vec![];
        for (worker_id, socket_addr) in self.workers_addresses.read().await.iter() {
            let worker_info = WorkerInfo::new(*worker_id, *socket_addr, vec![], vec![]);
            worker_infos.push(worker_info);
        }
        worker_infos
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
        tracing::info!("waiting to bind socket address");
        let socket_addr = addr_rx.await.context("Failed to receive master address")?;
        tracing::info!("socket address acquired: {socket_addr}");
        Ok((socket_addr, handle))
    }

    #[tracing::instrument("Run master until stopped", skip_all)]
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

    pub fn master_mut(&mut self) -> &mut Master {
        &mut self.master
    }
}

impl MasterService for MasterServer {
    #[tracing::instrument("Call home from worker", fields(
     worker_id = %call_home.worker_id(),
     socket_addr = %call_home.socket_address()
    ), skip_all)]
    async fn call_home(mut self, _: tarpc::context::Context, call_home: CallHome) -> bool {
        let CallHome::WorkerResponse(worker_id, socket_address) = call_home;
        self.master_mut()
            .workers_addresses
            .write()
            .await
            .insert(worker_id, socket_address);
        tracing::info!(
            "num workers who called home: {}",
            self.master.workers_addresses.read().await.len()
        );
        true
    }

    async fn update_task(self, _context: tarpc::context::Context) {}

    #[tracing::instrument("Master status", skip_all)]
    async fn status(self, _context: tarpc::context::Context) -> MasterStatus {
        MasterStatus::Idle
    }

    #[tracing::instrument("Worker info", skip_all)]
    async fn worker_info(self, _: tarpc::context::Context) -> Vec<WorkerInfo> {
        self.master.worker_info().await
    }
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[cfg(test)]
mod tests {
    use crate::master::TaskState;
    use crate::test_utils::setup_master;
    use claims::{assert_none, assert_some};

    #[tokio::test]
    async fn should_create_idle_unassigned_map_tasks_for_each_input_split_and_reduce_tasks_from_spec()
     {
        let master = setup_master();
        assert_eq!(100, master.number_of_map_tasks);
        assert_eq!(100, master.map_tasks.read().await.len());

        let mut input_split_keys = vec![];
        for i in 0..100 {
            let key = format!("mr_input_0_{i}_of_99");
            input_split_keys.push(key);
        }

        for (tid, task) in master.map_tasks().read().await.iter() {
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

        for (tid, task) in master.reduce_tasks().read().await.iter() {
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
}
