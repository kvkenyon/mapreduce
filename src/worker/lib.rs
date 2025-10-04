//! src/workers/lib.rs

use crate::configuration::Settings;
use crate::master::{CallHome, MapTask, MasterServiceClient, ReduceTask, TaskState};
use crate::worker::executor::{MapExecutor, ReduceExecutor};
use crate::worker::{
    WorkerInfo, WorkerService, WorkerServiceClient, WorkerServiceError, WorkerStatus,
};
use anyhow::{Context, anyhow};
use futures::{future, prelude::*};
use std::fmt::Formatter;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tarpc::{
    context,
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};
use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{error, info, instrument};
use uuid::Uuid;

#[derive(Clone, PartialEq, Debug, serde::Serialize, serde::Deserialize, Eq, Hash, Copy)]
pub struct WorkerId(Uuid);

impl std::fmt::Display for WorkerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.0)
    }
}

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

#[derive(Debug, Clone)]
enum ActiveTask {
    Map(Uuid),
    Reduce(Uuid),
}

#[derive(Clone, Debug)]
pub struct Worker {
    id: WorkerId,
    worker_status: Arc<RwLock<WorkerStatus>>,
    map_tasks: Arc<RwLock<Vec<MapTask>>>,
    reduce_tasks: Arc<RwLock<Vec<ReduceTask>>>,
    master_service_client: Option<MasterServiceClient>,
    active_task: Arc<RwLock<Option<ActiveTask>>>,
}

#[derive(Debug, Clone)]
pub struct WorkerClient {
    worker_id: WorkerId,
    client: WorkerServiceClient,
}

impl WorkerClient {
    pub fn new(worker_id: WorkerId, client: &WorkerServiceClient) -> Self {
        Self {
            worker_id,
            client: client.clone(),
        }
    }

    pub fn id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn client(&self) -> &WorkerServiceClient {
        &self.client
    }
}

impl Default for Worker {
    fn default() -> Self {
        Self::new()
    }
}

impl Worker {
    pub fn new() -> Self {
        let wid = WorkerId::new();
        Self {
            id: wid,
            worker_status: Arc::new(RwLock::new(WorkerStatus::Idle(wid))),
            map_tasks: Arc::new(RwLock::new(vec![])),
            reduce_tasks: Arc::new(RwLock::new(vec![])),
            master_service_client: None,
            active_task: Arc::new(RwLock::new(None)),
        }
    }

    pub fn id(&self) -> &WorkerId {
        &self.id
    }

    /// Find a task that is in idle state and wrap it in ActiveTask. Map tasks
    /// have higher priority. Reduce tasks must be idle and ready (file location
    /// was updated by the master after a map tasks completed)
    ///
    /// Returns a cloned task inside ActiveTask enum
    #[instrument("Get ready task", skip_all)]
    async fn get_ready_task(&self) -> Option<ActiveTask> {
        for map_task in self.map_tasks.read().await.iter() {
            if map_task.state == TaskState::Idle {
                info!("Found idle map task with id = {}", map_task.task_id);
                return Some(ActiveTask::Map(map_task.task_id));
            }
        }

        for reduce_task in self.reduce_tasks.read().await.iter() {
            if reduce_task.state == TaskState::Idle && reduce_task.is_ready() {
                info!("Found idle reduce task with id = {}", reduce_task.task_id);
                return Some(ActiveTask::Reduce(reduce_task.task_id));
            }
        }
        None
    }

    #[instrument("Execute tasks", skip_all, fields(worker_id = %self.id()))]
    async fn execute_tasks(self) -> anyhow::Result<()> {
        let mut ws = self.worker_status.write().await;
        *ws = WorkerStatus::InProgress(*self.id());
        info!("setting worker status to {:?}", *ws);
        while let Some(task) = self.get_ready_task().await {
            match task {
                ActiveTask::Map(map_task_id) => {
                    let mut tasks = self.map_tasks.write().await;
                    let maybe_map_task = tasks.iter_mut().find(|m| m.task_id == map_task_id);
                    info!("Maybe map task: {maybe_map_task:?}");
                    if let Some(map_task) = maybe_map_task {
                        let executor = MapExecutor::new(map_task).await?;
                        let mut active_task = self.active_task.write().await;
                        map_task.state = TaskState::InProgress;
                        *active_task = Some(task);
                        info!("Begin execute active map task = {active_task:?}");
                        executor.execute().await.inspect_err(|e| {
                            error!("Error in execute: {e}");
                            map_task.state = TaskState::Idle;
                        })?;
                        info!("End execute active map task = {active_task:?}");
                        map_task.state = TaskState::Completed;
                    }
                }
                ActiveTask::Reduce(reduce_task_id) => {
                    let mut tasks = self.reduce_tasks.write().await;
                    let maybe_reduce_task = tasks.iter_mut().find(|r| r.task_id == reduce_task_id);
                    info!("Maybe reduce task: {maybe_reduce_task:?}");
                    if let Some(reduce_task) = maybe_reduce_task {
                        let executor = ReduceExecutor::new(reduce_task.clone())?;
                        let mut active_task = self.active_task.write().await;
                        reduce_task.state = TaskState::InProgress;
                        *active_task = Some(task);
                        info!("Begin execute active reduce task = {active_task:?}");
                        executor.execute().await.inspect_err(|e| {
                            error!("Error in execute: {e}");
                            reduce_task.state = TaskState::Idle;
                        })?;
                        info!("End execute reduce active task = {active_task:?}");
                        reduce_task.state = TaskState::Completed;
                    }
                }
            }
        }
        *ws = WorkerStatus::Completed(*self.id());
        Ok(())
    }

    /// Spins until there are no more ready tasks. Then returns state to Idle.
    /// When working on tasks the worker will set its state to InProgress.
    pub async fn run(&self) -> Result<JoinHandle<anyhow::Result<()>>, anyhow::Error> {
        let worker_clone = self.clone();
        let handle = tokio::spawn(worker_clone.execute_tasks());
        Ok(handle)
    }

    pub async fn assign_map(&mut self, task: MapTask) -> Result<(), anyhow::Error> {
        self.map_tasks.write().await.push(task);
        Ok(())
    }

    pub async fn assign_reduce(&mut self, task: ReduceTask) -> Result<(), anyhow::Error> {
        self.reduce_tasks.write().await.push(task);
        Ok(())
    }

    pub async fn has_task(&self) -> bool {
        !self.map_tasks.read().await.is_empty() || !self.reduce_tasks.read().await.is_empty()
    }

    pub fn set_master_service_client(&mut self, master_service_client: MasterServiceClient) {
        self.master_service_client = Some(master_service_client);
    }
}

#[derive(Debug, Clone)]
pub struct WorkerServer {
    worker: Worker,
    host: String,
    port: u16,
    socket_addr: Arc<RwLock<Option<SocketAddr>>>,
}

impl WorkerServer {
    pub async fn build(config: Settings) -> anyhow::Result<Self> {
        let worker_server = WorkerServer {
            host: config.rpc.host,
            port: config.rpc.port,
            worker: Worker::new(),
            socket_addr: Arc::new(RwLock::new(None)),
        };
        Ok(worker_server)
    }

    #[tracing::instrument("WorkerServer start", skip_all)]
    pub async fn start(
        &mut self,
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
                    info!("Worker shutting down");
                   Ok(())
                }
            }
        });
        info!("waiting to bind socket address");
        let socket_addr = addr_rx.await.context("Failed to receive worker address")?;
        let mut curr_addr = self.socket_addr.write().await;
        *curr_addr = Some(socket_addr);
        info!("socket address acquired: {socket_addr}");
        Ok((socket_addr, handle))
    }

    #[tracing::instrument("Run worker until stopped", skip_all)]
    async fn run_until_stopped(
        server_addr: &SocketAddr,
        addr_tx: oneshot::Sender<SocketAddr>,
        worker_server: WorkerServer,
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
                    .execute(worker_server.clone().serve())
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

    pub fn worker(&mut self) -> &mut Worker {
        &mut self.worker
    }

    #[tracing::instrument("Worker call home", skip_all, fields(
        worker_id=%self.worker.id(),
    ))]
    pub async fn call_home(&self) -> anyhow::Result<bool> {
        let worker_id = self.worker.id();
        let socket_addr = self.socket_addr.as_ref();

        if socket_addr.read().await.is_none() {
            return Err(anyhow!(
                "Socket address is None, cannot call home to master"
            ));
        }

        let socket_addr = socket_addr.read().await.unwrap();

        let call_home = CallHome::WorkerResponse(*worker_id, socket_addr);
        self.worker
            .master_service_client
            .as_ref()
            .unwrap()
            .call_home(context::current(), call_home)
            .await?
            .context("Failed to call home")
    }
}

impl WorkerService for WorkerServer {
    async fn status(self, _context: context::Context) -> Result<WorkerStatus, WorkerServiceError> {
        let worker_status = self.worker.worker_status.read().await.clone();
        Ok(worker_status)
    }
    #[tracing::instrument("Assign map task", skip_all, fields(
       worker_id = %self.worker.id()
    ))]
    async fn assign_map_task(
        mut self,
        _: context::Context,
        map_task: MapTask,
    ) -> Result<bool, WorkerServiceError> {
        self.worker
            .assign_map(map_task)
            .await
            .context("Failed to assign map task")?;
        Ok(true)
    }

    #[tracing::instrument("Assign reduce task", skip_all, fields(
       worker_id = %self.worker.id()
    ))]
    async fn assign_reduce_task(
        mut self,
        _: context::Context,
        reduce_task: ReduceTask,
    ) -> Result<bool, WorkerServiceError> {
        self.worker
            .assign_reduce(reduce_task)
            .await
            .context("Failed to assign reduce task")?;
        Ok(true)
    }

    #[tracing::instrument("Start tasks", skip_all, fields(
       worker_id = %self.worker.id()
    ))]
    async fn start_tasks(self, _context: context::Context) -> Result<(), WorkerServiceError> {
        let _handle = self.worker.run().await?;
        Ok(())
    }

    async fn worker_info(self, _: context::Context) -> Result<WorkerInfo, WorkerServiceError> {
        Ok(WorkerInfo::new(
            self.worker.id,
            self.socket_addr.read().await.unwrap(),
            WorkerStatus::Idle(self.worker.id),
            self.worker.map_tasks.read().await.to_vec(),
            self.worker.reduce_tasks.read().await.to_vec(),
        ))
    }
}
async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[cfg(test)]
mod test {
    use crate::mapreduce::split_inputs;
    use crate::master::{MapTask, ReduceTask, TaskState};
    use crate::storage::S3Storage;
    use crate::test_utils::setup_rigorous_spec;
    use crate::worker::{Worker, WorkerStatus};
    use claims::assert_matches;
    use uuid::Uuid;

    #[tokio::test]
    async fn should_be_able_to_execute_single_map_task_and_reduce_task() {
        // Arrange
        let (spec, bucket_name, _) = setup_rigorous_spec().await;
        let job_id = Uuid::new_v4();
        let input_splits = split_inputs(
            &job_id.to_string(),
            &bucket_name,
            spec.inputs(),
            "mr_output",
            2048,
        )
        .await
        .expect("Failed to split input");
        let input_split = input_splits
            .get("input_0.txt")
            .unwrap()
            .first()
            .unwrap()
            .clone();
        let mut worker = Worker::new();
        let map_task_id = Uuid::new_v4();
        let reduce_task_id = Uuid::new_v4();
        let task = MapTask {
            job_id,
            task_id: map_task_id,
            r: spec.output().unwrap().num_tasks(),
            state: TaskState::Idle,
            worker_id: Some(*worker.id()),
            input_split,
        };

        let reduce_task = ReduceTask {
            job_id,
            task_id: reduce_task_id,
            state: TaskState::Idle,
            worker_id: Some(*worker.id()),
            output: spec.output().unwrap().clone(),
            input_location: Some(map_task_id.to_string()),
            r: 0,
        };

        worker
            .assign_reduce(reduce_task)
            .await
            .expect("Failed to assign reduce task");

        worker
            .assign_map(task)
            .await
            .expect("failed to assign map task");

        assert_matches!(
            worker.worker_status.read().await.clone(),
            WorkerStatus::Idle(_)
        );

        // Act
        let handle = worker.run().await.expect("Failed to spawn run task");

        handle.await.expect("failed to join run handle").unwrap();

        // Assert
        // we get the bucket for the task id and check for the mr_output splits
        // based on partition size r = 8
        let s3 = S3Storage::new(&map_task_id.to_string())
            .await
            .expect("Failed to get s3");

        let results = s3.list().await.expect("failed to list s3 bucket");

        for result in results {
            assert!(result.starts_with("mr_input_0_"));
        }
        let s3 = S3Storage::new(&bucket_name)
            .await
            .expect("Failed to get s3");
        let mr_output = s3
            .get("mr_output_0")
            .await
            .expect("Failed to get mr output");

        println!("mr_output = {mr_output}");

        assert_matches!(
            worker.worker_status.read().await.clone(),
            WorkerStatus::Completed(_)
        );
    }
}
