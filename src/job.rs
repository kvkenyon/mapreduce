//! src/job
use crate::configuration::{Settings, get_configuration};
use crate::mapreduce::InputSplit;
use crate::master::{MasterServer, MasterServiceClient, MasterStatus};
use crate::spec::MapReduceSpecification;
use crate::worker::{WorkerClient, WorkerServer, WorkerServiceClient, WorkerStatus};
use anyhow::Context;
use std::collections::HashMap;
use tarpc::client::Config;
use tarpc::context;
use tarpc::tokio_serde::formats::Json;
use tokio::sync::broadcast::{self, Sender};
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct MapReduceJob {
    handles: Vec<JoinHandle<anyhow::Result<()>>>,
    shutdown_tx: Sender<()>,
    input_splits: HashMap<String, Vec<InputSplit>>,
    spec: MapReduceSpecification,
    master_service_client: MasterServiceClient,
    worker_clients: Vec<WorkerClient>,
}

#[tracing::instrument("Setup worker servers", skip_all)]
pub async fn setup_worker_servers(
    configuration: &Settings,
    worker_servers: &mut Vec<WorkerServer>,
    worker_clients: &mut Vec<WorkerClient>,
    handles: &mut Vec<JoinHandle<anyhow::Result<()>>>,
    shutdown_tx: &Sender<()>,
) -> anyhow::Result<()> {
    let worker_count = configuration.cluster.workers;
    for i in 0..worker_count {
        let mut server = WorkerServer::build(configuration.clone()).await?;
        let (socket_addr, handle) = server
            .start(shutdown_tx)
            .await
            .context(format!("Failed to start worker {i}"))?;
        let mut transport = tarpc::serde_transport::tcp::connect(socket_addr, Json::default);
        transport.config_mut().max_frame_length(usize::MAX);
        let worker_client = WorkerServiceClient::new(Config::default(), transport.await?).spawn();
        worker_clients.push(WorkerClient::new(*server.worker().id(), &worker_client));
        handles.push(handle);
        worker_servers.push(server);
    }
    Ok(())
}

#[tracing::instrument("Setup master", skip_all)]
pub async fn setup_master_server(
    configuration: &Settings,
    spec: &MapReduceSpecification,
    input_splits: &HashMap<String, Vec<InputSplit>>,
    worker_clients: &[WorkerClient],
    handles: &mut Vec<JoinHandle<anyhow::Result<()>>>,
    shutdown_tx: &Sender<()>,
) -> anyhow::Result<(MasterServiceClient, MasterServer)> {
    let master_server = MasterServer::build(
        configuration.clone(),
        spec.clone(),
        input_splits.clone(),
        worker_clients.to_vec(),
    )
    .await?;
    let (socket_addr, handle) = master_server
        .start(shutdown_tx)
        .await
        .context("Failed to start master")?;
    handles.push(handle);
    let mut transport = tarpc::serde_transport::tcp::connect(socket_addr, Json::default);
    transport.config_mut().max_frame_length(usize::MAX);
    let master_client = MasterServiceClient::new(Config::default(), transport.await?).spawn();
    Ok((master_client, master_server))
}

#[tracing::instrument("Register master service client with workers", skip_all)]
pub async fn register_master_service_client_with_workers(
    master_service_client: &MasterServiceClient,
    worker_servers: &mut [WorkerServer],
) -> anyhow::Result<()> {
    for worker_server in worker_servers.iter_mut() {
        worker_server
            .worker()
            .set_master_service_client(master_service_client.clone());
    }
    Ok(())
}

#[tracing::instrument("Register workers with master", skip_all)]
pub async fn register_workers_with_master(worker_servers: &[WorkerServer]) -> anyhow::Result<()> {
    for worker_server in worker_servers.iter() {
        worker_server
            .call_home()
            .await
            .context("Failed to call home")?;
    }
    Ok(())
}

pub async fn setup_cluster(
    configuration: &Settings,
    spec: &MapReduceSpecification,
    input_splits: &HashMap<String, Vec<InputSplit>>,
) -> anyhow::Result<(
    Vec<JoinHandle<anyhow::Result<()>>>,
    Sender<()>,
    MasterServiceClient,
    Vec<WorkerClient>,
    MasterServer,
    Vec<WorkerServer>,
)> {
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut worker_servers: Vec<WorkerServer> = vec![];
    let mut worker_clients = vec![];
    let mut handles = vec![];

    setup_worker_servers(
        configuration,
        &mut worker_servers,
        &mut worker_clients,
        &mut handles,
        &shutdown_tx,
    )
    .await
    .context("Failed to setup worker servers")?;

    let (master_service_client, master_server) = setup_master_server(
        configuration,
        spec,
        input_splits,
        &worker_clients,
        &mut handles,
        &shutdown_tx,
    )
    .await
    .context("Failed to setup master server")?;

    register_master_service_client_with_workers(&master_service_client, &mut worker_servers)
        .await
        .context("Failed to register master service clients with workers.")?;

    register_workers_with_master(&worker_servers)
        .await
        .context("Failed to register workers with master")?;

    Ok((
        handles,
        shutdown_tx,
        master_service_client,
        worker_clients,
        master_server,
        worker_servers,
    ))
}

impl MapReduceJob {
    #[tracing::instrument(name = "Start MapReduceJob", skip_all)]
    pub async fn start(
        spec: MapReduceSpecification,
        input_splits: HashMap<String, Vec<InputSplit>>,
    ) -> Result<Self, anyhow::Error> {
        let configuration = get_configuration().context("Failed to get configuration")?;

        let (handles, shutdown_tx, master_service_client, worker_clients, _, _) =
            setup_cluster(&configuration, &spec, &input_splits)
                .await
                .context("Failed to setup cluster")?;

        Ok(Self {
            spec,
            handles,
            shutdown_tx,
            input_splits,
            master_service_client,
            worker_clients,
        })
    }

    pub fn input_splits(&self) -> &HashMap<String, Vec<InputSplit>> {
        &self.input_splits
    }

    pub fn spec(&self) -> &MapReduceSpecification {
        &self.spec
    }

    #[tracing::instrument("Shutdown MapReduceJob", skip_all)]
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.shutdown_tx.send(()).ok();

        // Wait for all workers
        for handle in self.handles {
            handle.await??;
        }

        tracing::info!("All services shut down gracefully");
        Ok(())
    }

    pub fn handles(&self) -> &Vec<JoinHandle<anyhow::Result<()>>> {
        &self.handles
    }

    pub fn shutdown_tx(&self) -> &Sender<()> {
        &self.shutdown_tx
    }

    pub async fn master_status(&self) -> Result<MasterStatus, anyhow::Error> {
        self.master_service_client
            .status(context::current())
            .await?
            .context("Failed to get status from master")
    }

    pub async fn worker_statuses(&self) -> Result<Vec<WorkerStatus>, anyhow::Error> {
        let mut statuses = vec![];
        for worker_client in self.worker_clients.iter() {
            let status = worker_client
                .client()
                .status(context::current())
                .await?
                .context(
                    "Failed to get \
           status from worker",
                )?;
            statuses.push(status);
        }

        Ok(statuses)
    }
}

#[cfg(test)]
mod tests {
    use crate::master::MasterStatus;
    use crate::test_utils::setup_job;
    use crate::worker::WorkerStatus;
    use claims::assert_matches;
    use tarpc::context;

    #[tokio::test]
    async fn should_be_able_to_get_status_from_master() {
        let job = setup_job();
        let job = job.await.expect("Failed to run job");
        let status = job
            .master_status()
            .await
            .expect("Failed to get master status");
        assert_eq!(MasterStatus::Idle, status);
        job.shutdown().await.expect("Failed to shutdown job");
    }

    #[tokio::test]
    async fn should_be_able_to_get_status_from_workers() {
        let job = setup_job();
        let job = job.await.expect("Failed to run job");
        let worker_statuses = job
            .worker_statuses()
            .await
            .expect("Failed to get worker statuses");
        for status in worker_statuses {
            assert_matches!(status, WorkerStatus::Idle(_));
        }
        job.shutdown().await.expect("Failed to shutdown job");
    }

    #[tokio::test]
    async fn should_be_able_to_get_worker_info_from_master_since_they_call_home() {
        let job = setup_job();
        let job = job.await.expect("Failed to run job");

        let worker_info = job
            .master_service_client
            .worker_info(context::current())
            .await
            .expect("Failed to make it happen")
            .expect("Failed to get worker info");

        assert_eq!(2, worker_info.len());
    }
}
