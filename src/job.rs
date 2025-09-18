//! src/job
use crate::configuration::Settings;
use crate::mapreduce::InputSplit;
use crate::master::{MasterServer, MasterServiceClient, MasterStatus};
use crate::spec::MapReduceSpecification;
use crate::worker::{WorkerServer, WorkerServiceClient, WorkerStatus};
use anyhow::Context;
use std::collections::HashMap;
use tarpc::client::Config;
use tarpc::context;
use tarpc::tokio_serde::formats::Json;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct MapReduceJob {
    handles: Vec<JoinHandle<anyhow::Result<()>>>,
    shutdown_tx: broadcast::Sender<()>,
    input_splits: HashMap<String, Vec<InputSplit>>,
    spec: MapReduceSpecification,
    master_service_client: MasterServiceClient,
    worker_service_clients: Vec<WorkerServiceClient>,
}

impl MapReduceJob {
    #[tracing::instrument(name = "Start MapReduceJob", skip_all)]
    pub async fn start(
        spec: MapReduceSpecification,
        configuration: Settings,
        input_splits: HashMap<String, Vec<InputSplit>>,
    ) -> Result<Self, anyhow::Error> {
        let worker_count = configuration.cluster.workers;

        let mut worker_servers: Vec<WorkerServer> = vec![];

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let mut worker_clients = vec![];
        let mut handles = vec![];
        for i in 0..worker_count {
            let server = WorkerServer::build(configuration.clone()).await?;
            let (socket_addr, handle) = server
                .start(&shutdown_tx)
                .await
                .context(format!("Failed to start worker {i}"))?;
            let mut transport = tarpc::serde_transport::tcp::connect(socket_addr, Json::default);
            transport.config_mut().max_frame_length(usize::MAX);
            let worker_client =
                WorkerServiceClient::new(Config::default(), transport.await?).spawn();
            worker_clients.push(worker_client);
            handles.push(handle);
            worker_servers.push(server);
        }

        let master_server = MasterServer::build(
            configuration.clone(),
            spec.clone(),
            input_splits.clone(),
            worker_clients.clone(),
        )
        .await?;
        let (socket_addr, handle) = master_server
            .start(&shutdown_tx)
            .await
            .context("Failed to start worker")?;
        handles.push(handle);
        let mut transport = tarpc::serde_transport::tcp::connect(socket_addr, Json::default);
        transport.config_mut().max_frame_length(usize::MAX);
        let master_client = MasterServiceClient::new(Config::default(), transport.await?).spawn();

        for worker_server in worker_servers.iter_mut() {
            worker_server
                .worker()
                .set_master_service_client(master_client.clone());
        }

        Ok(Self {
            spec,
            handles,
            shutdown_tx,
            input_splits,
            master_service_client: master_client,
            worker_service_clients: worker_clients,
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

    pub fn shutdown_tx(&self) -> &broadcast::Sender<()> {
        &self.shutdown_tx
    }

    pub async fn master_status(&self) -> Result<MasterStatus, anyhow::Error> {
        self.master_service_client
            .status(context::current())
            .await
            .context("Failed to get status from master")
    }

    pub async fn worker_statuses(&self) -> Result<Vec<WorkerStatus>, anyhow::Error> {
        let mut statuses = vec![];
        for worker_client in self.worker_service_clients.iter() {
            let status = worker_client.status(context::current()).await.context(
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
    use crate::configuration::get_configuration;
    use crate::job::MapReduceJob;
    use crate::mapreduce::InputSplit;
    use crate::master::MasterStatus;
    use crate::spec::{
        MapReduceInput, MapReduceInputFormat, MapReduceOutput, MapReduceOutputFormat,
        MapReduceSpecification,
    };
    use crate::telemetry::init_tracing;
    use crate::worker::WorkerStatus;
    use std::collections::HashMap;
    use std::sync::LazyLock;
    use uuid::Uuid;
    use claims::assert_matches;

    static TRACING: LazyLock<()> = LazyLock::new(|| {
        init_tracing("tests::job").expect("Failed to setup tracing");
    });

    fn setup_job() -> impl Future<Output = Result<MapReduceJob, anyhow::Error>> {
        LazyLock::force(&TRACING);
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

        let config = get_configuration().expect("Failed to get config");

        MapReduceJob::start(spec, config, input_splits)
    }

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
}
