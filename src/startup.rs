//! src/startup.rs
use crate::configuration::Settings;
use crate::master::MasterServer;
use crate::worker::WorkerServer;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct MapReduceJob {
    handles: Vec<JoinHandle<anyhow::Result<()>>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl MapReduceJob {
    #[tracing::instrument(name = "Start MapReduceJob", skip_all)]
    pub async fn start(configuration: Settings) -> Result<Self, anyhow::Error> {
        let worker_count = configuration.cluster.workers;

        let mut workers = vec![];

        for _ in 0..worker_count {
            let server = WorkerServer::build(configuration.clone()).await?;
            workers.push(server);
        }

        let master = MasterServer::build(configuration.clone()).await?;

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let mut handles = Vec::new();
        for worker in workers {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let handle = tokio::spawn(async move {
                tokio::select! {
                    result = worker.run_until_stopped() => {
                        result
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Worker shutting down");
                       Ok(())
                    }
                }
            });
            handles.push(handle);
        }
        let mut shutdown_rx = shutdown_tx.subscribe();
        let handle = tokio::spawn(async move {
            tokio::select! {
                result = master.run_until_stopped() => {
                    result
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!("Master shutting down");
                   Ok(())
                }
            }
        });
        handles.push(handle);

        Ok(Self {
            handles,
            shutdown_tx,
        })
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
}
