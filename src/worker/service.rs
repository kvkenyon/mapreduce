//! src/worker/service.rs
#[tarpc::service]
pub trait WorkerService {
    async fn ping() -> anyhow::Result<bool>;
}
