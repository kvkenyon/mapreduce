//! src/master/service.rs

#[derive(PartialEq, Debug, serde::Serialize, serde::Deserialize)]
pub enum MasterStatus {
    Idle,
    InProgress,
    Complete,
}

#[tarpc::service]
pub trait MasterService {
    async fn call_home() -> bool;

    async fn update_task();

    async fn status() -> MasterStatus;
}
