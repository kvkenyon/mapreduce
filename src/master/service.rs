//! src/master/service.rs
#[tarpc::service]
pub trait MasterService {
    async fn call_home() -> bool;
    async fn update_task();
}
