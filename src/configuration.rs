//! src/configuration.rs
use dotenv::dotenv;
use secrecy::SecretBox;
use serde_aux::field_attributes::deserialize_number_from_string;
use std::{
    net::{IpAddr, Ipv6Addr},
    str::FromStr,
};

#[derive(serde::Deserialize, Clone)]
pub struct Settings {
    pub storage: StorageSettings,
    pub rpc: RpcSettings,
}

#[derive(serde::Deserialize, Clone)]
pub struct StorageSettings {
    pub aws_region: String,
    pub aws_access_key_id: String,
    pub aws_secret_key: SecretBox<str>,
    pub aws_endpoint_url: String,
}

#[derive(serde::Deserialize, Clone)]
pub struct RpcSettings {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub port: u16,
    pub host: String,
}

impl RpcSettings {
    pub fn get_host(&self) -> IpAddr {
        IpAddr::V6(Ipv6Addr::from_str(&self.host).expect("Invalid host"))
    }
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    dotenv().ok();
    let settings = config::Config::builder()
        .add_source(
            config::Environment::with_prefix("MAPREDUCE")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;
    settings.try_deserialize::<Settings>()
}
