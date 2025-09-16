//! src/configuration.rs
use secrecy::SecretBox;
use serde_aux::field_attributes::deserialize_number_from_string;
use std::{
    net::{IpAddr, Ipv6Addr},
    str::FromStr,
};

#[derive(serde::Deserialize, Clone)]
pub struct Settings {
    pub cluster: ClusterSettings,
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

#[derive(serde::Deserialize, Clone)]
pub struct ClusterSettings {
    pub workers: u16,
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine the current directory.");
    let config_dir = base_path.join("configuration");

    let settings = config::Config::builder()
        .add_source(config::File::from(config_dir.join("spec.yaml")))
        .add_source(
            config::Environment::with_prefix("MAPREDUCE")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;
    settings.try_deserialize::<Settings>()
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::get_configuration;

    #[test]
    fn should_get_spec_dot_yaml() {
        let settings = get_configuration().expect("Failed to get configuration");

        assert_eq!(settings.storage.aws_secret_key.expose_secret(), "test");
        assert_eq!(settings.cluster.workers, 2);
    }
}
