//! src/configuration.rs
use serde_aux::field_attributes::deserialize_number_from_string;

#[derive(serde::Deserialize, Clone, Debug)]
pub struct Settings {
    pub task_settings: TaskSettings,
    pub partition_settings: PartitionSettings,
    // Mode is either: sequential or parallel
    pub mode: String,
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct TaskSettings {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub map_tasks: u16,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub reduce_task: u16,
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct PartitionSettings {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub partition_size: usize,
    pub partition_function: String,
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine the current directory.");
    let config_dir = base_path.join("configuration");

    let settings = config::Config::builder()
        .add_source(config::File::from(config_dir.join("base.yaml")))
        .build()?;
    settings.try_deserialize::<Settings>()
}
