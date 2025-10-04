//! src/test_utils.rs
use crate::configuration::get_configuration;
#[cfg(test)]
use crate::job::MapReduceJob;
use crate::mapreduce::{InputSplit, split_inputs};
use crate::master::{Master, MasterServer};
use crate::spec::{
    MapReduceInput, MapReduceInputFormat, MapReduceOutput, MapReduceOutputFormat,
    MapReduceSpecification,
};
use crate::storage::S3Storage;
use crate::telemetry::init_tracing;
use crate::worker::WorkerServer;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;
use tokio::sync::broadcast::Sender;
use uuid::Uuid;

static TRACING: LazyLock<()> = LazyLock::new(|| {
    init_tracing("tests").expect("Failed to setup tracing");
});

pub fn setup_job() -> impl Future<Output = Result<MapReduceJob, anyhow::Error>> {
    LazyLock::force(&TRACING);
    let bucket_name = Uuid::new_v4().to_string();
    let mut spec = MapReduceSpecification::new(&bucket_name, 3, 128, 128, 128);
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
        let bucket_name_clone = bucket_name.clone();
        splits.push(InputSplit::new(
            &bucket_name_clone,
            &format!("mr_input_0_{i}_of_99"),
            "WordCounter",
        ));
    }
    input_splits.insert("input0.txt".to_string(), splits);

    MapReduceJob::start(Uuid::new_v4(), spec, input_splits)
}

pub fn setup_master() -> Master {
    let bucket_name = Uuid::new_v4().to_string();
    let mut spec = MapReduceSpecification::new(&bucket_name, 3, 128, 128, 128);
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
        let bucket_name_clone = bucket_name.clone();
        splits.push(InputSplit::new(
            &bucket_name_clone,
            &format!("mr_input_0_{i}_of_99"),
            "WordCounter",
        ));
    }
    input_splits.insert("input0.txt".to_string(), splits);

    Master::new(Uuid::new_v4(), spec, input_splits, vec![])
}

pub async fn setup_rigorous_spec() -> (MapReduceSpecification, String, S3Storage) {
    LazyLock::force(&TRACING);
    let bucket_name = Uuid::new_v4().to_string();
    let mut spec = MapReduceSpecification::new(&bucket_name, 3, 128, 128, 128);
    let s3 = S3Storage::new(&bucket_name)
        .await
        .expect("Failed to get storage");
    let mut path = test_data_dir();
    for i in 0..5 {
        let filename = format!("input_{i}.txt");
        path.push(&filename);
        let data = std::fs::read_to_string(&path).expect("Failed to read test file");
        s3.put(&filename, data.as_bytes())
            .await
            .expect("Failed to store input file in s3");
        spec.add_input(MapReduceInput::new(
            MapReduceInputFormat::Text,
            filename,
            "WordCounter".to_string(),
        ));
        path.pop();
    }
    let files = s3.list().await.expect("Failed to list files");
    assert_eq!(files.len(), 5);

    spec.set_output(MapReduceOutput::new(
        bucket_name.clone(),
        1,
        MapReduceOutputFormat::Text,
        "Adder".to_string(),
        None,
    ));

    (spec, bucket_name, s3)
}

pub async fn setup_cluster(num_worker: u16) -> (MasterServer, Vec<WorkerServer>, Sender<()>) {
    let (spec, bucket_name, _) = setup_rigorous_spec().await;

    let mut config = get_configuration().expect("failed to get config");
    config.cluster.workers = num_worker;

    let job_id = Uuid::new_v4();
    let input_splits = split_inputs(
        &job_id.to_string(),
        &bucket_name,
        spec.inputs(),
        "mr_output",
        128,
    )
    .await
    .expect("Failed to split input");

    let mut configuration = get_configuration().expect("Failed to get configuration");

    configuration.cluster.workers = num_worker;

    let (_, shutdown_tx, _, _, master_server, worker_servers) =
        crate::job::setup_cluster(job_id, &configuration, &spec, &input_splits)
            .await
            .expect("Failed to setup cluster");

    (master_server, worker_servers, shutdown_tx)
}

pub fn test_data_dir() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("data");
    path
}
