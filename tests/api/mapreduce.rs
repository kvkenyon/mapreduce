//! src/tests/api/mapreduce.rs
use crate::helpers::test_data_dir;
use mapreduce::mapreduce::MapReduce;
use mapreduce::spec::{
    MapReduceInput, MapReduceInputFormat, MapReduceOutput, MapReduceSpecification,
};
use mapreduce::storage::S3Storage;
use mapreduce::telemetry::init_tracing;
use uuid::Uuid;

async fn setup() -> (MapReduceSpecification, S3Storage) {
    init_tracing("tests::api::mapreduce").expect("Failed to setup tracing");
    let bucket_name = Uuid::new_v4().to_string();
    let mut spec = MapReduceSpecification::new(&bucket_name, 3, 128, 128);
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
        "mr_output".into(),
        10,
        mapreduce::spec::MapReduceOutputFormat::Text,
        "Adder".to_string(),
        None,
    ));

    (spec, s3)
}

#[allow(unused)]
async fn teardown(s3: S3Storage, test_key: &str) {
    todo!()
}

#[tokio::test]
async fn should_process_input_splits_on_remote_storage() {
    // Arrange
    let (spec, s3) = setup().await;

    // Act
    let job = MapReduce::new(spec)
        .await
        .expect("Failed to create map reduce job");

    let input_splits = job.input_splits();

    for (input_file_name, splits) in input_splits {
        let input_file_as_string = s3
            .get(input_file_name)
            .await
            .unwrap_or_else(|_| panic!("Failed to get input file: {input_file_name}"));

        let mut total_size = 0;
        for split in splits {
            let remote_key = split.key();
            let split_as_string = s3
                .get(remote_key)
                .await
                .unwrap_or_else(|_| panic!("Failed to get remote key: {remote_key}"));
            total_size += split_as_string.len();
        }
        assert_eq!(input_file_as_string.len(), total_size);
    }
}
