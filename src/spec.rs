//! src/spec.rs

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum MapReduceInputFormat {
    Text,
    Json,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum MapReduceOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone)]
pub struct MapReduceInput {
    format: MapReduceInputFormat,
    filename: String,
    mapper: String,
}

impl MapReduceInput {
    pub fn new(format: MapReduceInputFormat, filename: String, mapper: String) -> Self {
        MapReduceInput {
            format,
            filename,
            mapper,
        }
    }

    pub fn mapper(&self) -> &str {
        &self.mapper
    }

    pub fn format(&self) -> &MapReduceInputFormat {
        &self.format
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }
}

#[derive(Debug, Clone)]
pub struct MapReduceOutput {
    base_path: String,
    num_tasks: u32,
    format: MapReduceOutputFormat,
    reducer: String,
    combiner: Option<String>,
}

impl MapReduceOutput {
    pub fn new(
        base_path: String,
        num_tasks: u32,
        format: MapReduceOutputFormat,
        reducer: String,
        combiner: Option<String>,
    ) -> Self {
        MapReduceOutput {
            base_path,
            num_tasks,
            format,
            reducer,
            combiner,
        }
    }

    pub fn reducer(&self) -> &str {
        &self.reducer
    }

    pub fn format(&self) -> &MapReduceOutputFormat {
        &self.format
    }

    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    pub fn num_tasks(&self) -> u32 {
        self.num_tasks
    }

    pub fn combiner(&self) -> &Option<String> {
        &self.combiner
    }
}

#[derive(Debug, Clone)]
pub struct MapReduceSpecification {
    bucket_name: String,
    input: Vec<MapReduceInput>,
    output: Option<MapReduceOutput>,
    machines: u32,
    map_megabytes: u32,
    reduce_megabytes: u32,
}

impl MapReduceSpecification {
    pub fn new(
        bucket_name: &str,
        machines: u32,
        map_megabytes: u32,
        reduce_megabytes: u32,
    ) -> Self {
        MapReduceSpecification {
            bucket_name: bucket_name.into(),
            input: vec![],
            output: None,
            machines,
            map_megabytes,
            reduce_megabytes,
        }
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn add_input(&mut self, input: MapReduceInput) {
        self.input.push(input);
    }

    pub fn inputs(&self) -> &Vec<MapReduceInput> {
        &self.input
    }

    pub fn machines(&self) -> u32 {
        self.machines
    }

    pub fn map_megabytes(&self) -> u32 {
        self.map_megabytes
    }

    pub fn reduce_megabytes(&self) -> u32 {
        self.reduce_megabytes
    }

    pub fn set_output(&mut self, output: MapReduceOutput) {
        self.output = Some(output);
    }

    pub fn output(&self) -> Option<MapReduceOutput> {
        self.output.clone()
    }
}
