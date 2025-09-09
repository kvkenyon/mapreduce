//! src/mapreduce.rs
use crate::{
    functions::{Mapper, Reducer},
    spec::MapReduceSpecification,
};

pub struct MapReduce<M: Mapper, R: Reducer> {
    mapper: Option<M>,
    reducer: Option<R>,
    spec: MapReduceSpecification,
}

impl<M: Mapper, R: Reducer> MapReduce<M, R> {
    pub fn new(spec: MapReduceSpecification) -> Self {
        MapReduce {
            spec,
            mapper: None,
            reducer: None,
        }
    }

    pub fn register_mapper(&mut self, m: M)
    where
        M: Mapper,
    {
        self.mapper = Some(m);
    }

    pub fn mapper(&self) -> Option<&M> {
        match &self.mapper {
            Some(m) => Some(m),
            None => None,
        }
    }

    pub fn register_reducer(&mut self, r: R)
    where
        R: Reducer,
    {
        self.reducer = Some(r);
    }

    pub fn reducer(&self) -> Option<&R> {
        match &self.reducer {
            Some(r) => Some(r),
            None => None,
        }
    }

    pub fn spec(&self) -> &MapReduceSpecification {
        &self.spec
    }
}
