//! src/registry.rs
use crate::functions::{Key, Mapper, Reducer, Value};

// Dynamic versions of your traits
pub trait MapperDyn: Send + Sync {
    fn name(&self) -> &str;
    fn map(&mut self, key: Key, value: Value);
}

pub trait ReducerDyn: Send + Sync {
    fn name(&self) -> &str;
    fn reduce(&self, key: Key, values: Vec<Value>);
}

// Registration structs
pub struct MapperRegistration {
    pub name: &'static str,
    pub factory: fn() -> Box<dyn MapperDyn>,
}

pub struct ReducerRegistration {
    pub name: &'static str,
    pub factory: fn() -> Box<dyn ReducerDyn>,
}

// Inventory collections
inventory::collect!(MapperRegistration);
inventory::collect!(ReducerRegistration);

// Get mapper by name
pub fn get_mapper(name: &str) -> Result<Box<dyn MapperDyn>, String> {
    inventory::iter::<MapperRegistration>()
        .find(|reg| reg.name == name)
        .map(|reg| (reg.factory)())
        .ok_or_else(|| format!("Mapper '{}' not found", name))
}

// Get reducer by name
pub fn get_reducer(name: &str) -> Result<Box<dyn ReducerDyn>, String> {
    inventory::iter::<ReducerRegistration>()
        .find(|reg| reg.name == name)
        .map(|reg| (reg.factory)())
        .ok_or_else(|| format!("Reducer '{}' not found", name))
}

// List all registered mappers (useful for debugging)
pub fn list_mappers() -> Vec<&'static str> {
    inventory::iter::<MapperRegistration>()
        .map(|reg| reg.name)
        .collect()
}

// List all registered reducers (useful for debugging)
pub fn list_reducers() -> Vec<&'static str> {
    inventory::iter::<ReducerRegistration>()
        .map(|reg| reg.name)
        .collect()
}

// Wrapper struct to add name to mappers - WITH PUBLIC FIELDS
pub struct MapperWrapper<M: Mapper> {
    pub name: &'static str,
    pub inner: M,
}

impl<M: Mapper + Send + Sync> MapperDyn for MapperWrapper<M> {
    fn name(&self) -> &str {
        self.name
    }

    fn map(&mut self, key: Key, value: Value) {
        self.inner.map(key, value);
    }
}

// Wrapper struct to add name to reducers - WITH PUBLIC FIELDS
pub struct ReducerWrapper<R: Reducer> {
    pub name: &'static str,
    pub inner: R,
}

impl<R: Reducer + Send + Sync> ReducerDyn for ReducerWrapper<R> {
    fn name(&self) -> &str {
        self.name
    }

    fn reduce(&self, key: Key, values: Vec<Value>) {
        self.inner.reduce(key, values.into_iter());
    }
}

// Macro for mappers with custom emitter
#[macro_export]
macro_rules! impl_mapper_with_emitter {
    ($mapper_type:ty, $name:expr, $emitter_type:ty) => {
        inventory::submit! {
            $crate::registry::MapperRegistration {
                name: $name,
                factory: || {
                    use $crate::functions::Mapper;
                    let emitter = <$emitter_type>::default();
                    let mapper = <$mapper_type>::build(emitter);
                    Box::new($crate::registry::MapperWrapper {
                        name: $name,
                        inner: mapper,
                    })
                },
            }
        }
    };
}

// Macro for mappers with default emitter
#[macro_export]
macro_rules! impl_mapper {
    ($mapper_type:ty, $name:expr, $r:expr) => {
        inventory::submit! {
            $crate::registry::MapperRegistration {
                name: $name,
                factory: || {
                    use $crate::functions::{Mapper, FileMapEmitter};
                    let emitter = FileMapEmitter::new("/tmp/mapreduce/output/map", $r).expect("Failed to build file map emitter");
                    let mapper = <$mapper_type>::build(emitter);
                    Box::new($crate::registry::MapperWrapper {
                        name: $name,
                        inner: mapper,
                    })
                },
            }
        }
    };
}

// Macro for reducers with custom emitter
#[macro_export]
macro_rules! impl_reducer_with_emitter {
    ($reducer_type:ty, $name:expr, $emitter_type:ty) => {
        inventory::submit! {
            $crate::registry::ReducerRegistration {
                name: $name,
                factory: || {
                    use $crate::functions::Reducer;
                    let emitter = <$emitter_type>::default();
                    let reducer = <$reducer_type>::build(emitter);
                    Box::new($crate::registry::ReducerWrapper {
                        name: $name,
                        inner: reducer,
                    })
                },
            }
        }
    };
}

// Macro for reducers with default emitter
#[macro_export]
macro_rules! impl_reducer {
    ($reducer_type:ty, $name:expr) => {
        inventory::submit! {
            $crate::registry::ReducerRegistration {
                name: $name,
                factory: || {
                    use $crate::functions::{Reducer, DefaultReduceEmitter};
                    let emitter = DefaultReduceEmitter::default();
                    let reducer = <$reducer_type>::build(emitter);
                    Box::new($crate::registry::ReducerWrapper {
                        name: $name,
                        inner: reducer,
                    })
                },
            }
        }
    };
}
