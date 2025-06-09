use std::{borrow::Borrow, collections::HashMap, num::NonZeroU16, sync::Arc};

use fjall::{Keyspace, Partition, PartitionCreateOptions};
use parking_lot::RwLock;
use rune::{Context, runtime::RuntimeContext};
use wasmtime::{
    Config as WASMConfig, Engine as WASMEngine, InstanceAllocationStrategy, component::Linker,
};

use crate::{
    DataCell,
    error::TimeseriesError,
    format::Metadata,
    rune::{single::SingleRunePartition, tiered::TieredRunePartition},
    wasi::{
        WasiStateMaybeUninit,
        single::{Single, SingleComponent, SingleWasmPartition},
        tiered::{Tiered, TieredComponent, TieredWasmPartition},
    },
};

#[derive(Clone)]
pub enum TimeseriesPartition {
    SingleRune(SingleRunePartition),
    TieredRune(TieredRunePartition),
    SingleWasm(SingleWasmPartition),
    TieredWasm(TieredWasmPartition),
}

impl From<SingleRunePartition> for TimeseriesPartition {
    fn from(value: SingleRunePartition) -> Self {
        Self::SingleRune(value)
    }
}

impl From<TieredRunePartition> for TimeseriesPartition {
    fn from(value: TieredRunePartition) -> Self {
        Self::TieredRune(value)
    }
}

impl From<SingleWasmPartition> for TimeseriesPartition {
    fn from(value: SingleWasmPartition) -> Self {
        Self::SingleWasm(value)
    }
}

impl From<TieredWasmPartition> for TimeseriesPartition {
    fn from(value: TieredWasmPartition) -> Self {
        Self::TieredWasm(value)
    }
}

impl TimeseriesPartition {
    pub fn insert_metric<K>(&self, key: K, metric: DataCell) -> Result<(), TimeseriesError>
    where
        K: AsRef<[u8]>,
    {
        match self {
            Self::SingleRune(part) => part.insert_metric(key, metric),
            Self::TieredRune(part) => part.insert_metric(key, metric),
            Self::SingleWasm(part) => part.insert_metric(key, metric),
            Self::TieredWasm(part) => part.insert_metric(key, metric),
        }
    }

    pub fn as_single_rune(&self) -> Option<&SingleRunePartition> {
        if let Self::SingleRune(part) = self {
            Some(part)
        } else {
            None
        }
    }

    pub fn as_tiered_rune(&self) -> Option<&TieredRunePartition> {
        if let Self::TieredRune(part) = self {
            Some(part)
        } else {
            None
        }
    }

    pub fn as_single_wasm(&self) -> Option<&SingleWasmPartition> {
        if let Self::SingleWasm(part) = self {
            Some(part)
        } else {
            None
        }
    }

    pub fn as_tiered_wasm(&self) -> Option<&TieredWasmPartition> {
        if let Self::TieredWasm(part) = self {
            Some(part)
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub struct TimeseriesDatabase {
    keyspace: Keyspace,
    meta_partition: Partition,
    partitions: Arc<RwLock<HashMap<Arc<str>, TimeseriesPartition>>>,
    rune_single_context: Arc<Context>,
    rune_single_runtime: Arc<RuntimeContext>,
    rune_tiered_context: Arc<Context>,
    rune_tiered_runtime: Arc<RuntimeContext>,
    wasm_engine: WASMEngine,
    wasm_single_linker: Linker<WasiStateMaybeUninit<SingleComponent>>,
    wasm_tiered_linker: Linker<WasiStateMaybeUninit<TieredComponent>>,
}

impl TimeseriesDatabase {
    /// Create a new Timeseries Database that can hold different
    /// partitions (tables) of data.
    ///
    /// Takes an open [Keyspace] and a partition name that this can
    /// use for metadata.
    pub fn new<K: Borrow<Keyspace>, N: AsRef<str>>(
        keyspace: K,
        partition_name: N,
    ) -> Result<Self, TimeseriesError> {
        let keyspace: Keyspace = keyspace.borrow().clone();

        let partition_name = partition_name.as_ref();
        let partition_options = PartitionCreateOptions::default().block_size(8192);

        let meta_partition = keyspace.open_partition(partition_name, partition_options)?;

        let mut rune_single_context = Context::with_config(true)?;
        rune_single_context.install(crate::rune::single::module()?)?;

        let rune_single_context = Arc::new(rune_single_context);

        let rune_single_runtime = rune_single_context.runtime()?;
        let rune_single_runtime = Arc::new(rune_single_runtime);

        let mut rune_tiered_context = Context::with_config(true)?;
        rune_tiered_context.install(crate::rune::tiered::module()?)?;

        let rune_tiered_context = Arc::new(rune_tiered_context);

        let rune_tiered_runtime = rune_tiered_context.runtime()?;
        let rune_tiered_runtime = Arc::new(rune_tiered_runtime);

        let mut wasm_config = WASMConfig::default();

        wasm_config.consume_fuel(true);

        wasm_config.strategy(wasmtime::Strategy::Cranelift);
        wasm_config.memory_init_cow(true);

        wasm_config.allocation_strategy(InstanceAllocationStrategy::Pooling(Default::default()));

        let wasm_engine = WASMEngine::new(&wasm_config).map_err(TimeseriesError::WebAssembly)?;

        let mut wasm_single_linker = Linker::new(&wasm_engine);
        Single::add_to_linker(
            &mut wasm_single_linker,
            |component: &mut WasiStateMaybeUninit<SingleComponent>| {
                component.state.as_mut().expect("Checked init before call")
            },
        )
        .map_err(TimeseriesError::WebAssembly)?;

        wasmtime_wasi::p2::add_to_linker_sync(&mut wasm_single_linker)
            .map_err(TimeseriesError::WebAssembly)?;

        let mut wasm_tiered_linker = Linker::new(&wasm_engine);
        Tiered::add_to_linker(
            &mut wasm_tiered_linker,
            |component: &mut WasiStateMaybeUninit<TieredComponent>| {
                component.state.as_mut().expect("Checked init before call")
            },
        )
        .map_err(TimeseriesError::WebAssembly)?;

        wasmtime_wasi::p2::add_to_linker_sync(&mut wasm_tiered_linker)
            .map_err(TimeseriesError::WebAssembly)?;

        let mut partitions = HashMap::new();
        for key_pair in meta_partition.iter() {
            let (name, meta) = key_pair?;

            let name = Arc::<str>::from(String::from_utf8_lossy(&name));
            let partition =
                keyspace.open_partition(name.as_ref(), PartitionCreateOptions::default())?;

            let metadata = Metadata::from(meta);

            let timeseries_partition = match metadata {
                Metadata::SingleRune(metadata) => {
                    TimeseriesPartition::SingleRune(SingleRunePartition::new(
                        name.clone(),
                        &rune_single_context,
                        rune_single_runtime.clone(),
                        metadata,
                        partition,
                    )?)
                }
                Metadata::TieredRune(metadata) => {
                    TimeseriesPartition::TieredRune(TieredRunePartition::new(
                        name.clone(),
                        &rune_tiered_context,
                        rune_tiered_runtime.clone(),
                        metadata,
                        partition,
                    )?)
                }
                Metadata::SingleWasm(metadata) => {
                    TimeseriesPartition::SingleWasm(SingleWasmPartition::new(
                        name.clone(),
                        wasm_engine.clone(),
                        wasm_single_linker.clone(),
                        partition,
                        metadata,
                    )?)
                }
                Metadata::TieredWasm(metadata) => {
                    TimeseriesPartition::TieredWasm(TieredWasmPartition::new(
                        name.clone(),
                        wasm_engine.clone(),
                        wasm_tiered_linker.clone(),
                        partition,
                        metadata,
                    )?)
                }
            };

            partitions.insert(name, timeseries_partition);
        }

        Ok(Self {
            partitions: Arc::new(RwLock::new(partitions)),
            meta_partition,
            keyspace,
            rune_single_context,
            rune_single_runtime,
            rune_tiered_context,
            rune_tiered_runtime,
            wasm_engine,
            wasm_single_linker,
            wasm_tiered_linker,
        })
    }

    pub fn open_single_wasm<N: AsRef<str>, W: AsRef<[u8]>>(
        &self,
        name: N,
        width: NonZeroU16,
        interval: NonZeroU16,
        component: W,
    ) -> Result<SingleWasmPartition, TimeseriesError> {
        SingleWasmPartition::open_new(
            &self.keyspace,
            &self.meta_partition,
            self.wasm_engine.clone(),
            self.wasm_single_linker.clone(),
            name,
            width,
            interval,
            component,
        )
    }

    pub fn open_tiered_wasm<N: AsRef<str>, W: AsRef<[u8]>>(
        &self,
        name: N,
        component: W,
    ) -> Result<TieredWasmPartition, TimeseriesError> {
        TieredWasmPartition::open_new(
            &self.keyspace,
            &self.meta_partition,
            self.wasm_engine.clone(),
            self.wasm_tiered_linker.clone(),
            name,
            component,
        )
    }

    pub fn open_single_rune<N: AsRef<str>, S: AsRef<str>>(
        &self,
        name: N,
        width: NonZeroU16,
        interval: NonZeroU16,
        script: S,
    ) -> Result<SingleRunePartition, TimeseriesError> {
        SingleRunePartition::open_new(
            &self.keyspace,
            &self.meta_partition,
            &self.rune_single_context,
            self.rune_single_runtime.clone(),
            name,
            width,
            interval,
            script,
        )
    }

    pub fn open_tiered_rune<N: AsRef<str>, S: AsRef<str>>(
        &self,
        name: N,
        script: S,
    ) -> Result<TieredRunePartition, TimeseriesError> {
        TieredRunePartition::open_new(
            &self.keyspace,
            &self.meta_partition,
            &self.rune_tiered_context,
            self.rune_tiered_runtime.clone(),
            name,
            script,
        )
    }

    pub fn get_partition<N>(&self, name: N) -> Result<Option<TimeseriesPartition>, TimeseriesError>
    where
        N: AsRef<str>,
    {
        let partitions = self.partitions.read();

        Ok(partitions.get(name.as_ref()).cloned())
    }

    pub fn partitions(&self) -> impl ExactSizeIterator<Item = TimeseriesPartition> {
        let partitions = self.partitions.read();
        let partitions = partitions.values().cloned().collect::<Box<[_]>>();

        partitions.into_iter()
    }
}
