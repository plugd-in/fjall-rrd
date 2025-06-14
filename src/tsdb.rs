use std::{borrow::Borrow, collections::HashMap, num::NonZeroU16, sync::Arc};

use fjall::{Keyspace, Partition, PartitionCreateOptions};
use parking_lot::RwLock;

use crate::{DataCell, error::TimeseriesError, format::Metadata};

#[cfg(feature = "rune")]
use crate::rune::{single::SingleRunePartition, tiered::TieredRunePartition};
#[cfg(feature = "rune")]
use rune::{Context, runtime::RuntimeContext};

#[cfg(feature = "wasm")]
use super::wasm::{
    WasiStateMaybeUninit,
    single::{Single, SingleComponent, SingleWasmPartition},
    tiered::{Tiered, TieredComponent, TieredWasmPartition},
};
#[cfg(feature = "wasm")]
use wasmtime::{
    Config as WASMConfig, Engine as WASMEngine, InstanceAllocationStrategy, component::Linker,
};

#[derive(Clone)]
/// The general interface to timeseries data.
///
/// To replace scripts or perform language-specific
/// actions, you can use a match statement to extract
/// the language-specific variant, or you can use functions
/// like, for example, [as_single_rune](TimeseriesPartition)
/// to try to get the specific language+type of partition.
pub enum TimeseriesPartition {
    #[cfg(feature = "rune")]
    SingleRune(SingleRunePartition),
    #[cfg(feature = "rune")]
    TieredRune(TieredRunePartition),
    #[cfg(feature = "wasm")]
    SingleWasm(SingleWasmPartition),
    #[cfg(feature = "wasm")]
    TieredWasm(TieredWasmPartition),
}

#[cfg(feature = "rune")]
impl From<SingleRunePartition> for TimeseriesPartition {
    fn from(value: SingleRunePartition) -> Self {
        Self::SingleRune(value)
    }
}

#[cfg(feature = "rune")]
impl From<TieredRunePartition> for TimeseriesPartition {
    fn from(value: TieredRunePartition) -> Self {
        Self::TieredRune(value)
    }
}

#[cfg(feature = "wasm")]
impl From<SingleWasmPartition> for TimeseriesPartition {
    fn from(value: SingleWasmPartition) -> Self {
        Self::SingleWasm(value)
    }
}

#[cfg(feature = "wasm")]
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
            #[cfg(feature = "rune")]
            Self::SingleRune(part) => part.insert_metric(key, metric),
            #[cfg(feature = "rune")]
            Self::TieredRune(part) => part.insert_metric(key, metric),
            #[cfg(feature = "wasm")]
            Self::SingleWasm(part) => part.insert_metric(key, metric),
            #[cfg(feature = "wasm")]
            Self::TieredWasm(part) => part.insert_metric(key, metric),
        }
    }

    #[cfg(feature = "rune")]
    pub fn as_single_rune(&self) -> Option<&SingleRunePartition> {
        if let Self::SingleRune(part) = self {
            Some(part)
        } else {
            None
        }
    }

    #[cfg(feature = "rune")]
    pub fn as_tiered_rune(&self) -> Option<&TieredRunePartition> {
        if let Self::TieredRune(part) = self {
            Some(part)
        } else {
            None
        }
    }

    #[cfg(feature = "wasm")]
    pub fn as_single_wasm(&self) -> Option<&SingleWasmPartition> {
        if let Self::SingleWasm(part) = self {
            Some(part)
        } else {
            None
        }
    }

    #[cfg(feature = "wasm")]
    pub fn as_tiered_wasm(&self) -> Option<&TieredWasmPartition> {
        if let Self::TieredWasm(part) = self {
            Some(part)
        } else {
            None
        }
    }
}

#[derive(Clone)]
/// The main [TimeseriesDatabase] struct.
///
/// Used to open [TimeseriesPartitions](TimeseriesPartition) with
/// seperately stored timeseries data and customized insertion/collection
/// logic.
pub struct TimeseriesDatabase {
    keyspace: Keyspace,
    meta_partition: Partition,
    partitions: Arc<RwLock<HashMap<Arc<str>, TimeseriesPartition>>>,
    /// Used to lock timeseries operations while
    /// recovering from a backup.
    ///
    /// Creating a backup will just use Fjall's [Snapshots](fjall::Snapshot).
    backup_lock: Arc<RwLock<()>>,
    #[cfg(feature = "rune")]
    rune_single_context: Arc<Context>,
    #[cfg(feature = "rune")]
    rune_single_runtime: Arc<RuntimeContext>,
    #[cfg(feature = "rune")]
    rune_tiered_context: Arc<Context>,
    #[cfg(feature = "rune")]
    rune_tiered_runtime: Arc<RuntimeContext>,
    #[cfg(feature = "wasm")]
    wasm_engine: WASMEngine,
    #[cfg(feature = "wasm")]
    wasm_single_linker: Linker<WasiStateMaybeUninit<SingleComponent>>,
    #[cfg(feature = "wasm")]
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

        #[cfg(feature = "rune")]
        let (rune_single_context, rune_single_runtime) = {
            let mut rune_single_context = Context::with_config(true)?;
            rune_single_context.install(crate::rune::single::module()?)?;

            let rune_single_context = Arc::new(rune_single_context);

            let rune_single_runtime = rune_single_context.runtime()?;
            let rune_single_runtime = Arc::new(rune_single_runtime);

            (rune_single_context, rune_single_runtime)
        };

        #[cfg(feature = "rune")]
        let (rune_tiered_context, rune_tiered_runtime) = {
            let mut rune_tiered_context = Context::with_config(true)?;
            rune_tiered_context.install(crate::rune::tiered::module()?)?;

            let rune_tiered_context = Arc::new(rune_tiered_context);

            let rune_tiered_runtime = rune_tiered_context.runtime()?;
            let rune_tiered_runtime = Arc::new(rune_tiered_runtime);

            (rune_tiered_context, rune_tiered_runtime)
        };

        #[cfg(feature = "wasm")]
        let (wasm_engine, wasm_single_linker, wasm_tiered_linker) = {
            let mut wasm_config = WASMConfig::default();

            wasm_config.consume_fuel(true);

            wasm_config.strategy(wasmtime::Strategy::Cranelift);
            wasm_config.memory_init_cow(true);

            wasm_config
                .allocation_strategy(InstanceAllocationStrategy::Pooling(Default::default()));

            let wasm_engine =
                WASMEngine::new(&wasm_config).map_err(TimeseriesError::WebAssembly)?;

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

            (wasm_engine, wasm_single_linker, wasm_tiered_linker)
        };

        let mut partitions = HashMap::new();
        for key_pair in meta_partition.iter() {
            let (name, meta) = key_pair?;

            let name = Arc::<str>::from(String::from_utf8_lossy(&name));
            let partition =
                keyspace.open_partition(name.as_ref(), PartitionCreateOptions::default())?;

            let metadata = Metadata::try_from(meta)?;

            let timeseries_partition = match metadata {
                #[cfg(feature = "rune")]
                Metadata::SingleRune(metadata) => {
                    TimeseriesPartition::SingleRune(SingleRunePartition::new(
                        name.clone(),
                        rune_single_context.clone(),
                        rune_single_runtime.clone(),
                        metadata,
                        partition,
                    )?)
                }
                #[cfg(feature = "rune")]
                Metadata::TieredRune(metadata) => {
                    TimeseriesPartition::TieredRune(TieredRunePartition::new(
                        name.clone(),
                        rune_tiered_context.clone(),
                        rune_tiered_runtime.clone(),
                        metadata,
                        partition,
                    )?)
                }
                #[cfg(feature = "wasm")]
                Metadata::SingleWasm(metadata) => {
                    TimeseriesPartition::SingleWasm(SingleWasmPartition::new(
                        name.clone(),
                        wasm_engine.clone(),
                        wasm_single_linker.clone(),
                        partition,
                        metadata,
                    )?)
                }
                #[cfg(feature = "wasm")]
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
            backup_lock: Default::default(),
            meta_partition,
            keyspace,
            #[cfg(feature = "rune")]
            rune_single_context,
            #[cfg(feature = "rune")]
            rune_single_runtime,
            #[cfg(feature = "rune")]
            rune_tiered_context,
            #[cfg(feature = "rune")]
            rune_tiered_runtime,
            #[cfg(feature = "wasm")]
            wasm_engine,
            #[cfg(feature = "wasm")]
            wasm_single_linker,
            #[cfg(feature = "wasm")]
            wasm_tiered_linker,
        })
    }

    #[cfg(feature = "wasm")]
    pub fn open_single_wasm<N: AsRef<str>, W: AsRef<[u8]>>(
        &self,
        name: N,
        width: NonZeroU16,
        interval: NonZeroU16,
        component: W,
    ) -> Result<SingleWasmPartition, TimeseriesError> {
        let name = Arc::<str>::from(name.as_ref());
        let wasm_part = SingleWasmPartition::open_new(
            &self.keyspace,
            &self.meta_partition,
            self.wasm_engine.clone(),
            self.wasm_single_linker.clone(),
            name.clone(),
            width,
            interval,
            component,
        )?;

        let mut partitions = self.partitions.write();
        partitions.insert(name, TimeseriesPartition::SingleWasm(wasm_part.clone()));

        Ok(wasm_part)
    }

    #[cfg(feature = "wasm")]
    pub fn open_tiered_wasm<N: AsRef<str>, W: AsRef<[u8]>>(
        &self,
        name: N,
        component: W,
    ) -> Result<TieredWasmPartition, TimeseriesError> {
        let name = Arc::<str>::from(name.as_ref());
        let wasm_part = TieredWasmPartition::open_new(
            &self.keyspace,
            &self.meta_partition,
            self.wasm_engine.clone(),
            self.wasm_tiered_linker.clone(),
            name.clone(),
            component,
        )?;

        let mut partitions = self.partitions.write();
        partitions.insert(name, TimeseriesPartition::TieredWasm(wasm_part.clone()));

        Ok(wasm_part)
    }

    #[cfg(feature = "rune")]
    pub fn open_single_rune<N: AsRef<str>, S: AsRef<str>>(
        &self,
        name: N,
        width: NonZeroU16,
        interval: NonZeroU16,
        script: S,
    ) -> Result<SingleRunePartition, TimeseriesError> {
        let name = Arc::<str>::from(name.as_ref());

        let rune_part = SingleRunePartition::open_new(
            &self.keyspace,
            &self.meta_partition,
            self.rune_single_context.clone(),
            self.rune_single_runtime.clone(),
            name.clone(),
            width,
            interval,
            script,
        )?;

        let mut partitions = self.partitions.write();
        partitions.insert(name, TimeseriesPartition::SingleRune(rune_part.clone()));

        Ok(rune_part)
    }

    #[cfg(feature = "rune")]
    pub fn open_tiered_rune<N: AsRef<str>, S: AsRef<str>>(
        &self,
        name: N,
        script: S,
    ) -> Result<TieredRunePartition, TimeseriesError> {
        let name = Arc::<str>::from(name.as_ref());
        let rune_part = TieredRunePartition::open_new(
            &self.keyspace,
            &self.meta_partition,
            self.rune_tiered_context.clone(),
            self.rune_tiered_runtime.clone(),
            name.clone(),
            script,
        )?;

        let mut partitions = self.partitions.write();
        partitions.insert(name, TimeseriesPartition::TieredRune(rune_part.clone()));

        Ok(rune_part)
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
