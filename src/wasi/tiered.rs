use std::{
    num::{NonZeroU16, NonZeroU32},
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::{
    TimeseriesError,
    format::{KeyType, Metadata, SeriesData, TieredData, TieredKey, TieredWasmMetadata},
    util::TimeCellMut,
    wasi::WasiStateMaybeUninit,
};

use super::impl_data_cell;
use fjall::{Keyspace, Partition, PartitionCreateOptions, Slice};
use itertools::Itertools;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use wasmtime::{
    Engine, Store,
    component::{Component, Linker, bindgen},
};

bindgen!("fjall-rrd:hooks/tiered" in "wit/fjall-rrd");
impl_data_cell!();

pub(crate) struct TieredComponent {
    pub(crate) partition_name: Arc<str>,
    pub(crate) inner_key: Box<[u8]>,
    pub(crate) partition: Partition,
    pub(crate) tiers: Vec<TieredData>,
    pub(crate) nth_tier: Option<u16>,
    pub(crate) timestamp: i64,
    pub(crate) cumulative_interval: u32,
    pub(crate) metric: crate::DataCell,
}

impl fjall_rrd::hooks::data::Host for TieredComponent {}

impl TieredImports for TieredComponent {
    /// Look back at the previous `how-far` numbers
    /// of items in the current tier.
    fn look_back_current(&mut self, how_far: u16) -> Vec<DataCell> {
        let Some(current_tier) = self.nth_tier else {
            return Vec::new();
        };

        let Some(current_tier) = self.tiers.get(usize::from(current_tier)) else {
            return Vec::new();
        };

        current_tier
            .look_back(self.timestamp, self.cumulative_interval, how_far)
            .into_iter()
            .map(Into::into)
            .collect_vec()
    }

    /// Look back at the previous `how-far` numbers
    /// of items in the previous tier.
    ///
    /// *Note:* Always returns an empty collection for
    /// the first (0th) tier.
    fn look_back_previous(&mut self, how_far: u16) -> Vec<DataCell> {
        let Some(previous_tier) = self.nth_tier.and_then(|nth_tier| nth_tier.checked_sub(1)) else {
            return Vec::new();
        };

        let Some(previous_tier) = self.tiers.get(usize::from(previous_tier)) else {
            return Vec::new();
        };

        let cumulative_interval = if self.cumulative_interval == 0
            || self.cumulative_interval == u32::from(previous_tier.interval.get())
        {
            0u32
        } else {
            self.cumulative_interval / u32::from(previous_tier.interval.get())
        };

        previous_tier
            .look_back(self.timestamp, cumulative_interval, how_far)
            .into_iter()
            .map(Into::into)
            .collect_vec()
    }

    /// Get the name of the current timeseries
    /// partition this hook is running for.
    fn partition_name(&mut self) -> String {
        self.partition_name.to_string()
    }

    /// Clear past misses in the current tier.
    fn clear_misses(&mut self) {
        let Some(current_tier) = self.nth_tier else {
            return;
        };

        let Some(current_tier) = self.tiers.get_mut(usize::from(current_tier)) else {
            return;
        };

        current_tier.clear_misses(self.timestamp, self.cumulative_interval);
    }

    /// Get whether there are no storage tiers
    /// created for this item.
    fn empty(&mut self) -> bool {
        self.nth_tier.is_none()
    }

    /// Get how many tiers there for the item.
    fn tier_count(&mut self) -> u16 {
        u16::try_from(self.tiers.len()).expect("restricted tiers count")
    }

    /// Get the current tier being inserted into,
    /// starting at 0. If there are no tiers, then
    /// None is returned.
    fn current_tier(&mut self) -> Option<u16> {
        self.nth_tier
    }

    /// Create a new tier.
    ///
    /// The width defines how many items will fit
    /// in the RRD, and the interval specifies
    /// the period between inserts, either in seconds
    /// if it's the first tier or in terms of the number
    /// of cells in the lower tier.
    ///
    /// *Note:* This does not commit the tier until
    /// `commit-current` is called at the appropriate
    /// time. Likewise, this does no change the current
    /// tier to the created tier. You have to wait until
    /// subsequent calls of the `handle-metric` hook
    /// to modify and commit the created tier to storage.
    ///
    /// *Warn:* Width and interval *must* be non-zero or
    /// this will silently fail.
    fn create_tier(&mut self, width: u16, interval: u16) {
        let Some(width) = NonZeroU16::new(width) else {
            return;
        };
        let Some(interval) = NonZeroU16::new(interval) else {
            return;
        };

        let Ok(new_tier) = TieredData::new_empty(width.get(), interval.get()) else {
            return;
        };

        self.tiers.push(new_tier);
    }

    /// Commit the current tier to storage.
    ///
    /// *Note:* The data is only committed if the current
    /// data has been changed (e.g. through `write-metric`).
    fn commit_current(&mut self) {
        let Some(nth_tier) = self.nth_tier else {
            return;
        };

        let Some(current_tier) = self.tiers.get(usize::from(nth_tier)) else {
            return;
        };

        if current_tier.dirty {
            if let Err(e) = self.partition.insert(
                KeyType::Tier(TieredKey {
                    inner_key: self.inner_key.as_ref().into(),
                    nth_tier: nth_tier,
                }),
                SeriesData::Tiered(current_tier.clone()),
            ) {
                tracing::error!(?e, "Error inserting into timeseries database...");
            }
        }
    }

    /// Write a metric into the current tier in the cell
    /// suitable for the current timestamp and cumulative
    /// interval. This is a no-op for items with no tiers.
    fn write_metric(&mut self, metric: DataCell) {
        let Some(current_tier) = self.nth_tier else {
            return;
        };

        let Some(current_tier) = self.tiers.get_mut(usize::from(current_tier)) else {
            return;
        };

        let metric = crate::DataCell::from(metric);

        let total_interval = if self.cumulative_interval == 0 {
            NonZeroU32::from(current_tier.interval)
        } else {
            NonZeroU32::new(self.cumulative_interval * u32::from(current_tier.interval.get()))
                .expect("at least zero, by math")
        };

        let current_cell = current_tier
            .data
            .get_cell_mut(self.timestamp, total_interval);

        *current_cell = metric;
        current_tier.last_timestamp = self.timestamp;
        current_tier.dirty = true;
    }

    /// Get the metric being inserted.
    fn metric(&mut self) -> DataCell {
        self.metric.clone().into()
    }

    /// Write custom data into the item.
    ///
    /// This differs from a metric and can be
    /// used to store non-metric data across calls.
    /// This can be useful for calculating and storing
    /// deltas, for example.
    fn write_custom_current(&mut self, data: DataCell) {
        let Some(current_tier) = self.nth_tier else {
            return;
        };

        let Some(current_tier) = self.tiers.get_mut(usize::from(current_tier)) else {
            return;
        };

        let data = crate::DataCell::from(data);

        current_tier.last_timestamp = self.timestamp;
        current_tier.custom_data = data;
        current_tier.dirty = true;
    }

    /// Get custom data for the item.
    ///
    /// This differs from a metric and can be
    /// used to store non-metric data across calls.
    /// This can be useful for calculating and storing
    /// deltas, for example.
    fn get_custom_current(&mut self) -> Option<DataCell> {
        let Some(current_tier) = self.nth_tier else {
            return None;
        };

        let Some(current_tier) = self.tiers.get(usize::from(current_tier)) else {
            return None;
        };

        Some(current_tier.custom_data.clone().into())
    }
}

struct LockedPersistentTieredComponent<'a> {
    component: RwLockReadGuard<'a, Tiered>,
    store: RwLockWriteGuard<'a, Store<WasiStateMaybeUninit<TieredComponent>>>,
}

impl<'a> LockedPersistentTieredComponent<'a> {
    fn set_state(&mut self, state: TieredComponent) {
        let maybe_state = self.store.data_mut();

        maybe_state.state = Some(state);
    }

    fn handle_metric<F>(&mut self, func: F) -> Result<(), TimeseriesError>
    where
        F: FnOnce(&mut TieredComponent) -> (),
    {
        if let Some(state) = self.store.data_mut().state.as_mut() {
            func(state);
        } else {
            return Err(TimeseriesError::StateUninit);
        };

        self.store
            .set_fuel(250000)
            .map_err(TimeseriesError::WebAssembly)?;

        self.component
            .call_handle_tiered_metric(self.store.deref_mut())
            .map_err(TimeseriesError::WebAssembly)?;

        Ok(())
    }

    fn state(&self) -> Result<&TieredComponent, TimeseriesError> {
        self.store
            .data()
            .state
            .as_ref()
            .ok_or(TimeseriesError::StateUninit)
    }

    fn state_mut(&mut self) -> Result<&mut TieredComponent, TimeseriesError> {
        self.store
            .data_mut()
            .state
            .as_mut()
            .ok_or(TimeseriesError::StateUninit)
    }
}

#[derive(Clone)]
struct PersistentTieredComponent {
    component: Arc<RwLock<Tiered>>,
    store: Arc<RwLock<Store<WasiStateMaybeUninit<TieredComponent>>>>,
}

impl PersistentTieredComponent {
    fn lock(&self) -> LockedPersistentTieredComponent<'_> {
        let component = self.component.deref();
        let store = self.store.deref();

        LockedPersistentTieredComponent {
            component: component.read(),
            store: store.write(),
        }
    }
}

impl From<(Tiered, Store<WasiStateMaybeUninit<TieredComponent>>)> for PersistentTieredComponent {
    fn from(value: (Tiered, Store<WasiStateMaybeUninit<TieredComponent>>)) -> Self {
        let (component, store) = value;

        Self {
            component: Arc::new(RwLock::new(component)),
            store: Arc::new(RwLock::new(store)),
        }
    }
}

#[derive(Clone)]
pub struct TieredWasmPartition {
    name: Arc<str>,
    partition: Partition,
    metadata: TieredWasmMetadata,
    engine: Engine,
    linker: Linker<WasiStateMaybeUninit<TieredComponent>>,
    component: PersistentTieredComponent,
}

#[allow(unused)]
impl TieredWasmPartition {
    pub(crate) fn new(
        name: Arc<str>,
        engine: Engine,
        linker: Linker<WasiStateMaybeUninit<TieredComponent>>,
        partition: Partition,
        metadata: TieredWasmMetadata,
    ) -> Result<Self, TimeseriesError> {
        let compiled_component =
            Component::new(&engine, &metadata.component).map_err(TimeseriesError::WebAssembly)?;

        let components_with_imports = linker
            .instantiate_pre(&compiled_component)
            .map_err(TimeseriesError::WebAssembly)?;

        let component_with_exports =
            TieredPre::new(components_with_imports).map_err(TimeseriesError::WebAssembly)?;

        let mut store = Store::new(&engine, WasiStateMaybeUninit::default());

        let tiered_component = component_with_exports
            .instantiate(&mut store)
            .map_err(TimeseriesError::WebAssembly)?;

        Ok(Self {
            component: PersistentTieredComponent::from((tiered_component, store)),
            linker: linker.clone(),
            name,
            metadata,
            partition,
            engine,
        })
    }

    pub(crate) fn open_new<N: AsRef<str>, W: AsRef<[u8]>>(
        keyspace: &Keyspace,
        meta: &Partition,
        engine: Engine,
        linker: Linker<WasiStateMaybeUninit<TieredComponent>>,
        name: N,
        component: W,
    ) -> Result<Self, TimeseriesError> {
        let name = name.as_ref();
        let component = component.as_ref();

        let name = Arc::<str>::from(name);

        if let Some(_meta) = meta.get(name.as_ref())? {
            return Err(TimeseriesError::PartitionExists);
        }

        let partition = keyspace.open_partition(&name, PartitionCreateOptions::default())?;

        let compiled_component =
            Component::new(&engine, component).map_err(TimeseriesError::WebAssembly)?;

        let components_with_imports = linker
            .instantiate_pre(&compiled_component)
            .map_err(TimeseriesError::WebAssembly)?;

        let component_with_exports =
            TieredPre::new(components_with_imports).map_err(TimeseriesError::WebAssembly)?;

        let mut store = Store::new(&engine, WasiStateMaybeUninit::default());

        let tiered_component = component_with_exports
            .instantiate(&mut store)
            .map_err(TimeseriesError::WebAssembly)?;

        let metadata = Metadata::TieredWasm(TieredWasmMetadata {
            component: component.into(),
        });

        meta.insert(name.as_ref(), &metadata)?;

        let Metadata::TieredWasm(metadata) = metadata else {
            panic!("This should never happen.");
        };

        let partition = TieredWasmPartition {
            name: name.clone(),
            component: PersistentTieredComponent::from((tiered_component, store)),
            linker: linker.clone(),
            engine,
            partition,
            metadata,
        };

        Ok(partition)
    }

    pub fn update_component<W>(&self, component: W) -> Result<(), TimeseriesError>
    where
        W: AsRef<[u8]>,
    {
        let mut store = self.component.store.write();
        let mut current_component = self.component.component.write();

        let compiled_component = Component::new(&self.engine, component.as_ref())
            .map_err(TimeseriesError::WebAssembly)?;

        let components_with_imports = self
            .linker
            .instantiate_pre(&compiled_component)
            .map_err(TimeseriesError::WebAssembly)?;

        let component_with_exports =
            TieredPre::new(components_with_imports).map_err(TimeseriesError::WebAssembly)?;

        let tiered_component = component_with_exports
            .instantiate(store.deref_mut())
            .map_err(TimeseriesError::WebAssembly)?;

        *current_component = tiered_component;

        Ok(())
    }

    pub fn insert_metric<K>(&self, key: K, metric: crate::DataCell) -> Result<(), TimeseriesError>
    where
        K: AsRef<[u8]>,
    {
        let user_key = key.as_ref();

        let key = KeyType::Tier(TieredKey {
            inner_key: user_key.into(),
            nth_tier: 0,
        });
        let encoded_key = Slice::from(&key);

        let tiers: Vec<TieredData> = self
            .partition
            .range(encoded_key.as_ref()..)
            .map_ok(|(k, v)| (KeyType::from(k), SeriesData::from(v)))
            .take_while(|read_result| {
                read_result
                    .as_ref()
                    .map(|(k, _)| {
                        if let Some(tiered) = k.as_tiered() {
                            tiered.inner_key.deref().eq(user_key)
                        } else {
                            false
                        }
                    })
                    .unwrap_or(true)
            })
            .filter_map_ok(|(_, v)| v.into_tiered())
            .try_collect()?;

        let timestamp = chrono::Utc::now().timestamp();
        let user_key = Box::<[u8]>::from(user_key);

        let tiers = tiers;
        let metric = metric;

        let mut n: u16 = 0;
        let component_state = TieredComponent {
            partition_name: self.name.clone(),
            inner_key: user_key,
            partition: self.partition.clone(),
            tiers: tiers,
            nth_tier: None,
            metric: metric,
            cumulative_interval: 0,
            timestamp,
        };

        let is_empty = component_state.tiers.is_empty();
        let mut component = self.component.lock();

        component.set_state(component_state);

        if is_empty {
            component.handle_metric(|_| {})?;
        }

        loop {
            let tiers_len = component.state()?.tiers.len();

            if usize::from(n) >= tiers_len {
                break;
            }

            component.handle_metric(|state| {
                state.nth_tier = Some(n);
            })?;

            let state = component.state_mut()?;

            if let Some(interval) = state.tiers.get(usize::from(n)).map(|tier| tier.interval) {
                if state.cumulative_interval == 0 {
                    state.cumulative_interval = u32::from(interval.get());
                } else {
                    state.cumulative_interval *= u32::from(interval.get());
                }
            }

            n += 1;
        }

        Ok(())
    }
}
