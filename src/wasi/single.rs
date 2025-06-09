use std::{
    num::NonZeroU16,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::{
    TimeseriesError,
    format::{KeyType, Metadata, SeriesData, SingleData, SingleKey, SingleWasmMetadata},
    util::{TimeCell, TimeCellMut, timestamp_bucket},
    wasi::WasiStateMaybeUninit,
};
use fjall::{Keyspace, Partition, PartitionCreateOptions, Slice};
use parking_lot::RwLock;
use wasmtime::{
    Engine, Store,
    component::{Component, Linker, bindgen},
};

use super::impl_data_cell;

bindgen!({
    path: "wit/fjall-rrd",
    world: "fjall-rrd:hooks/single",
});
impl_data_cell!();

pub(crate) struct SingleComponent {
    pub(crate) partition_name: Arc<str>,
    pub(crate) inner_key: Box<[u8]>,
    pub(crate) partition: Partition,
    pub(crate) metadata: SingleWasmMetadata,
    pub(crate) data: SingleData,
    pub(crate) timestamp: i64,
    pub(crate) metric: crate::DataCell,
}

impl fjall_rrd::hooks::data::Host for SingleComponent {}

impl SingleImports for SingleComponent {
    /// Get the interval.
    ///
    /// *Note:* This should never return zero.
    fn interval(&mut self) -> u16 {
        self.metadata.interval.get()
    }

    /// Get the width of the round-robin structure.
    ///
    /// *Note:* This should never return zero.
    fn width(&mut self) -> u16 {
        self.metadata.width.get()
    }

    /// Get the name of the current timeseries partition
    /// this hook is running for.
    fn partition_name(&mut self) -> String {
        self.partition_name.as_ref().to_string()
    }

    /// Get the metric being inserted.
    fn metric(&mut self) -> DataCell {
        self.metric.clone().into()
    }

    /// Get whether the storage is untouched and
    /// unvisited for the item.
    fn pristine(&mut self) -> bool {
        self.data.last_timestamp == i64::MIN && self.data.dirty == false
    }

    /// Clear past misses from the last timestamp to now.
    fn clear_misses(&mut self) -> () {
        if self.pristine() {
            return;
        }

        let mut dirty = false;

        let current_bucket = timestamp_bucket(self.timestamp, self.metadata.interval.into());
        let previous_bucket =
            timestamp_bucket(self.data.last_timestamp, self.metadata.interval.into());

        let idx = self
            .data
            .data
            .cell_idx(self.timestamp, self.metadata.interval.into());
        let mut bucket_offset =
            (current_bucket - previous_bucket).clamp(0, self.metadata.width.get().into());

        {
            let before = self.data.data.get_mut(0..=usize::from(idx));

            if let Some(before) = before {
                for cell in before.into_iter().rev() {
                    if !(bucket_offset > 0) {
                        break;
                    }

                    if crate::DataCell::Empty.ne(cell) {
                        dirty = true;
                        *cell = crate::DataCell::Empty;
                    }

                    bucket_offset -= 1;
                }
            }
        }

        {
            let after = self.data.data.get_mut(usize::from(idx)..);

            if let Some(after) = after {
                for cell in after.into_iter().skip(1).rev() {
                    if !(bucket_offset > 0) {
                        break;
                    }

                    if crate::DataCell::Empty.ne(cell) {
                        dirty = true;
                        *cell = crate::DataCell::Empty;
                    }

                    bucket_offset -= 1;
                }
            }
        }

        if !self.data.dirty {
            self.data.dirty = dirty;
        }
    }

    /// Commit any changes to storage.
    fn commit(&mut self) -> () {
        if self.data.dirty {
            self.data.last_timestamp = self.timestamp;

            if let Err(e) = self.partition.insert(
                KeyType::Single(SingleKey {
                    inner_key: self.inner_key.clone(),
                }),
                SeriesData::Single(self.data.clone()),
            ) {
                tracing::error!(?e, "Error inserting into timeseries database...");
            }
        }
    }

    /// Write the given metric into the Round-robin
    /// structure at the cell for the current bucket.
    fn write_metric(&mut self, metric: DataCell) -> () {
        let metric = crate::DataCell::from(metric);

        let current_cell = self
            .data
            .data
            .get_cell_mut(self.timestamp, self.metadata.interval.into());

        *current_cell = metric;
        self.data.dirty = true;
    }

    /// Look back at the previous `how-far` numbers of items.
    fn look_back(&mut self, how_far: u16) -> Vec<DataCell> {
        let interval = self.metadata.interval;
        let mut n_cells = how_far.min(self.metadata.width.get());

        let mut cells = Vec::new();
        let cell = self.data.data.cell_idx(self.timestamp, interval.into());

        let (idx, data) = (cell, self.data.data.as_ref());

        for cell in (&data[0..=usize::from(idx)]).iter().rev() {
            if !(n_cells > 0) {
                break;
            }

            cells.push(cell.clone().into());

            n_cells -= 1;
        }

        for cell in (&data[usize::from(idx)..]).iter().skip(1).rev() {
            if !(n_cells > 0) {
                break;
            }

            cells.push(cell.clone().into());

            n_cells -= 1;
        }

        cells
    }

    /// Write custom data into the item.
    ///
    /// This differs from a metric and can be
    /// used to store non-metric data across calls.
    /// This can be useful for calculating and storing
    /// deltas, for example.
    fn write_custom(&mut self, data: DataCell) {
        let data = crate::DataCell::from(data);

        self.data.custom_data = data;
        self.data.dirty = true;
    }

    /// Get custom data for the item.
    ///
    /// This differs from a metric and can be
    /// used to store non-metric data across calls.
    /// This can be useful for calculating and storing
    /// deltas, for example.
    fn get_custom(&mut self) -> DataCell {
        self.data.custom_data.clone().into()
    }
}

#[derive(Clone)]
struct PersistentSingleComponent {
    component: Arc<RwLock<Single>>,
    store: Arc<RwLock<Store<WasiStateMaybeUninit<SingleComponent>>>>,
}

impl From<(Single, Store<WasiStateMaybeUninit<SingleComponent>>)> for PersistentSingleComponent {
    fn from(value: (Single, Store<WasiStateMaybeUninit<SingleComponent>>)) -> Self {
        let (component, store) = value;

        Self {
            component: Arc::new(RwLock::new(component)),
            store: Arc::new(RwLock::new(store)),
        }
    }
}

impl PersistentSingleComponent {
    fn handle_insert(&self, state: SingleComponent) -> Result<(), TimeseriesError> {
        let component = self.component.deref();
        let component = component.read();
        let store = self.store.deref();
        let mut store = store.write();

        {
            let maybe_state = store.data_mut();
            maybe_state.state = Some(state);
        }

        store
            .set_fuel(250000)
            .map_err(TimeseriesError::WebAssembly)?;

        component
            .call_handle_single_metric(store.deref_mut())
            .map_err(TimeseriesError::WebAssembly)
    }
}

#[derive(Clone)]
pub struct SingleWasmPartition {
    name: Arc<str>,
    partition: Partition,
    metadata: SingleWasmMetadata,
    engine: Engine,
    linker: Linker<WasiStateMaybeUninit<SingleComponent>>,
    component: PersistentSingleComponent,
}

#[allow(unused)]
impl SingleWasmPartition {
    pub(crate) fn new(
        name: Arc<str>,
        engine: Engine,
        linker: Linker<WasiStateMaybeUninit<SingleComponent>>,
        partition: Partition,
        metadata: SingleWasmMetadata,
    ) -> Result<Self, TimeseriesError> {
        let compiled_component =
            Component::new(&engine, &metadata.component).map_err(TimeseriesError::WebAssembly)?;

        let components_with_imports = linker
            .instantiate_pre(&compiled_component)
            .map_err(TimeseriesError::WebAssembly)?;

        let component_with_exports =
            SinglePre::new(components_with_imports).map_err(TimeseriesError::WebAssembly)?;

        let mut store = Store::new(&engine, WasiStateMaybeUninit::default());

        let single_component = component_with_exports
            .instantiate(&mut store)
            .map_err(TimeseriesError::WebAssembly)?;

        Ok(Self {
            component: PersistentSingleComponent::from((single_component, store)),
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
        linker: Linker<WasiStateMaybeUninit<SingleComponent>>,
        name: N,
        width: NonZeroU16,
        interval: NonZeroU16,
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
            SinglePre::new(components_with_imports).map_err(TimeseriesError::WebAssembly)?;

        let mut store = Store::new(&engine, WasiStateMaybeUninit::default());

        let single_component = component_with_exports
            .instantiate(&mut store)
            .map_err(TimeseriesError::WebAssembly)?;

        let metadata = Metadata::SingleWasm(SingleWasmMetadata {
            component: component.into(),
            width,
            interval,
        });

        meta.insert(name.as_ref(), &metadata)?;

        let Metadata::SingleWasm(metadata) = metadata else {
            panic!("This should never happen.");
        };

        let partition = SingleWasmPartition {
            name: name.clone(),
            component: PersistentSingleComponent::from((single_component, store)),
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
            SinglePre::new(components_with_imports).map_err(TimeseriesError::WebAssembly)?;

        let single_component = component_with_exports
            .instantiate(store.deref_mut())
            .map_err(TimeseriesError::WebAssembly)?;

        *current_component = single_component;

        Ok(())
    }

    pub fn insert_metric<K>(&self, key: K, metric: crate::DataCell) -> Result<(), TimeseriesError>
    where
        K: AsRef<[u8]>,
    {
        let user_key = key.as_ref();
        let width = self.metadata.width;

        let key = KeyType::Single(SingleKey {
            inner_key: user_key.into(),
        });
        let encoded_key = Slice::from(&key);

        let data = self.partition.get(encoded_key.as_ref())?;

        let data = if let Some(data) = data {
            let data = SeriesData::from(data);

            if let SeriesData::Single(data) = data {
                data
            } else {
                let data = SingleData {
                    custom_data: crate::DataCell::Empty,
                    data: vec![crate::DataCell::Empty; usize::from(width.get())].into_boxed_slice(),
                    last_timestamp: i64::MIN,
                    dirty: true,
                };

                data
            }
        } else {
            let data = SingleData {
                custom_data: crate::DataCell::Empty,
                data: vec![crate::DataCell::Empty; usize::from(width.get())].into_boxed_slice(),
                last_timestamp: i64::MIN,
                dirty: true,
            };

            data
        };

        let timestamp = chrono::Utc::now().timestamp();

        let single_component = SingleComponent {
            partition_name: self.name.clone(),
            inner_key: user_key.into(),
            partition: self.partition.clone(),
            metadata: self.metadata.clone(),
            data: data,
            metric,
            timestamp,
        };

        self.component.handle_insert(single_component)?;

        Ok(())
    }
}
