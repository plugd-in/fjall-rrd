use std::{num::NonZeroU16, ops::Deref, sync::Arc};

use fjall::{Keyspace, Partition, PartitionCreateOptions, Slice};
use parking_lot::RwLock;
use rune::{
    Any, Context, ContextError, Module, Source, Sources, ToTypeHash, Unit, Vm,
    alloc::clone::TryClone,
    function,
    runtime::{Args, RuntimeContext},
};

use crate::{
    DataCell, TimeseriesError,
    format::{KeyType, Metadata, SeriesData, SingleData, SingleKey, SingleRuneMetadata},
    util::{TimeCell, TimeCellMut, timestamp_bucket},
};

#[derive(Clone)]
pub struct SingleRunePartition {
    name: Arc<str>,
    partition: Partition,
    metadata: SingleRuneMetadata,
    rune_runtime_context: Arc<RuntimeContext>,
    rune_unit: Arc<RwLock<Unit>>,
}

#[allow(unused)]
impl SingleRunePartition {
    pub(crate) fn new(
        name: Arc<str>,
        context: &Context,
        runtime: Arc<RuntimeContext>,
        metadata: SingleRuneMetadata,
        partition: Partition,
    ) -> Result<Self, TimeseriesError> {
        let mut sources = Sources::new();
        sources.insert(Source::memory(&metadata.script)?)?;

        let unit = rune::prepare(&mut sources).with_context(context).build()?;

        let rune_unit = Arc::new(RwLock::new(unit));

        Ok(Self {
            rune_runtime_context: runtime,
            name,
            metadata,
            partition,
            rune_unit,
        })
    }

    pub(crate) fn open_new<N: AsRef<str>, S: AsRef<str>>(
        keyspace: &Keyspace,
        meta: &Partition,
        context: &Context,
        runtime_context: Arc<RuntimeContext>,
        name: N,
        width: NonZeroU16,
        interval: NonZeroU16,
        script: S,
    ) -> Result<Self, TimeseriesError> {
        let name = name.as_ref();
        let script = script.as_ref();

        let name = Arc::<str>::from(name);

        if let Some(_meta) = meta.get(name.as_ref())? {
            return Err(TimeseriesError::PartitionExists);
        }

        let partition = keyspace.open_partition(&name, PartitionCreateOptions::default())?;

        let mut sources = Sources::new();
        sources.insert(Source::memory(script)?)?;

        let unit = rune::prepare(&mut sources).with_context(context).build()?;
        let rune_unit = Arc::new(RwLock::new(unit));

        let metadata = Metadata::SingleRune(SingleRuneMetadata {
            script: script.into(),
            width,
            interval,
        });

        meta.insert(name.as_ref(), Slice::try_from(&metadata)?)?;

        let Metadata::SingleRune(metadata) = metadata else {
            panic!("This should never happen.");
        };

        let partition = SingleRunePartition {
            name: name.clone(),
            rune_runtime_context: runtime_context.clone(),
            rune_unit: rune_unit,
            partition,
            metadata,
        };

        Ok(partition)
    }

    fn exec_rune_fn<H: ToTypeHash, A: Args>(
        &self,
        name: H,
        args: A,
    ) -> Result<(), TimeseriesError> {
        let unit = self.rune_unit.deref();
        let unit = {
            let unit = unit.read();
            Arc::new(unit.try_clone()?)
        };

        let mut vm = Vm::new(self.rune_runtime_context.clone(), unit);
        let mut output = vm.execute(name, args)?;

        output.complete().into_result()?;

        Ok(())
    }

    pub fn insert_metric<K>(&self, key: K, metric: DataCell) -> Result<(), TimeseriesError>
    where
        K: AsRef<[u8]>,
    {
        let user_key = key.as_ref();
        let width = self.metadata.width;

        let key = KeyType::Single(SingleKey {
            inner_key: user_key.into(),
        });
        let encoded_key = Slice::try_from(&key)?;

        let data = self.partition.get(encoded_key.as_ref())?;

        let data = if let Some(data) = data {
            let data = SeriesData::try_from(data)?;

            if let SeriesData::Single(data) = data {
                data
            } else {
                let data = SingleData {
                    custom_data: DataCell::Empty,
                    data: vec![DataCell::Empty; usize::from(width.get())].into_boxed_slice(),
                    last_timestamp: i64::MIN,
                    dirty: true,
                };

                data
            }
        } else {
            let data = SingleData {
                custom_data: DataCell::Empty,
                data: vec![DataCell::Empty; usize::from(width.get())].into_boxed_slice(),
                last_timestamp: i64::MIN,
                dirty: true,
            };

            data
        };

        let timestamp = chrono::Utc::now().timestamp();
        self.exec_rune_fn(
            ["handle_single_insert"],
            (SingleRuneContext {
                partition_name: self.name.clone(),
                inner_key: user_key.into(),
                partition: self.partition.clone(),
                metadata: self.metadata.clone(),
                data: data,
                metric,
                timestamp,
            },),
        )?;

        Ok(())
    }
}

#[derive(Any)]
pub(crate) struct SingleRuneContext {
    pub(crate) partition_name: Arc<str>,
    pub(crate) inner_key: Box<[u8]>,
    pub(crate) partition: Partition,
    pub(crate) metadata: SingleRuneMetadata,
    pub(crate) data: SingleData,
    pub(crate) timestamp: i64,
    pub(crate) metric: DataCell,
}

impl SingleRuneContext {
    #[function(keep)]
    fn width(&self) -> u16 {
        return self.metadata.width.get();
    }

    #[function]
    fn missed(&self) -> u16 {
        if self.pristine() {
            return 0;
        }

        let current_bucket = timestamp_bucket(self.timestamp, self.metadata.interval.into());
        let previous_bucket =
            timestamp_bucket(self.data.last_timestamp, self.metadata.interval.into());

        u16::try_from((current_bucket - previous_bucket).clamp(0, self.width().into()))
            .expect("Within u16, by clamp.")
            .saturating_sub(1)
    }

    #[function]
    fn write_multi_metric(&mut self, back: u16, metric: DataCell) {
        let mut back = back;

        let idx = self
            .data
            .data
            .cell_idx(self.timestamp, self.metadata.interval.into());

        {
            let before = self.data.data.get_mut(0..=usize::from(idx));

            if let Some(before) = before {
                for cell in before.into_iter().rev() {
                    if !(back > 0) {
                        break;
                    }

                    if crate::DataCell::Empty.ne(cell) {
                        *cell = metric.clone();
                    }

                    back -= 1;
                }
            }
        }

        {
            let after = self.data.data.get_mut(usize::from(idx)..);

            if let Some(after) = after {
                for cell in after.into_iter().skip(1).rev() {
                    if !(back > 0) {
                        break;
                    }

                    if crate::DataCell::Empty.ne(cell) {
                        *cell = metric.clone();
                    }

                    back -= 1;
                }
            }
        }

        self.data.dirty = true;
    }

    #[function]
    fn metric(&self) -> DataCell {
        self.metric.clone()
    }

    #[function(keep)]
    fn pristine(&self) -> bool {
        self.data.last_timestamp == i64::MIN && self.data.dirty == false
    }

    #[function]
    fn commit(&self) -> Result<(), TimeseriesError> {
        if self.data.dirty {
            self.partition.insert(
                Slice::try_from(KeyType::Single(SingleKey {
                    inner_key: self.inner_key.clone(),
                }))?,
                Slice::try_from(SeriesData::Single(self.data.clone()))?,
            )?;
        }

        Ok(())
    }

    #[function]
    fn interval(&mut self) -> u16 {
        self.metadata.interval.get()
    }

    #[function]
    fn write_custom(&mut self, data: DataCell) {
        self.data.custom_data = data;
        self.data.dirty = true;
        self.data.last_timestamp = self.timestamp;
    }

    #[function]
    fn get_custom(&mut self) -> DataCell {
        self.data.custom_data.clone()
    }

    #[function]
    /// Write either the verbatim passed in metric or
    /// a custom metric, perhaps after some processing.
    fn write_metric(&mut self, metric: Option<DataCell>) {
        let metric = if let Some(metric) = metric {
            metric
        } else {
            self.metric.clone()
        };

        let current_cell = self
            .data
            .data
            .get_cell_mut(self.timestamp, self.metadata.interval.into());

        *current_cell = metric;
        self.data.dirty = true;
        self.data.last_timestamp = self.timestamp;
    }

    #[function(keep)]
    /// Clear the cells between the current cell
    /// and the last modified cell.
    fn clear_misses(&mut self) {
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

        let mut bucket_offset = (current_bucket - previous_bucket).clamp(0, self.width().into());

        {
            let before = self.data.data.get_mut(0..=usize::from(idx));

            if let Some(before) = before {
                for cell in before.into_iter().rev() {
                    if !(bucket_offset > 0) {
                        break;
                    }

                    if DataCell::Empty.ne(cell) {
                        dirty = true;
                        *cell = DataCell::Empty;
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

                    if DataCell::Empty.ne(cell) {
                        dirty = true;
                        *cell = DataCell::Empty;
                    }

                    bucket_offset -= 1;
                }
            }
        }

        if !self.data.dirty {
            self.data.dirty = dirty;
        }
    }

    #[function(keep)]
    /// Get the partition name.
    fn partition_name(&self) -> String {
        self.partition_name.to_string()
    }

    #[function(keep)]
    /// Look back from the current cell (including the current cell)
    /// by the specified number of cells.
    fn look_back(&self, n_cells: u16) -> Vec<DataCell> {
        let interval = self.metadata.interval;
        let mut n_cells = n_cells.min(self.metadata.width.get());

        let mut cells = Vec::new();
        let cell = self.data.data.cell_idx(self.timestamp, interval.into());

        let (idx, data) = (cell, self.data.data.as_ref());

        for cell in (&data[0..=usize::from(idx)]).iter().rev() {
            if !(n_cells > 0) {
                break;
            }

            cells.push(cell.clone());

            n_cells -= 1;
        }

        for cell in (&data[usize::from(idx)..]).iter().skip(1).rev() {
            if !(n_cells > 0) {
                break;
            }

            cells.push(cell.clone());

            n_cells -= 1;
        }

        cells
    }
}

pub(crate) fn module() -> Result<Module, ContextError> {
    let mut module = Module::new();
    module.ty::<SingleRuneContext>()?;
    module.ty::<DataCell>()?;
    module.function_meta(SingleRuneContext::look_back__meta)?;
    module.function_meta(SingleRuneContext::partition_name__meta)?;
    module.function_meta(SingleRuneContext::pristine__meta)?;
    module.function_meta(SingleRuneContext::clear_misses__meta)?;
    module.function_meta(SingleRuneContext::commit)?;
    module.function_meta(SingleRuneContext::write_metric)?;
    module.function_meta(SingleRuneContext::metric)?;
    module.function_meta(SingleRuneContext::write_custom)?;
    module.function_meta(SingleRuneContext::get_custom)?;
    module.function_meta(SingleRuneContext::width__meta)?;
    module.function_meta(SingleRuneContext::interval)?;
    module.function_meta(SingleRuneContext::missed)?;
    module.function_meta(DataCell::custom)?;

    Ok(module)
}
