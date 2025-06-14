use std::{cell::RefCell, num::NonZeroU16, ops::Deref, rc::Rc, sync::Arc};

use fjall::{Keyspace, Partition, PartitionCreateOptions, Slice};
use parking_lot::RwLock;
use rune::{
    self, Any, Context, ContextError, Module, Source, Sources, ToTypeHash, Unit, Vm, function,
    runtime::{Args, RuntimeContext},
};

use crate::{
    DataCell, TimeseriesError,
    format::{KeyType, Metadata, SeriesData, SingleData, SingleKey, SingleRuneMetadata},
    util::{TimeCell, TimeCellMut, timestamp_bucket},
};

#[derive(Clone)]
/// Single RRD structure that uses the [Rune](rune) scripting language.
pub struct SingleRunePartition {
    name: Arc<str>,
    partition: Partition,
    metadata: SingleRuneMetadata,
    rune_context: Arc<Context>,
    rune_runtime_context: Arc<RuntimeContext>,
    rune_unit: Arc<RwLock<Arc<Unit>>>,
}

#[allow(unused)]
impl SingleRunePartition {
    pub(crate) fn new(
        name: Arc<str>,
        context: Arc<Context>,
        runtime: Arc<RuntimeContext>,
        metadata: SingleRuneMetadata,
        partition: Partition,
    ) -> Result<Self, TimeseriesError> {
        let mut sources = Sources::new();
        sources.insert(Source::memory(&metadata.script)?)?;

        let unit = rune::prepare(&mut sources)
            .with_context(context.deref())
            .build()?;
        let rune_unit = Arc::new(RwLock::new(Arc::new(unit)));

        Ok(Self {
            rune_context: context,
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
        context: Arc<Context>,
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

        let unit = rune::prepare(&mut sources)
            .with_context(context.deref())
            .build()?;

        let rune_unit = Arc::new(RwLock::new(Arc::new(unit)));

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
            rune_context: context,
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

            unit.deref().clone()
        };

        let mut vm = Vm::new(self.rune_runtime_context.clone(), unit);
        let mut output = vm.execute(name, args)?;

        output.complete().into_result()?;

        Ok(())
    }

    pub fn update_script<S>(&self, script: S) -> Result<(), TimeseriesError>
    where
        S: AsRef<str>,
    {
        let mut unit = self.rune_unit.write();

        let mut sources = Sources::new();
        sources.insert(Source::memory(script)?)?;

        let new_unit = rune::prepare(&mut sources)
            .with_context(self.rune_context.deref())
            .build()?;

        *unit = Arc::new(new_unit);

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
        let data = Rc::new(RefCell::new(Some(data)));
        let context = SingleRuneContext {
            partition_name: self.name.clone(),
            inner_key: user_key.into(),
            partition: self.partition.clone(),
            metadata: self.metadata.clone(),
            data: data.clone(),
            metric,
            timestamp,
        };

        self.exec_rune_fn(["handle_single_insert"], (context,))?;

        let mut data = data.borrow_mut();
        let mut data = data.take().ok_or(TimeseriesError::StateUninit)?;

        if data.dirty {
            data.last_timestamp = timestamp;

            self.partition.insert(
                Slice::try_from(KeyType::Single(SingleKey {
                    inner_key: user_key.into(),
                }))?,
                Slice::try_from(SeriesData::Single(data))?,
            )?;
        }

        Ok(())
    }
}

#[derive(Any)]
pub(crate) struct SingleRuneContext {
    pub(crate) partition_name: Arc<str>,
    pub(crate) inner_key: Box<[u8]>,
    pub(crate) partition: Partition,
    pub(crate) metadata: SingleRuneMetadata,
    // Rc<RefCell<Option>> so we can Option::take it
    // after we are done with it.
    pub(crate) data: Rc<RefCell<Option<SingleData>>>,
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

        let data = self.data.borrow();
        let data = data.as_ref().expect("Never uninit.");

        let current_bucket = timestamp_bucket(self.timestamp, self.metadata.interval.into());
        let previous_bucket = timestamp_bucket(data.last_timestamp, self.metadata.interval.into());

        u16::try_from((current_bucket - previous_bucket).clamp(0, self.width().into()))
            .expect("Within u16, by clamp.")
            .saturating_sub(1)
    }

    #[function]
    fn write_multi_metric(&mut self, back: u16, metric: DataCell) {
        let mut back = back;

        let mut data = self.data.borrow_mut();
        let data = data.as_mut().expect("Never uninit.");

        let idx = data
            .data
            .cell_idx(self.timestamp, self.metadata.interval.into());

        {
            let before = data.data.get_mut(0..=usize::from(idx));

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
            let after = data.data.get_mut(usize::from(idx)..);

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

        data.dirty = true;
    }

    #[function]
    fn metric(&self) -> DataCell {
        self.metric.clone()
    }

    #[function(keep)]
    fn pristine(&self) -> bool {
        let data = self.data.borrow();
        let data = data.as_ref().expect("Never uninit.");

        data.last_timestamp == i64::MIN && data.dirty == false
    }

    #[function]
    fn interval(&mut self) -> u16 {
        self.metadata.interval.get()
    }

    #[function]
    fn write_custom(&mut self, custom: DataCell) {
        let mut data = self.data.borrow_mut();
        let data = data.as_mut().expect("Never uninit.");

        data.custom_data = custom;
        data.dirty = true;
    }

    #[function]
    fn get_custom(&mut self) -> DataCell {
        let data = self.data.borrow();
        let data = data.as_ref().expect("Never uninit.");

        data.custom_data.clone()
    }

    #[function]
    /// Write the given metric into the Round-robin structure.
    fn write_metric(&mut self, metric: DataCell) {
        let mut data = self.data.borrow_mut();
        let data = data.as_mut().expect("Never uninit.");

        let current_cell = data
            .data
            .get_cell_mut(self.timestamp, self.metadata.interval.into());

        *current_cell = metric;
        data.dirty = true;
    }

    #[function(keep)]
    /// Clear the cells between the current cell
    /// and the last modified cell.
    fn clear_misses(&mut self) {
        if self.pristine() {
            return;
        }

        let mut dirty = false;

        let mut data = self.data.borrow_mut();
        let data = data.as_mut().expect("Never uninit.");

        let current_bucket = timestamp_bucket(self.timestamp, self.metadata.interval.into());
        let previous_bucket = timestamp_bucket(data.last_timestamp, self.metadata.interval.into());

        let idx = data
            .data
            .cell_idx(self.timestamp, self.metadata.interval.into());

        let mut bucket_offset = (current_bucket - previous_bucket).clamp(0, self.width().into());

        {
            let before = data.data.get_mut(0..=usize::from(idx));

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
            let after = data.data.get_mut(usize::from(idx)..);

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

        data.dirty = data.dirty || dirty;
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

        let data = self.data.borrow();
        let data = data.as_ref().expect("Never uninit.");

        let mut cells = Vec::new();
        let cell = data.data.cell_idx(self.timestamp, interval.into());

        let (idx, data) = (cell, data.data.as_ref());

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
