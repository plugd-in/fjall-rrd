use std::{
    cell::{Ref, RefCell, RefMut},
    num::{NonZeroU16, NonZeroU32},
    ops::{Deref, DerefMut},
    rc::Rc,
    sync::Arc,
};

use fjall::{Keyspace, Partition, PartitionCreateOptions, Slice};
use itertools::Itertools;
use parking_lot::RwLock;
use rune::{
    self, Any, Context, ContextError, Module, Source, Sources, ToTypeHash, Unit, Vm, function,
    runtime::{Args, RuntimeContext},
};

use crate::{
    DataCell, TimeseriesError,
    format::{KeyType, Metadata, SeriesData, TieredData, TieredKey, TieredRuneMetadata},
    util::{TimeCellMut, timestamp_bucket},
};

#[derive(Clone)]
/// Tiered Round-robin structure that use the
/// [Rune](rune) scripting language for logic.
pub struct TieredRunePartition {
    name: Arc<str>,
    partition: Partition,
    metadata: TieredRuneMetadata,
    rune_context: Arc<Context>,
    rune_runtime_context: Arc<RuntimeContext>,
    rune_unit: Arc<RwLock<Arc<Unit>>>,
}

#[allow(unused)]
impl TieredRunePartition {
    pub(crate) fn new(
        name: Arc<str>,
        context: Arc<Context>,
        runtime: Arc<RuntimeContext>,
        metadata: TieredRuneMetadata,
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

    pub(crate) fn open_new<S: AsRef<str>>(
        keyspace: &Keyspace,
        meta: &Partition,
        context: Arc<Context>,
        runtime_context: Arc<RuntimeContext>,
        name: Arc<str>,
        script: S,
    ) -> Result<Self, TimeseriesError> {
        let script = script.as_ref();

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

        let metadata = Metadata::TieredRune(TieredRuneMetadata {
            script: script.into(),
        });

        meta.insert(name.as_ref(), Slice::try_from(&metadata)?)?;

        let Metadata::TieredRune(metadata) = metadata else {
            panic!("This should never happen.");
        };

        let partition = TieredRunePartition {
            name: name.clone(),
            rune_context: context,
            rune_runtime_context: runtime_context,
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

        let key = KeyType::Tier(TieredKey {
            inner_key: user_key.into(),
            nth_tier: 0,
        });
        let encoded_key = Slice::try_from(&key)?;

        let tiers: Vec<TieredData> = self
            .partition
            .range(encoded_key.as_ref()..)
            .map(|maybe_value| match maybe_value {
                Ok((key, value)) => {
                    let key = KeyType::try_from(key)?;
                    let value = SeriesData::try_from(value)?;

                    Ok((key, value))
                }
                Err(e) => Err(TimeseriesError::Fjall(e)),
            })
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
        let user_key = Rc::<[u8]>::from(user_key);

        let tiers = Rc::new(RefCell::new(tiers));
        let metric = Rc::new(metric);

        let mut n: u16 = 0;
        let mut ctx = TieredRuneContext {
            partition_name: self.name.clone(),
            inner_key: user_key.clone(),
            partition: self.partition.clone(),
            tiers: tiers.clone(),
            nth_tier: None,
            metric: metric.clone(),
            cumulative_interval: 0,
            timestamp,
        };

        if tiers.as_ref().borrow().is_empty() {
            self.exec_rune_fn(["handle_tiered_insert"], (ctx.clone(),))?;
        }

        loop {
            let tiers_len = tiers.as_ref().borrow().len();

            if usize::from(n) >= tiers_len {
                break;
            }

            ctx.nth_tier = Some(n);
            self.exec_rune_fn(["handle_tiered_insert"], (ctx.clone(),))?;

            if let Some(tier) = tiers.as_ref().borrow().get(usize::from(n)) {
                if ctx.cumulative_interval == 0 {
                    ctx.cumulative_interval = u32::from(tier.interval.get());
                } else {
                    ctx.cumulative_interval *= u32::from(tier.interval.get());
                }
            }

            n += 1;
        }

        let mut tiers = tiers.borrow_mut();
        let tiers = std::mem::take(tiers.deref_mut());

        for (nth_tier, mut tier) in tiers.into_iter().enumerate() {
            if tier.dirty {
                tier.last_timestamp = timestamp;

                self.partition.insert(
                    Slice::try_from(KeyType::Tier(TieredKey {
                        inner_key: user_key.as_ref().into(),
                        nth_tier: u16::try_from(nth_tier).expect("limited tiers"),
                    }))?,
                    Slice::try_from(SeriesData::Tiered(tier))?,
                )?;
            }
        }

        Ok(())
    }
}

#[derive(Any, Clone)]
pub(crate) struct TieredRuneContext {
    pub(crate) partition_name: Arc<str>,
    pub(crate) inner_key: Rc<[u8]>,
    pub(crate) partition: Partition,
    pub(crate) tiers: Rc<RefCell<Vec<TieredData>>>,
    pub(crate) nth_tier: Option<u16>,
    pub(crate) timestamp: i64,
    pub(crate) cumulative_interval: u32,
    pub(crate) metric: Rc<DataCell>,
}

impl TieredRuneContext {
    fn inner_current_tier(&self) -> Option<Ref<'_, TieredData>> {
        let Some(nth_tier) = self.nth_tier else {
            return None;
        };

        let tiers = self.tiers.borrow();
        let nth_tier = usize::from(nth_tier);

        if nth_tier < tiers.len() {
            Some(Ref::map(tiers, |tiers| &tiers[nth_tier]))
        } else {
            None
        }
    }

    fn current_tier_mut(&self) -> Option<RefMut<'_, TieredData>> {
        let Some(nth_tier) = self.nth_tier else {
            return None;
        };

        let tiers = self.tiers.borrow_mut();
        let nth_tier = usize::from(nth_tier);

        if nth_tier < tiers.len() {
            Some(RefMut::map(tiers, |tiers| &mut tiers[nth_tier]))
        } else {
            None
        }
    }

    fn previous_comitted(&self) -> bool {
        let Some(current_tier) = self.nth_tier.and_then(|tier| tier.checked_sub(1)) else {
            return true;
        };

        let tiers = self.tiers.borrow();
        let Some(previous_tier) = tiers.get(usize::from(current_tier)) else {
            return true;
        };

        !previous_tier.pristine()
    }

    #[function]
    fn empty(&self) -> bool {
        self.nth_tier.is_none()
    }

    #[function]
    fn metric(&self) -> DataCell {
        self.metric.deref().clone()
    }

    #[function]
    fn missed(&self) -> Option<u16> {
        if let Some(current_tier) = self.inner_current_tier() {
            if current_tier.pristine() {
                return Some(0);
            }

            let total_interval = self.total_interval(&current_tier);

            let current_bucket = timestamp_bucket(self.timestamp, total_interval);
            let previous_bucket = timestamp_bucket(current_tier.last_timestamp, total_interval);

            Some(
                u16::try_from(
                    (current_bucket - previous_bucket).clamp(0, current_tier.width.get().into()),
                )
                .expect("Within u16, by clamp.")
                .saturating_sub(1),
            )
        } else {
            None
        }
    }

    #[function]
    fn write_custom_current(&self, data: DataCell) {
        if let Some(mut current_tier) = self.current_tier_mut() {
            current_tier.custom_data = data;
            current_tier.dirty = true;
            current_tier.last_timestamp = self.timestamp;
        }
    }

    #[function]
    fn get_custom_current(&self) -> Option<DataCell> {
        if let Some(current_tier) = self.inner_current_tier() {
            Some(current_tier.custom_data.clone())
        } else {
            None
        }
    }

    #[function]
    fn current_width(&self) -> u16 {
        if let Some(current_tier) = self.inner_current_tier() {
            current_tier.width.get()
        } else {
            0
        }
    }

    #[function]
    fn current_interval(&self) -> u16 {
        if let Some(current_tier) = self.inner_current_tier() {
            current_tier.interval.get()
        } else {
            0
        }
    }

    fn total_interval(&self, tier: &TieredData) -> NonZeroU32 {
        if self.cumulative_interval == 0 {
            NonZeroU32::from(tier.interval)
        } else {
            NonZeroU32::new(self.cumulative_interval * u32::from(tier.interval.get()))
                .expect("at least zero, by math")
        }
    }

    #[function]
    fn write_metric(&mut self, metric: DataCell) {
        if let Some(mut current_tier) = self.current_tier_mut() {
            let total_interval = self.total_interval(&current_tier);
            let current_cell = current_tier
                .data
                .get_cell_mut(self.timestamp(), total_interval);

            *current_cell = metric;
            current_tier.dirty = true;
            current_tier.last_timestamp = self.timestamp;
        }
    }

    #[function]
    fn create_tier(&mut self, width: u16, interval: u16) -> Result<(), TimeseriesError> {
        let width = NonZeroU16::new(width).ok_or(TimeseriesError::ZeroU16)?;
        let interval = NonZeroU16::new(interval).ok_or(TimeseriesError::ZeroU16)?;

        let new_tier = TieredData::new_empty(width.get(), interval.get())?;

        let mut tiers = self.tiers.borrow_mut();
        tiers.push(new_tier);

        Ok(())
    }

    #[function(keep)]
    fn clear_misses(&mut self) {
        let Some(current_tier) = self.nth_tier else {
            return;
        };

        let mut tiers = self.tiers.borrow_mut();
        let Some(current_tier) = tiers.get_mut(usize::from(current_tier)) else {
            return;
        };

        current_tier.clear_misses(self.timestamp, self.cumulative_interval);
    }

    #[function(keep)]
    /// Get the current timestamp of the operation.
    fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[function(keep)]
    /// Get the name of the partition associated
    /// with this operation.
    fn partition_name(&self) -> String {
        self.partition_name.deref().to_string()
    }

    #[function(keep)]
    /// Look back at the last `n_cells` in the previous
    /// tier.
    fn look_back_previous(&self, n_cells: u16) -> Option<Vec<DataCell>> {
        let Some(current_tier) = self.nth_tier.and_then(|tier| tier.checked_sub(1)) else {
            return None;
        };

        let tiers = self.tiers.borrow();
        let Some(previous_tier) = tiers.get(usize::from(current_tier)) else {
            return None;
        };

        let cumulative_interval = if self.cumulative_interval == 0
            || self.cumulative_interval == u32::from(previous_tier.interval.get())
        {
            0u32
        } else {
            self.cumulative_interval / u32::from(previous_tier.interval.get())
        };

        Some(
            previous_tier
                .look_back(self.timestamp, cumulative_interval, n_cells)
                .into_vec(),
        )
    }

    #[function(keep)]
    /// Look back at the last `n_cells` in the current
    /// tier.
    fn look_back_current(&self, n_cells: u16) -> Option<Vec<DataCell>> {
        let Some(current_tier) = self.nth_tier else {
            return None;
        };

        let tiers = self.tiers.borrow();
        let Some(current_tier) = tiers.get(usize::from(current_tier)) else {
            return None;
        };

        Some(
            current_tier
                .look_back(self.timestamp, self.cumulative_interval, n_cells)
                .into_vec(),
        )
    }
}

pub(crate) fn module() -> Result<Module, ContextError> {
    let mut module = Module::new();
    module.ty::<TieredRuneContext>()?;
    module.ty::<DataCell>()?;
    module.function_meta(TieredRuneContext::look_back_current__meta)?;
    module.function_meta(TieredRuneContext::look_back_previous__meta)?;
    module.function_meta(TieredRuneContext::partition_name__meta)?;
    module.function_meta(TieredRuneContext::clear_misses__meta)?;
    module.function_meta(TieredRuneContext::create_tier)?;
    module.function_meta(TieredRuneContext::write_metric)?;
    module.function_meta(TieredRuneContext::write_custom_current)?;
    module.function_meta(TieredRuneContext::get_custom_current)?;
    module.function_meta(TieredRuneContext::metric)?;
    module.function_meta(TieredRuneContext::empty)?;
    module.function_meta(TieredRuneContext::current_width)?;
    module.function_meta(TieredRuneContext::current_interval)?;
    module.function_meta(DataCell::custom)?;

    Ok(module)
}
