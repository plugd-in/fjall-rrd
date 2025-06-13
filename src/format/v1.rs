use std::num::{NonZeroU16, NonZeroU32};

use bytes::BufMut;
use cfg_if::cfg_if;
use fjall::Slice;
use rune::{Any, function};
use serde::{Deserialize, Serialize};

use crate::{
    TimeseriesError,
    util::{TimeCell, timestamp_bucket},
};

/// Points to a tier's data in a tiered RRD database.
#[derive(Serialize, Deserialize)]
pub(crate) struct TieredKey {
    /// The user's key for the item.
    pub(crate) inner_key: Box<[u8]>,
    /// Which tier of data this represents.
    ///
    /// Tiers waterfall down into higher tiers,
    /// based on some custom collection algorithm
    /// that can be specified based on the nth tier,
    /// the previous tier's data, and the current
    /// tier's data.
    pub(crate) nth_tier: u16,
}

/// Points to the data for a single item in
/// a single RRD database.
///
/// The data is filled in based on some custom collection
/// algorithm that can be specified based on the overall
/// data and the new data being inserted.
#[derive(Serialize, Deserialize)]
pub(crate) struct SingleKey {
    /// The user's key for the item.
    pub(crate) inner_key: Box<[u8]>,
}

/// The type of keys used in a [Partitions](fjall::Partition),
/// with each partition representing it's own time-series
/// database and this representing the keys in that partition.
#[derive(Serialize, Deserialize)]
pub(crate) enum KeyType {
    Tier(TieredKey),
    Single(SingleKey),
}

impl KeyType {
    pub(crate) fn as_tiered(&self) -> Option<&TieredKey> {
        if let Self::Tier(tiered) = self {
            Some(tiered)
        } else {
            None
        }
    }
}

impl TryFrom<&KeyType> for Slice {
    type Error = TimeseriesError;

    fn try_from(value: &KeyType) -> Result<Self, TimeseriesError> {
        postcard::to_stdvec(&value)
            .map(Into::into)
            .map_err(|_| TimeseriesError::FormatError)
    }
}

impl TryFrom<KeyType> for Slice {
    type Error = TimeseriesError;

    fn try_from(value: KeyType) -> Result<Self, TimeseriesError> {
        (&value).try_into()
    }
}

impl TryFrom<Slice> for KeyType {
    type Error = TimeseriesError;

    fn try_from(value: Slice) -> Result<Self, TimeseriesError> {
        postcard::from_bytes(value.as_ref()).map_err(|_| TimeseriesError::FormatError)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SingleRuneMetadata {
    /// How many cells the RRD structure can store.
    pub(crate) width: NonZeroU16,
    /// How much time is given to each cell.
    pub(crate) interval: NonZeroU16,
    /// Holds the collection script written in the [Rune](<https://rune-rs.github.io>)
    /// programming language.
    pub(crate) script: Box<str>,
}

#[derive(Clone, Default, Debug, Deserialize, Serialize)]
pub(crate) struct TieredRuneMetadata {
    /// Holds the collection script written in the [Rune](<https://rune-rs.github.io>)
    /// programming language.
    pub(crate) script: Box<str>,
}

#[cfg(feature = "wasm")]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SingleWasmMetadata {
    /// How many cells the RRD structure can store.
    pub(crate) width: NonZeroU16,
    /// How much time is given to each cell.
    pub(crate) interval: NonZeroU16,
    /// Holds a WASM component that implements
    /// [SingleComponent](crate::wasi::single::SingleComponent).
    pub(crate) component: Box<[u8]>,
}

#[cfg(feature = "wasm")]
#[derive(Clone, Default, Debug, Deserialize, Serialize)]
pub(crate) struct TieredWasmMetadata {
    /// Holds a WASM component that implements
    /// [TieredComponent](crate::wasi::tiered::TieredComponent).
    pub(crate) component: Box<[u8]>,
}

/// Describes the format of the timeseries database in
/// a partition.
///
/// Note: This enum is manually discriminated when
/// serialized/deserialized using the length-prefixed
/// snake_case variant name. This allows us to support
/// enabling/disabling language integrations using
/// crate features.
#[derive(Clone, Debug)]
pub(crate) enum Metadata {
    SingleRune(SingleRuneMetadata),
    TieredRune(TieredRuneMetadata),
    #[cfg(feature = "wasm")]
    SingleWasm(SingleWasmMetadata),
    #[cfg(feature = "wasm")]
    TieredWasm(TieredWasmMetadata),
}

impl TryFrom<&Metadata> for Slice {
    type Error = TimeseriesError;

    fn try_from(value: &Metadata) -> Result<Self, TimeseriesError> {
        let mut data = Vec::<u8>::new();

        match value {
            Metadata::SingleRune(meta) => {
                let name = "single_rune";
                data.put_u8(name.len() as u8);
                data.put(name.as_bytes());

                data = postcard::to_extend(meta, data).map_err(|_| TimeseriesError::FormatError)?;
            }
            Metadata::TieredRune(meta) => {
                let name = "tiered_rune";
                data.put_u8(name.len() as u8);
                data.put(name.as_bytes());

                data = postcard::to_extend(meta, data).map_err(|_| TimeseriesError::FormatError)?;
            }
            #[cfg(feature = "wasm")]
            Metadata::SingleWasm(meta) => {
                let name = "single_wasm";
                data.put_u8(name.len() as u8);
                data.put(name.as_bytes());

                data = postcard::to_extend(meta, data).map_err(|_| TimeseriesError::FormatError)?;
            }
            #[cfg(feature = "wasm")]
            Metadata::TieredWasm(meta) => {
                let name = "tiered_wasm";
                data.put_u8(name.len() as u8);
                data.put(name.as_bytes());

                data = postcard::to_extend(meta, data).map_err(|_| TimeseriesError::FormatError)?;
            }
        }

        Ok(data.into())
    }
}

impl TryFrom<Metadata> for Slice {
    type Error = TimeseriesError;

    fn try_from(value: Metadata) -> Result<Self, TimeseriesError> {
        (&value).try_into()
    }
}

impl TryFrom<Slice> for Metadata {
    type Error = TimeseriesError;

    fn try_from(value: Slice) -> Result<Self, TimeseriesError> {
        let variant_length: u8 = value.get(0).ok_or(TimeseriesError::FormatError)?.clone();

        let (variant_name, meta) = value
            .get(1..)
            .ok_or(TimeseriesError::FormatError)?
            .split_at_checked(usize::from(variant_length))
            .ok_or(TimeseriesError::FormatError)?;

        let variant_name =
            std::str::from_utf8(variant_name).map_err(|_| TimeseriesError::FormatError)?;

        match variant_name {
            "single_rune" => Ok(Metadata::SingleRune(
                postcard::from_bytes(meta).map_err(|_| TimeseriesError::FormatError)?,
            )),
            "tiered_rune" => Ok(Metadata::TieredRune(
                postcard::from_bytes(meta).map_err(|_| TimeseriesError::FormatError)?,
            )),
            "single_wasm" => {
                cfg_if! {
                    if #[cfg(feature = "wasm")] {
                        Ok(Metadata::SingleWasm(
                            postcard::from_bytes(meta).map_err(|_| TimeseriesError::FormatError)?,
                        ))
                    } else {
                        Err(TimeseriesError::LanguageDisabled("wasm"))
                    }
                }
            }
            "tiered_wasm" => {
                cfg_if! {
                    if #[cfg(feature = "wasm")] {
                        Ok(Metadata::TieredWasm(
                            postcard::from_bytes(meta).map_err(|_| TimeseriesError::FormatError)?,
                        ))
                    } else {
                        Err(TimeseriesError::LanguageDisabled("wasm"))
                    }
                }
            }
            _ => Err(TimeseriesError::FormatError),
        }
    }
}

/// The types of data that can be put into an RRD
/// cell, at the user's discretion.
#[derive(Any, Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum DataCell {
    #[rune(constructor)]
    Empty,
    #[rune(constructor)]
    U64(#[rune(get)] u64),
    #[rune(constructor)]
    Percent(#[rune(get)] u8),
    #[rune(constructor)]
    Text(#[rune(get)] String),
    Custom(Box<[u8]>),
}

impl DataCell {
    #[function(path = DataCell::Custom)]
    fn custom(data: Vec<u8>) -> Self {
        Self::Custom(data.into_boxed_slice())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SingleData {
    /// The timestamp of the last commit.
    pub(crate) last_timestamp: i64,
    /// A place for a user to programatically stick
    /// custom data.
    pub(crate) custom_data: DataCell,
    /// The underlying data.
    pub(crate) data: Box<[DataCell]>,
    #[serde(skip)]
    pub(crate) dirty: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct TieredData {
    /// The timestamp of the last commit.
    pub(crate) last_timestamp: i64,
    /// A place for a user to programatically stick
    /// custom data.
    pub(crate) custom_data: DataCell,
    /// The width of the current tier.
    pub(crate) width: NonZeroU16,
    /// How much time is given to each cell in the tier.
    pub(crate) interval: NonZeroU16,
    /// The underlying data.
    pub(crate) data: Box<[DataCell]>,
    #[serde(skip)]
    pub(crate) dirty: bool,
}

impl TieredData {
    pub(crate) fn look_back(
        &self,
        timestamp: i64,
        cumulative_interval: u32,
        how_far: u16,
    ) -> Box<[DataCell]> {
        let mut n_cells = how_far.min(self.width.get());

        let total_interval = if cumulative_interval == 0 {
            NonZeroU32::from(self.interval)
        } else {
            NonZeroU32::new(cumulative_interval * u32::from(self.interval.get()))
                .expect("at least zero, by math")
        };

        let mut cells = Vec::new();
        let cell = self.data.cell_idx(timestamp, total_interval);

        let (idx, data) = (cell, self.data.as_ref());

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

        cells.into_boxed_slice()
    }

    pub(crate) fn write_multi_back(
        &mut self,
        timestamp: i64,
        cumulative_interval: u32,
        back: u16,
        metric: DataCell,
    ) {
        let mut back = back + 1;
        let total_interval = if cumulative_interval == 0 {
            NonZeroU32::from(self.interval)
        } else {
            NonZeroU32::new(cumulative_interval * u32::from(self.interval.get()))
                .expect("at least zero, by math")
        };

        let idx = self.data.cell_idx(timestamp, total_interval);

        {
            let before = self.data.get_mut(0..=usize::from(idx));

            if let Some(before) = before {
                for cell in before.into_iter().rev() {
                    if !(back > 0) {
                        break;
                    }

                    *cell = metric.clone();

                    back -= 1;
                }
            }
        }

        {
            let after = self.data.get_mut(usize::from(idx)..);

            if let Some(after) = after {
                for cell in after.into_iter().skip(1).rev() {
                    if !(back > 0) {
                        break;
                    }

                    *cell = metric.clone();

                    back -= 1;
                }
            }
        }

        self.dirty = true;
    }

    pub(crate) fn clear_misses(&mut self, timestamp: i64, cumulative_interval: u32) {
        if self.pristine() {
            return;
        }

        let total_interval = if cumulative_interval == 0 {
            NonZeroU32::from(self.interval)
        } else {
            NonZeroU32::new(cumulative_interval * u32::from(self.interval.get()))
                .expect("at least zero, by math")
        };

        let mut dirty = false;

        let current_bucket = timestamp_bucket(timestamp, total_interval);
        let previous_bucket = timestamp_bucket(self.last_timestamp, total_interval);

        let idx = self.data.cell_idx(timestamp, total_interval);
        let mut bucket_offset =
            (current_bucket - previous_bucket).clamp(0, self.width.get().into());

        {
            let before = self.data.get_mut(0..=usize::from(idx));

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
            let after = self.data.get_mut(usize::from(idx)..);

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

        self.dirty = self.dirty || dirty;
    }

    pub(crate) fn pristine(&self) -> bool {
        return self.last_timestamp == i64::MIN;
    }

    pub(crate) fn new_empty(width: u16, interval: u16) -> Result<Self, TimeseriesError>
    where
        Self: Sized,
    {
        let width = NonZeroU16::new(width).ok_or(TimeseriesError::ZeroU16)?;
        let interval = NonZeroU16::new(interval).ok_or(TimeseriesError::ZeroU16)?;

        Ok(Self {
            custom_data: DataCell::Empty,
            data: vec![DataCell::Empty; usize::from(width.get())].into_boxed_slice(),
            last_timestamp: i64::MIN,
            dirty: true,
            width,
            interval,
        })
    }
}

/// Contains the RRD structure alongside some
/// information on the structure's state.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum SeriesData {
    Single(SingleData),
    Tiered(TieredData),
}

impl SeriesData {
    pub(crate) fn into_tiered(self) -> Option<TieredData> {
        if let Self::Tiered(data) = self {
            Some(data)
        } else {
            None
        }
    }
}

impl TryFrom<&SeriesData> for Slice {
    type Error = TimeseriesError;

    fn try_from(value: &SeriesData) -> Result<Self, TimeseriesError> {
        postcard::to_stdvec(&value)
            .map(Into::into)
            .map_err(|_| TimeseriesError::FormatError)
    }
}

impl TryFrom<SeriesData> for Slice {
    type Error = TimeseriesError;

    fn try_from(value: SeriesData) -> Result<Self, TimeseriesError> {
        (&value).try_into()
    }
}

impl TryFrom<Slice> for SeriesData {
    type Error = TimeseriesError;

    fn try_from(value: Slice) -> Result<Self, TimeseriesError> {
        postcard::from_bytes(value.as_ref()).map_err(|_| TimeseriesError::FormatError)
    }
}
