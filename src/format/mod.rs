//! Specifies the format of the data.
//!
//! Internally, the format is versioned. Major versions will
//! automatically upgrade the formats in the database,
//! but breaking changes are allowed to occur in terms of the
//! API of the formats, namely DataCell.

mod v1;

pub use v1::DataCell;

pub(crate) use v1::{
    KeyType, Metadata, SeriesData, SingleData, SingleKey, SingleRuneMetadata, SingleWasmMetadata,
    TieredData, TieredKey, TieredRuneMetadata, TieredWasmMetadata,
};
