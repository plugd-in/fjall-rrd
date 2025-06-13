//! Specifies the format of the data.
//!
//! Internally, the format is versioned. The format isn't
//! allowed to break across or within versions, without
//! an automated migration process.
//!
//! Changes to the format version should result
//! in an upgrade to the formats in the database
//! when opening the database.
//!
//! That said breaking changes are allowed to occur in terms
//! of the API of the formats, namely DataCell. Such breakages
//! will result in a bump to the major version.

mod v1;

pub use v1::DataCell;

pub(crate) use v1::{KeyType, Metadata, SeriesData, SingleData, SingleKey, TieredData, TieredKey};

#[cfg(feature = "rune")]
pub(crate) use v1::{SingleRuneMetadata, TieredRuneMetadata};

#[cfg(feature = "wasm")]
pub(crate) use v1::{SingleWasmMetadata, TieredWasmMetadata};
