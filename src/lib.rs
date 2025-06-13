#![allow(dead_code)]

pub(crate) mod error;
pub(crate) mod format;
pub(crate) mod tsdb;
pub(crate) mod util;

#[cfg(feature = "rune")]
pub(crate) mod rune;
#[cfg(feature = "wasm")]
pub(crate) mod wasm;

pub use error::{RuneError, TimeseriesError};
pub use format::DataCell;
pub use rune::{single::SingleRunePartition, tiered::TieredRunePartition};
pub use tsdb::{TimeseriesDatabase, TimeseriesPartition};
#[cfg(feature = "wasm")]
pub use wasm::{single::SingleWasmPartition, tiered::TieredWasmPartition};
