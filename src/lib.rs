#![allow(dead_code)]

pub(crate) mod error;
pub(crate) mod format;
pub(crate) mod tsdb;
pub(crate) mod util;

#[cfg(feature = "rune")]
pub(crate) mod rune;
#[cfg(feature = "wasm")]
pub(crate) mod wasm;

#[cfg(feature = "rune")]
pub use crate::{
    error::RuneError,
    rune::{single::SingleRunePartition, tiered::TieredRunePartition},
};
#[cfg(feature = "wasm")]
pub use wasm::{single::SingleWasmPartition, tiered::TieredWasmPartition};

pub use error::TimeseriesError;
pub use format::DataCell;
pub use tsdb::{TimeseriesDatabase, TimeseriesPartition};
