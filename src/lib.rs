#![allow(dead_code)]

pub(crate) mod error;
pub(crate) mod format;
pub(crate) mod tsdb;
pub(crate) mod util;

#[cfg(feature = "rune")]
pub mod rune;
#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "rune")]
pub use crate::{
    error::RuneError,
};

pub use error::TimeseriesError;
pub use format::DataCell;
pub use tsdb::{TimeseriesDatabase, TimeseriesPartition};
