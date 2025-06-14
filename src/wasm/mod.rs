//! WebAssembly integration integration.
//!
//! Allows you to bring your own language, so long
//! as the language can be compiled into WebAssembly.
//!
//! Good security and performance.
//!
//! This project includes a `wit/` directory that documents
//! the interface. Using a tool like
//! [wit-bindgen](https://github.com/bytecodealliance/wit-bindgen).

use wasmtime_wasi::{
    ResourceTable,
    p2::{IoView, WasiCtx, WasiCtxBuilder, WasiView},
};

pub(crate) mod single;
pub(crate) mod tiered;

pub use single::SingleWasmPartition;
pub use tiered::TieredWasmPartition;

pub(crate) struct WasiStateMaybeUninit<T> {
    pub(crate) wasi: WasiCtx,
    pub(crate) resources: ResourceTable,
    pub(crate) state: Option<T>,
}

impl<T> Default for WasiStateMaybeUninit<T> {
    fn default() -> Self {
        let wasi = WasiCtxBuilder::new()
            .inherit_stderr()
            .inherit_stdout()
            .build();

        Self {
            resources: Default::default(),
            state: None,
            wasi,
        }
    }
}

impl<T> IoView for WasiStateMaybeUninit<T>
where
    T: Send,
{
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.resources
    }
}

impl<T> WasiView for WasiStateMaybeUninit<T>
where
    T: Send,
{
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

macro_rules! impl_data_cell {
    () => {
        impl From<crate::DataCell> for DataCell {
            fn from(value: crate::DataCell) -> Self {
                match value {
                    crate::DataCell::U64(num) => DataCell::UnsignedEight(num),
                    crate::DataCell::Empty => DataCell::Empty,
                    crate::DataCell::Percent(pct) => DataCell::Percent(pct),
                    crate::DataCell::Text(text) => DataCell::Text(text),
                    crate::DataCell::Custom(bytes) => DataCell::Custom(bytes.into_vec()),
                }
            }
        }

        impl From<DataCell> for crate::DataCell {
            fn from(value: DataCell) -> Self {
                match value {
                    DataCell::UnsignedEight(num) => crate::DataCell::U64(num),
                    DataCell::Percent(pct) => crate::DataCell::Percent(pct),
                    DataCell::Text(text) => crate::DataCell::Text(text),
                    DataCell::Custom(bytes) => crate::DataCell::Custom(bytes.into()),
                    DataCell::Empty => crate::DataCell::Empty,
                }
            }
        }
    };
}

use impl_data_cell;
