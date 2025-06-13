use fjall::Error as FjallError;
#[cfg(feature = "rune")]
use rune::{Any, BuildError, ContextError, alloc::Error as AllocError, runtime::VmError};
use thiserror::Error;

#[cfg(feature = "rune")]
#[derive(Debug, Error)]
pub enum RuneError {
    #[error("{0}")]
    Alloc(#[from] AllocError),
    #[error("{0}")]
    Build(#[from] BuildError),
    #[error("{0}")]
    Context(#[from] ContextError),
    #[error("{0}")]
    VirtualMachine(#[from] VmError),
}

#[cfg_attr(feature = "rune", derive(Any))]
#[derive(Debug, Error)]
pub enum TimeseriesError {
    #[cfg(feature = "rune")]
    #[error("{0}")]
    Rune(RuneError),
    #[error("{0}")]
    Fjall(#[from] FjallError),
    #[cfg(feature = "wasm")]
    #[error("{0}")]
    WebAssembly(anyhow::Error),
    #[error("Required NonZeroU16, got 0.")]
    ZeroU16,
    #[cfg(feature = "wasm")]
    #[error("Tried to execute WebAssembly component but the state is not initialized.")]
    StateUninit,
    #[error("Tried to open a new partition, but it already exists.")]
    PartitionExists,
    #[error("Previous tier not committed...")]
    NotCommitted,
    #[error("Failed to parse format. Data corrupted.")]
    FormatError,
    #[error("Found disabled language integration ({0}).")]
    LanguageDisabled(&'static str),
}

impl TimeseriesError {
    pub(crate) fn as_disabled_language(&self) -> Option<&'static str> {
        if let Self::LanguageDisabled(language) = self {
            Some(language)
        } else {
            None
        }
    }
}

#[cfg(feature = "rune")]
impl<T> From<T> for TimeseriesError
where
    T: Into<RuneError>,
{
    fn from(value: T) -> Self {
        Self::Rune(value.into())
    }
}
