//! [Rune](rune) language integration.

pub(crate) mod single;
pub(crate) mod tiered;

pub use single::SingleRunePartition;
pub use tiered::TieredRunePartition;
