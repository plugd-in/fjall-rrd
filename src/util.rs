use std::num::{NonZeroU16, NonZeroU32};

/// Which bucket of time the timestamp falls into
/// for a given interval.
///
/// This cuts time (in seconds) into buckets of the given interval.
///
/// So a timestamp of 0 (Jan-1-1970) and an interval of 15 seconds
/// would fall into the 0th bucket. This would also hold true for a
/// timestamp of 0-14, while a timestamp of 15 would put it in the
/// next (1st) bucket.
#[inline]
pub(crate) fn timestamp_bucket(timestamp: i64, interval: NonZeroU32) -> i64 {
    return (timestamp - (timestamp % (i64::from(interval.get())))) / i64::from(interval.get());
}

/// Get the current cell index within a
/// collection given a timestamp.
#[inline]
pub(crate) fn stamp_cell(timestamp: i64, width: NonZeroU16, interval: NonZeroU32) -> u16 {
    let cell = (timestamp_bucket(timestamp, interval) % i64::from(width.get())).abs();
    let cell = u16::try_from(cell).expect("u16 based on mod math");

    cell
}

pub(crate) trait TimeCell<'a> {
    type Item;

    fn cell_idx(self, timestamp: i64, interval: NonZeroU32) -> u16;
    fn get_cell(self, timestamp: i64, interval: NonZeroU32) -> &'a Self::Item;
}

pub(crate) trait TimeCellMut<'a> {
    type Item;

    fn get_cell_mut(self, timestamp: i64, interval: NonZeroU32) -> &'a mut Self::Item;
}

impl<'a, T> TimeCell<'a> for &'a [T] {
    type Item = T;

    fn cell_idx(self, timestamp: i64, interval: NonZeroU32) -> u16 {
        let len = self.len();
        let len = u16::try_from(len).expect("Collection to be smaller than u16 max...");
        let len = NonZeroU16::new(len).expect("Collection to have non-zero length...");

        stamp_cell(timestamp, len, interval)
    }

    fn get_cell(self, timestamp: i64, interval: NonZeroU32) -> &'a Self::Item {
        let len = self.len();
        let len = u16::try_from(len).expect("Collection to be smaller than u16 max...");
        let len = NonZeroU16::new(len).expect("Collection to have non-zero length...");

        self.get(usize::from(stamp_cell(timestamp, len, interval)))
            .expect("Stamp cell index...")
    }
}

impl<'a, T> TimeCellMut<'a> for &'a mut [T] {
    type Item = T;

    fn get_cell_mut(self, timestamp: i64, interval: NonZeroU32) -> &'a mut Self::Item {
        let len = self.len();
        let len = u16::try_from(len).expect("Collection to be smaller than u16 max...");
        let len = NonZeroU16::new(len).expect("Collection to have non-zero length...");

        self.get_mut(usize::from(stamp_cell(timestamp, len, interval)))
            .expect("Stamp cell index...")
    }
}
