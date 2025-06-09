# [Fjall](https://github.com/fjall-rs/fjall) Round-Robin Database
Store timeseries data in a round-robin data structure, with customized
insertion logic.

## Screenshots
![Rust implementation of the WebAssembly interface.](/images/rust-impl.png)

## Data Layout
Supports inserting into a single circular buffer or inserting
into "tiers" of data. This allows you to do simple insertions after
processing some type of metric, or it allows you to consolidate your
data into lower tiers. One use of this is to implement a data
retention policy that decreases the precision of data over time.

## Collection Logic
Metrics are intercepted by logic that can decide whether to insert
the metric, decide to transform the metric before insertion,
decide the rollup logic for different tiers, etc.

Currently, WebAssembly modules and the [Rune](https://github.com/rune-rs/rune)
scripting language are supported.

The WebAssembly interface is documented in the [wit](/wit/fjall-rrd) files.
Currently, the Rune integration is undocumented.

## Work in Progress
This project is a work-in-progress. The interface needs some work and there's
some logic missing.

* [X] Finish implementing a complete WebAssembly interface.
* [ ] Finish implementing a complete Rune interface.
* [ ] Implement logic to create backups of timeseries data.
* [ ] Implement logic to ensure a stable data format,
    or implement migration logic to ensure stability.
* [ ] Create examples in Rune and Rust/WebAssembly.
* [ ] Implement tests for the format and language
    integrations.
