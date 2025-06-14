# [Fjall](https://github.com/fjall-rs/fjall) Round-Robin Database
Store timeseries data in a round-robin data structure, with customized
insertion logic.

## Collection Logic
Metrics are intercepted by logic that can decide whether to insert
the metric, decide to transform the metric before insertion,
decide the rollup logic for different tiers, etc.

### Supported Languages
The following language integrations are supported:

* WebAssembly Components (feature: `wasm`)
* [Rune](https://github.com/rune-rs/rune) (feature: `rune`)

The WebAssembly interface is documented in the [wit](/wit/fjall-rrd) files.
Currently, the Rune integration is undocumented.

### Screenshots
![Rust implementation of the WebAssembly interface.](/images/rust-impl.png)
![Rune implementation of a handler.](/images/rune-impl.png)


## Data Layout
Supports inserting into a single circular buffer or inserting
into "tiers" of data. This allows you to do simple insertions after
processing some type of metric, or it allows you to consolidate your
data into lower tiers. One use of this is to implement a data
retention policy that decreases the precision of data over time.

## Stability
The format of the data SHALL remain stable across and within versions.
Any changes to the format SHALL be accompanied by automated migrations
upon opening the database.

That said, changes to Fjall's storage format do not apply to this stable
format guarantee.

Additions to the language interfaces will not result in a major version
upgrade so long as those additions do not break existing interfaces. A
breakage to existing interfaces or deprecation of a language interface
will result in a major version bump.

Changes to the existing public API (e.g. the `DataCell` type) will result
in a major version bump, so long as those changes are not backward compatible.

These version semantics will hold for versions including and following v0.1.0.

## Work in Progress
This project is a work-in-progress. The interface needs some work and there's
some logic missing.

* [X] Finish implementing a complete WebAssembly interface.
* [X] Finish implementing a complete Rune interface.
* [ ] Implement logic to create backups of timeseries data.
* [X] Implement logic to ensure a stable data format,
    or implement migration logic to ensure stability.
* [ ] Create examples in Rune and Rust/WebAssembly.
* [ ] Implement tests for the format and language
    integrations.
