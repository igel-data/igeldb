# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]
### Added
- **Babashka compatibility.** IgelDB now loads and runs under Babashka (bb) as a
  pure-Clojure library — no pod, no native binary. See `test/bb_smoke.clj`.
- Explicit `igeldb.core/close!` to shut a store down (replaces the removed
  `Object.finalize` hook, which was unreliable and unsupported under bb's SCI).

### Changed
- Manifest edit encoding: Fressian → the project's own binary format (inside the
  unchanged Phase 1 length+CRC frame).
- Bloom filters: `blossom 1.1.0` → `2.0.0` (zero-dependency, `byte[]` API).
- Internal representation swaps (behavior unchanged, verified by the JVM suite):
  the memtable is now an immutable `sorted-map` behind an atom (lock-free reads,
  fixing the old concurrent-read defect); the reader/deletion lock is a
  `Semaphore`; fsync uses `FileChannel.force(true)`.

### Removed
- `org.clojure/data.fressian` dependency.

> **Migration:** a data directory written before this change is not readable
> afterward (manifest edit encoding and bloom serialization both changed). There
> are no external users yet, so no compatibility shim is provided.

## [0.1.1] - 2023-08-12
### Changed
- Documentation on how to make the widgets.

### Removed
- `make-widget-sync` - we're all async, all the time.

### Fixed
- Fixed widget maker to keep working when daylight savings switches over.

## 0.1.0 - 2023-08-12
### Added
- Files from the new template.
- Widget maker public API - `make-widget-sync`.

[Unreleased]: https://sourcehost.site/your-name/kvs/compare/0.1.1...HEAD
[0.1.1]: https://sourcehost.site/your-name/kvs/compare/0.1.0...0.1.1
