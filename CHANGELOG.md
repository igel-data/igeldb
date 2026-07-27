# Changelog

All notable changes to this project are documented here. This changelog follows
[Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [1.0.0] - 2026-07-27

Initial stable release.

### Added

- Embeddable, pure-Clojure LSM-tree key-value store for the JVM and Babashka.
- Durable WAL-backed writes, immutable memtables, SSTables, bloom filters, and
  leveled compaction.
- ACID multi-key transactions with snapshot isolation and first-committer-wins
  write-conflict detection.
- Synchronous `close!` and exclusive ownership of configured storage
  directories.
- Versioned WAL, SSTable, and generational manifest formats with crash recovery
  and manifest rotation.
- Jepsen verification of snapshot isolation, register linearizability, and
  acknowledged-write durability across graceful restarts and `SIGKILL`.

### Changed

- Manifest edit encoding now uses IgelDB's binary format inside its length-and-CRC
  frame instead of Fressian.
- Bloom filters use Blossom 2's zero-dependency `byte[]` API.
- The memtable is an immutable sorted map behind an atom, allowing lock-free
  reads.
- Filesystem durability uses `FileChannel.force(true)`.

### Removed

- The `org.clojure/data.fressian` dependency.

### Compatibility and limitations

- Snapshot isolation is not serializability; write skew remains possible when
  concurrent transactions update disjoint keys.
- Data directories written by the earlier unversioned storage format are
  rejected and are not upgraded automatically.
- `SIGKILL` crash tests do not model sudden power loss or loss of data retained
  only in the operating system's page cache.

[Unreleased]: https://github.com/igel-data/igeldb/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/igel-data/igeldb/releases/tag/v1.0.0
