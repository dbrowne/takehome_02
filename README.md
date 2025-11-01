# Take-home Assignment

Given the provided trait representing a key-value store, implement a struct KVLog that implements KVStore with the properties:
- reentrant (when shared across either threads or async tasks)
- backed by a log (filesystem is sufficient for this purpose)
- persistence is guaranteed before access, e.g. [Write Ahead Log](https://en.wikipedia.org/wiki/Write-ahead_logging) or equivalent guarantee
- will load the persisted state on startup

Please include the following:
- brief description of implementation decisions, including:
  - what is persisted (files, directories) and any significant tradeoffs
  - choices about contention and access control (e.g. Mutexes, Marker files, etc.)
  - assurances that recovery will always be in a good state, e.g. no partial writes
- basic tests for the above properties
- bonus: tests with multiple async tasks, single and multi-threaded executor
- extra bonus: thoughts on the interface (e.g. trait_variant, non-mut get and delete, return value on set, etc.)



# Implementation Checklist

## ✅ Core Implementation Requirements - FULFILLED

- **KVLog struct that implements KVStore**: ✅ Implemented
- **Reentrant**: ✅ Uses `Arc<RwLock<HashMap>>` and `Arc<tokio::sync::Mutex>`
- **Backed by a log**: ✅ Uses filesystem with JSON format
- **Write-Ahead Logging**: ✅ Writes to log with `fsync` before updating memory
- **Loads persisted state on startup**: ✅ Implemented in `load()` method

## ✅ Testing Requirements - FULFILLED

- **Basic tests**: ✅ `test_set_get_delete`
- **Persistence/recovery**: ✅ `test_rude_restart`
- **Concurrent async tasks**: ✅ `test_concurrent_access`
- **Multi-threaded executor**: ✅ `test_multi_threaded_executor`
- **Additional tests**: ✅ `test_persistence_verification`, `test_reentrancy`

## ✅ Code Coverage (not required) - 100% Function coverage 96% Line Coverage
-  llvm version 19.1.7-rust-1.86.0-stable

## ✅ Documentation Requirements FULFILLED

- **Brief description of implementation decisions**:  ✅ Documented in KVLog struct docs
- **What is persisted**: ✅ "A simple append-only log file containing JSON-serialized operations"
- **Choices about contention and access control**: ✅ Documented concurrency model with RwLock and AsyncMutex
- **Assurances about recovery**: ✅ "Line-based format prevents partial writes"
- **Thoughts on the interface**: ✅ Extensive thoughts in impl block about trait_variant, error handling, and API design
