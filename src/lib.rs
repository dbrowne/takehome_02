//! # KVLog - A Write-Ahead Logging Key-Value Store
//!
//! `kvlog` intends top provide a thread-safe persisting kv store with write-ahead logging
//! guarantees. Set and Delete operations are durably persisted before acknowledgment.
//!
//! ## Example
//! ```ignore
//! # use kvlog::{KVLog, KVStor:#![warn()]e};
//! # tokio_test::block_on(async {
//! let kv: KVLog<String, String> = KVLog::load("my_data.log");
//! kv.set("key".to_string(), "value".to_string()).await;
//! assert_eq!(kv.get("key".to_string()).await, Some("value".to_string()));
//! # });
//! ```

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// todo:  thoughts on trait variance:


// Given the following trait representing a key-value store
#[trait_variant::make(KVStore: Send)]
pub trait LocalKVStore<K, V>
where
  K: Serialize + DeserializeOwned + Send + Sync,
  V: Serialize + DeserializeOwned + Send + Sync,
{
  async fn get(&self, key: K) -> Option<V>;
  // should return the prior value if it exists
  async fn set(&self, key: K, value: V) -> Option<V>;
  async fn delete(&self, key: K);
}

// Log entry types for WAL
#[derive(Debug, Serialize, Deserialize)]
enum LogEntry<K, V> {
  Set { key: K, value: V },
  Delete { key: K },
}

// implement a version of struct KVLog that implements KVStore with the properties:

/// A persistent key-value store backed by a write-ahead log.
///
/// # Design Decisions based on the following requirements:
/// - reentrant (when shared across either threads or async tasks)
/// - backed by a log (filesystem is sufficient for this purpose)
/// - persistence is guaranteed before access (e.g. Write Ahead Log or equivalent guarantee)
/// - will load the persisted state on startup
///
/// ## Persistence Strategy
/// - A simple append-only log file containing JSON-serialized operations
/// - Each line represents one atomic operation
/// - Operations are written with `fsync` before updating in-memory state
/// - For this implementation there are two  tradeoffs:
///   -- efficiency: logging should be in a binary format to minimize data storage
///   -- lacking temporal information. No time stamp which can impedede debugging
///
/// ## Concurrency Model
/// - `RwLock<HashMap>` allows multiple concurrent readers
/// - Separate `AsyncMutex` serializes log writes
/// - Safe for use across threads and async tasks
///
/// ## Recovery Guarantees
/// - Line-based format prevents partial writes
/// - Corrupted entries are skipped during recovery
/// - Log is replayed sequentially to rebuild state
///
/// ## Data  will be stored in the following  format:
// - {"Set":{"key":"key1","value":"value0"}}
// - {"Delete":{"key":"key2"}}
///
pub struct KVLog<K, V> {
  // In-memory store protected by RwLock for concurrent reads
  store: Arc<RwLock<HashMap<String, V>>>,
  // Mutex for serializing write operations to the log
  log_write_mutex: Arc<tokio::sync::Mutex<()>>,
  // Path to the log file
  log_path: PathBuf,
  pub _key: PhantomData<K>,
  pub _value: PhantomData<V>,
}

impl<K, V> KVLog<K, V>
where
  K: Serialize + DeserializeOwned + Send + Sync,
  V: Serialize + DeserializeOwned + Send + Sync + Clone,
{
  /// Loads or creates a new KVLog at the specified path.
  ///
  /// If the log file exists, it will be replayed to restore state.
  /// Invalid entries are silently skipped during recovery.
  ///
  /// # Examples
  /// ```
  /// # use kv_log::KVLog;
  /// let kv: KVLog<String, i32> = KVLog::load("/tmp/my_data.log");
  /// ```
  pub fn load(path: &str) -> Self {
    let log_path = PathBuf::from(path);
    let store = Arc::new(RwLock::new(HashMap::new()));

    // Load existing log entries if the file exists
    if log_path.exists() {
      if let Ok(file) = File::open(&log_path) {
        let reader = BufReader::new(file);
        let mut temp_store = HashMap::new();

        for line in reader.lines().flatten() {
          if line.trim().is_empty() {
            continue;
          }

          if let Ok(entry) = serde_json::from_str::<LogEntry<K, V>>(&line) {
            match entry {
              LogEntry::Set { key, value } => {
                if let Ok(key_str) = serde_json::to_string(&key) {
                  temp_store.insert(key_str, value);
                }
              }
              LogEntry::Delete { key } => {
                if let Ok(key_str) = serde_json::to_string(&key) {
                  temp_store.remove(&key_str);
                }
              }
            }
          }
        }
        // Update the store with loaded data
        if let Ok(mut store_guard) = store.write() {
          *store_guard = temp_store;
        }
      }
    }

    KVLog {
      store,
      log_write_mutex: Arc::new(tokio::sync::Mutex::new(())),
      log_path,
      _key: PhantomData,
      _value: PhantomData,
    }
  }

  // Helper method to append entry to log with fsync
  async fn append_to_log(&self, entry: &LogEntry<K, V>) -> std::io::Result<()> {
    let _guard = self.log_write_mutex.lock().await;

    let mut file = OpenOptions::new().create(true).append(true).open(&self.log_path)?;

    let json = serde_json::to_string(entry)?;
    writeln!(file, "{}", json)?;

    // Ensure data is persisted to disk
    file.sync_all()?;

    Ok(())
  }
}

/// Implementation of the thread-safe key-value store operations.
///
/// This implementation provides atomic, durable operations using Write-Ahead Logging (WAL).
/// Only the `KVStore` trait (with Send bounds) is implemented to avoid trait method ambiguity
/// that would occur if both `LocalKVStore` and `KVStore` were implemented.
///
/// # Type Constraints
///
/// The implementation requires strict bounds to ensure thread safety and serializability:
///
/// - `K: Serialize + DeserializeOwned` - Keys must be serializable to JSON for storage
/// - `K: Send + Sync + 'static` - Keys must be thread-safe and owned (no borrowed data)
/// - `V: Clone` - Values must be cloneable for the WAL pattern (write before update)
/// - `V: Send + Sync + 'static` - Values must be thread-safe and owned
///
/// # Design Decisions
///
/// 1. **Single Trait Implementation**: Only `KVStore` (not `LocalKVStore`) prevents the
///    compiler error E0034 about ambiguous method calls.
///
/// 2. **Key Serialization**: Keys are serialized to JSON strings for HashMap storage.
///    This allows complex key types but adds overhead for simple keys.
///
/// 3. **WAL Pattern**: All mutations follow: Log → Sync → Memory Update
///
/// 4. **Error Handling**: Operations return `None` on failure rather than `Result` to
///    match the trait interface. Errors are logged to stderr for debugging.
impl<K, V> KVStore<K, V> for KVLog<K, V>
where
  K: Serialize + DeserializeOwned + Send + Sync + 'static,
  V: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{

  /// Retrieve a value from the store by key.
  ///
  /// This operation is read-only and does not write to the log. Multiple concurrent
  /// reads can proceed in parallel thanks to the `RwLock`.
  ///
  /// # Arguments
  ///
  /// * `key` - The key to look up, will be serialized to JSON for storage
  ///
  /// # Returns
  ///
  /// * `Some(value)` - The cloned value if the key exists
  /// * `None` - If the key doesn't exist or any error occurs:
  ///   - Key serialization fails (rare for well-formed types)
  ///   - Lock is poisoned (after a panic in another thread)
  ///
  /// # Note
  ///
  /// Read operations are not logged as they don't mutate state. In a production
  /// system, you might want to log reads for audit trails or access patterns.
  async fn get(&self, key: K) -> Option<V> {
    let key_str = serde_json::to_string(&key).ok()?;

    let store = self.store.read().ok()?;
    store.get(&key_str).cloned()
  }

  /// Sets a key-value pair in the store, returning the previous value.
  ///
  /// This operation follows the Write-Ahead Logging pattern:
  /// 1. Serialize the operation to the log
  /// 2. Sync to disk (fsync)
  /// 3. Update in-memory state
  ///
  /// This ensures durability - even if the process crashes after step 2,
  /// the operation can be recovered on restart.
  ///
  /// # Arguments
  ///
  /// * `key` - The key to set
  /// * `value` - The value to store (will be cloned for the log)
  ///
  /// # Returns
  ///
  /// * `Some(old_value)` - The previous value if the key existed
  /// * `None` - If this is a new key OR if any error occurs:
  ///   - Key serialization fails
  ///   - Log write fails (disk full, permissions, I/O error)
  ///   - Lock is poisoned
  ///
  /// # Error Handling
  ///
  /// Errors are logged to stderr but the operation returns `None` to maintain
  /// the trait interface. This is a limitation - I would use `Result` in a production
  /// interface
  async fn set(&self, key: K, value: V) -> Option<V> {
    let key_str = serde_json::to_string(&key).ok()?;

    // Write to log first (WAL)
    let entry = LogEntry::Set { key, value: value.clone() };

    if let Err(e) = self.append_to_log(&entry).await {
      eprintln!("[KVLog] Failed to persist set operation for key - {}: {}", key_str, e);
      return None;
    }
    // Then update in-memory store
    let mut store = self.store.write().ok()?;
    store.insert(key_str, value)
  }

  /// Removes a key-value pair from the store.
  ///
  /// Like `set`, this follows the WAL pattern: log first, then update memory.
  /// Unlike `set`, this operation doesn't return the deleted value.
  ///
  /// # Arguments
  ///
  /// * `key` - The key to delete
  ///
  /// # Design Note
  ///
  /// This method doesn't return the deleted value, which is inconsistent with
  /// `set` returning the old value and HashMap's behavior. This is a limitation
  /// of the trait design and should return an  `Option<V>` to be consistent
  ///
  /// # Error Handling::  NOTE!!!! THIS IS PROBLEMATIC!
  /// ## Errors are silently ignored since since it does not return a value!!
  /// Would re write this to return a 'Result' since delete does not provide any indication of success
  ///
  async fn delete(&self, key: K) {
    let key_str = match serde_json::to_string(&key) {
      Ok(s) => s,
      Err(_) => return,
    };

    // Write to log first (WAL)
    let entry = LogEntry::Delete { key };

    if self.append_to_log(&entry).await.is_err() {
      return;
    }

    // Then update in-memory store
    if let Ok(mut store) = self.store.write() {
      store.remove(&key_str);
    }
  }
}


#[cfg(test)]
/// Tests the fundamental CRUD operations of the KVLog implementation. (AI generated doc)
///
/// This test validates the core functionality of the key-value store:
/// 1. **Create**: Setting new key-value pairs
/// 2. **Read**: Retrieving values by key
/// 3. **Update**: Overwriting existing values
/// 4. **Delete**: Removing key-value pairs
///
/// The test uses a temporary file to ensure isolation from other tests and
/// verifies that all operations work correctly in a single-threaded async context.
///
/// # Test Structure
///
/// The test is organized into four distinct sections, each validating a specific
/// aspect of the KVStore behavior:
///
/// 1. **Basic Set/Get** - Confirms new keys can be stored and retrieved
/// 2. **Value Overwriting** - Validates that updates return the old value
/// 3. **Deletion** - Ensures keys can be removed completely
/// 4. **Multi-Key Operations** - Verifies the store handles multiple keys correctly
///
/// # Why This Test Matters
///
/// This is the most fundamental test in the suite. If this fails, nothing else
/// can be trusted. It validates:
/// - The basic API contract works as expected
/// - The in-memory store correctly maintains state
/// - The async runtime integration functions properly
/// - Key serialization/deserialization works for simple strings
mod tests {
  use super::{KVLog, KVStore};
  use std::sync::Arc;
  use tempfile::NamedTempFile;
  use tokio::runtime::Runtime;
  use tokio::task;


  #[test]
   fn test_set_get_delete() {
    let rt = Runtime::new().unwrap();
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let test_key_1 = "key_one";
    let test_key_2 = "Second key";
    let test_val_1 = "Test Value 1";
    let test_val_2 = "something completely different";
    let test_key_3 = "third key";
    let test_key_4 = "Four";
    let test_val_3 = "Lorem";
    let test_val_4 = "ipsum";

    rt.block_on(async {
      let kv: KVLog<String, String> = KVLog::load(path);

      // Test set and get
      let old_val = KVStore::set(&kv, test_key_1.to_string(), test_val_1.to_string()).await;
      assert_eq!(old_val, None);

      let val = KVStore::get(&kv, test_key_1.to_string()).await;

      assert_eq!(val, Some(test_val_1.to_string()));

      // Test overriteing values

      let old_val = KVStore::set(&kv, test_key_1.to_string(), test_val_2.to_string()).await;
      assert_eq!(old_val, Some(test_val_1.to_string()));

      let val = KVStore::get(&kv, test_key_1.to_string()).await;
      assert_eq!(val, Some(test_val_2.to_string()));

      // Test deletion
      KVStore::delete(&kv, test_key_1.to_string()).await;
      let val = KVStore::get(&kv, test_key_1.to_string()).await;
      assert_eq!(val, None);

      // Sanity check:  confirm we can work with multiple keys and values

      let _ = KVStore::set(&kv, test_key_1.to_string(), test_val_1.to_string()).await;
      let _ = KVStore::set(&kv, test_key_2.to_string(), test_val_2.to_string()).await;
      let _ = KVStore::set(&kv, test_key_3.to_string(), test_val_3.to_string()).await;
      let _ = KVStore::set(&kv, test_key_4.to_string(), test_val_4.to_string()).await;

      let val = KVStore::get(&kv, test_key_3.to_string()).await;
      assert_eq!(val, Some(test_val_3.to_string()));
      let val = KVStore::get(&kv, test_key_2.to_string()).await;
      assert_ne!(val, Some(test_key_4.to_string()));
    });
  }

  /// Tests crash recovery and persistence guarantees after an unexpected shutdown. (AI generated doc)
  ///
  /// This test simulates a "rude" (ungraceful) shutdown where the process terminates
  /// without proper cleanup - similar to a power failure, kill -9, or panic. It validates
  /// that the Write-Ahead Logging (WAL) implementation correctly preserves data even
  /// when the process doesn't shut down cleanly.
  ///
  /// # Test Scenario
  ///
  /// The test simulates a real-world failure pattern:
  /// 1. **Session 1**: Performs various operations then crashes
  /// 2. **Session 2**: Restarts and verifies all data was preserved
  ///
  /// This is critical for a storage system - users expect their data to survive
  /// crashes, power failures, and other unexpected terminations.
  ///
  /// # What Makes This a "Rude" Restart?
  ///
  /// The first runtime and KVLog instance are dropped without:
  /// - Calling any cleanup methods
  /// - Flushing buffers explicitly
  /// - Graceful shutdown procedures
  ///
  /// The scope block forces the runtime to be dropped immediately, simulating
  /// a crash at an arbitrary point in the program.
  ///
  /// # Operations Tested
  ///
  /// The test performs a specific sequence to validate different scenarios:
  /// 1. **Initial writes**: Set a=1, b=2, c=3
  /// 2. **Deletion**: Delete b (tests that deletes are persisted)
  /// 3. **Update**: Change a from 1 to 10 (tests that updates are persisted)
  ///
  /// Final expected state:
  /// - a = 10 (updated value)
  /// - b = None (deleted)
  /// - c = 3 (unchanged)
  ///
  /// # Why This Test Matters
  ///
  /// Without proper WAL implementation, any of these could happen:
  /// - Data could be lost (operations only in memory)
  /// - Partial writes could corrupt the store
  /// - The store could revert to an earlier state
  /// - Deletes might not be persisted
  ///
  /// This test proves that every operation is durably persisted before acknowledgment.
  #[test]
  fn test_rude_restart() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    // 1st time write some data
    {
      let rt = Runtime::new().unwrap();
      rt.block_on(async {
        let kv: KVLog<String, i32> = KVLog::load(path);
        KVStore::set(&kv, "a".to_string(), 1).await;
        KVStore::set(&kv, "b".to_string(), 2).await;
        KVStore::set(&kv, "c".to_string(), 3).await;
        KVStore::delete(&kv, "b".to_string()).await;
        KVStore::set(&kv, "a".to_string(), 10).await;
      });
      // let the runtime get dropped so it is a rude shutdown
    }
    // Second session: verify data persisted correctly
    {
      let rt = Runtime::new().unwrap();
      rt.block_on(async {
        let kv: KVLog<String, i32> = KVLog::load(path);

        assert_eq!(KVStore::get(&kv, "a".to_string()).await, Some(10));
        assert_eq!(KVStore::get(&kv, "b".to_string()).await, None);
        assert_eq!(KVStore::get(&kv, "c".to_string()).await, Some(3));
      });
    }
  }

  /// Tests thread safety and concurrent access patterns under high contention. (AI gened doc)
  ///
  /// This is a comprehensive stress test that validates the KVLog implementation's
  /// ability to handle multiple concurrent operations without data corruption,
  /// deadlocks, or race conditions. It specifically tests the synchronization
  /// mechanisms (RwLock for reads, Mutex for writes) under various access patterns.
  ///
  /// # Test Design
  ///
  /// The test uses specific constants to create predictable contention patterns:
  /// - `DATA_ELEMENTS = 17`: A prime number to ensure good key distribution
  /// - `THREAD_SPAWN_COUNT = 9`: Odd number for alternating set/delete patterns
  /// - `CONTENTION_STRING`: A long key name to test string handling under contention
  ///
  /// # Test Scenarios
  ///
  /// The test orchestrates 5 different concurrent access patterns that could
  /// expose different types of synchronization bugs:
  ///
  /// 1. **Write-Write Conflicts**: Multiple tasks updating the same keys
  /// 2. **Read-Write Races**: Readers accessing data while it's being modified
  /// 3. **Set-Delete Races**: Concurrent creation and deletion of keys
  /// 4. **High Contention**: Many tasks competing for a single key
  /// 5. **Persistence Verification**: Ensures durability after concurrent ops
  ///
  /// # What This Test Validates
  ///
  /// - **Atomicity**: Operations complete fully or not at all
  /// - **Isolation**: Concurrent operations don't see partial states
  /// - **Consistency**: The store maintains valid state under contention
  /// - **Durability**: All acknowledged writes survive to disk
  ///
  /// # Why This Test Matters
  ///
  /// Concurrent bugs are notoriously hard to reproduce. This test creates
  /// enough contention to expose issues like:
  /// - Lost updates (write-write conflicts)
  /// - Torn reads (seeing partial writes)
  /// - Deadlocks (incorrect lock ordering)
  /// - Data races (unsynchronized access)
  /// - Persistence gaps (in-memory updates not reaching disk)
  #[test]
  fn test_concurrent_access() {
    let rt = Runtime::new().unwrap();
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    const TWENTY: i32 = 20;
    const HUNDRED: i32 = 100;
    const CONTENTION_STRING: &str = "High contention will make problems !!!!!!";

    const DATA_ELEMENTS: i32 = 17;
    const TREAD_SPAWN_COUNT: i32 = 9;

    rt.block_on(async {
      let kv = Arc::new(KVLog::<String, i32>::load(path));

      // Pre-populate some data
      for i in 0..DATA_ELEMENTS {
        KVStore::set(&*kv, format!("shared_{}", i), i * HUNDRED).await;
      }

      let mut handles = vec![];

      // Test 1: Concurrent writes to same keys (testing atomicity)
      for i in 0..DATA_ELEMENTS {
        let kv_clone = Arc::clone(&kv);
        let handle = task::spawn(async move {
          for j in 0..50 {
            // Multiple tasks writing to overlapping keys
            let key = format!("shared_{}", j % DATA_ELEMENTS);
            let old = KVStore::set(&*kv_clone, key.clone(), i * 1000 + j).await;

            // Verify old value was valid (either initial or from another task)
            if let Some(old_val) = old {
              assert!(old_val % HUNDRED == 0 || old_val >= 0);
            }
          }
        });
        handles.push(handle);
      }

      // Test 2: Concurrent readers during writes
      for reader_id in 0..TREAD_SPAWN_COUNT {
        let kv_clone = Arc::clone(&kv);
        let handle = task::spawn(async move {
          for i in 0..HUNDRED {
            // Read keys in a pattern while others are writing
            let key = format!("shared_{}", (i + reader_id) % DATA_ELEMENTS);
            if let Some(value) = KVStore::get(&*kv_clone, key).await {
              // Value should always be valid (no partial writes)
              assert!(value >= 0);
            }
          }
        });
        handles.push(handle);
      }

      // Test 3: Concurrent set/delete operations
      for i in 0..TREAD_SPAWN_COUNT {
        let kv_clone = Arc::clone(&kv);
        let handle = task::spawn(async move {
          for j in 0..TWENTY {
            let key = format!("delete_test_{}", j % TREAD_SPAWN_COUNT);
            if i % 2 == 0 {
              KVStore::set(&*kv_clone, key, i * HUNDRED + j).await;
            } else {
              KVStore::delete(&*kv_clone, key).await;
            }
          }
        });
        handles.push(handle);
      }

      // Test 4: High contention on single key
      for i in 0..TWENTY {
        let kv_clone = Arc::clone(&kv);
        let key = CONTENTION_STRING.to_string().clone();
        let handle = task::spawn(async move {
          for j in 0..10 {
            let value = i * HUNDRED + j;
            let _ = KVStore::set(&*kv_clone, key.clone(), value).await;

            // Immediately read back to verify
            let read_back = KVStore::get(&*kv_clone, key.clone()).await;
            assert!(read_back.is_some());

            // If we just set it, we might not read our value due to races
            // but we should read *some* valid value
            if let Some(read_val) = read_back {
              assert!(read_val >= 0);
            }
          }
        });
        handles.push(handle);
      }

      // Wait for all tasks to complete
      for handle in handles {
        handle.await.unwrap();
      }

      // Verify final state consistency
      // Shared keys should have some value from the concurrent writes
      for i in 0..DATA_ELEMENTS {
        let value = KVStore::get(&*kv, format!("shared_{}", i)).await;
        assert!(value.is_some());
        if let Some(v) = value {
          assert!(v >= 0); // Should be a valid value from some task
        }
      }

      // High contention key should exist with some value
      let contention_value = KVStore::get(&*kv, CONTENTION_STRING.to_string()).await;
      assert!(contention_value.is_some());

      // Verify persistence by checking the log file
      drop(kv); // Flush  pending writes
      let log_size = std::fs::metadata(path).unwrap().len();
      assert!(log_size > 0, "Log file should contain entries");

      // Test 5: Reload and verify data survived
      let kv_reloaded = KVLog::<String, i32>::load(path);
      let reloaded_value = KVStore::get(&kv_reloaded, CONTENTION_STRING.to_string()).await;
      assert_eq!(reloaded_value, contention_value, "Data should persist correctly");
    });
  }


  /// (AI generated documentation)
  /// Tests KVLog correctness across multiple OS threads sharing a multi-threaded Tokio runtime.
  ///
  /// This test is more demanding than typical async concurrency tests because it validates
  /// thread safety across actual OS threads, not just async tasks. It ensures the KVLog
  /// implementation is truly thread-safe at the system level, not just within a single-threaded
  /// async executor.
  ///
  /// # Test Architecture
  ///
  /// The test creates a unique scenario that stresses several aspects:
  /// 1. **Multi-threaded Tokio runtime**: 4 worker threads handle async tasks
  /// 2. **OS thread spawning**: 16 OS threads each submit work to the runtime
  /// 3. **Shared KVLog instance**: One Arc<KVLog> shared across all threads
  /// 4. **Cross-thread async execution**: OS threads schedule async work on Tokio threads
  ///
  /// This creates a complex execution environment where:
  /// - OS threads compete to submit tasks
  /// - Tokio worker threads compete to execute tasks
  /// - All threads share the same KVLog instance
  /// - Synchronization must work across both thread models
  ///
  /// # Why This Test Matters
  ///
  /// Many async applications use thread pools or spawn blocking threads that need
  /// to interact with async resources. This test validates that pattern by ensuring:
  /// - Arc<KVLog> can be safely shared across OS thread boundaries
  /// - The internal synchronization (RwLock, Mutex) works with OS threads
  /// - No data races occur between OS threads and Tokio worker threads
  /// - Memory ordering is correct across CPU cores
  ///
  /// # Test Pattern
  ///
  /// Each thread writes a non-overlapping set of keys to avoid false positives
  /// from accidental key collisions. The pattern `thread_X_key_Y` ensures:
  /// - No two threads write the same key
  /// - Easy verification of which thread wrote which data
  /// - Detection of any cross-thread data corruption
  ///
  /// # Constants Explained
  ///
  /// - `THREAD_COUNT = 16`: Exceeds typical CPU core counts to force contention
  /// - `ITERATION_COUNT = 50`: Enough operations to likely expose race conditions
  /// - `WORKER_THREAD_COUNT = 4`: Fewer Tokio threads than OS threads creates queuing
  #[test]
  fn test_multi_threaded_executor() {
    use std::thread;
    const THREAD_COUNT: i32 = 16;
    const ITERATION_COUNT: i32 = 50;
    const WORKER_THREAD_COUNT: usize = 4;

    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    // Create multi-threaded runtime
    let rt =
      tokio::runtime::Builder::new_multi_thread().worker_threads(WORKER_THREAD_COUNT).enable_all().build().unwrap();

   // This single log instance is shared accross all the threads
    let kv = Arc::new(KVLog::<String, String>::load(&path));

    // Spawn threads that will use the runtime
    let mut thread_handles = vec![];

    for i in 0..THREAD_COUNT {
      let kv_clone = Arc::clone(&kv);
      let rt_handle = rt.handle().clone(); // Allow external task submission

      let handle = thread::spawn(move || {
        rt_handle.block_on(async {
          for j in 0..ITERATION_COUNT {
            //
            let key = format!("thread_{}_key_{}", i, j);
            let value = format!("value_{}_{}", i, j);

            // write to disk
            KVStore::set(&*kv_clone, key.clone(), value.clone()).await;

            // Verify immediate consistency
            let retrieved = KVStore::get(&*kv_clone, key).await;
            assert_eq!(retrieved, Some(value));
          }
        });
      });
      thread_handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in thread_handles {
      handle.join().unwrap();
    }

    // Verify all data is present
    rt.block_on(async {
      for i in 0..THREAD_COUNT {
        for j in 0..ITERATION_COUNT {
          let key = format!("thread_{}_key_{}", i, j);
          let expected = format!("value_{}_{}", i, j);
          assert_eq!(KVStore::get(&*kv, key).await, Some(expected));
        }
      }
    });
  }

  /// (AI generated doc based on current constant values)
  /// Tests the persistence layer under high-volume operations with complex patterns.
  ///
  /// This test verifies that the Write-Ahead Log (WAL) correctly handles a large
  /// number of operations including overwrites and periodic deletes. It validates
  /// both the logical correctness of the final state and the physical presence
  /// of the log file.
  ///
  /// # Test Design
  ///
  /// The test performs 800 iterations of operations in a specific pattern designed
  /// to stress the logging mechanism:
  /// - Continuous overwrites of the same keys (tests log growth)
  /// - Periodic deletes (tests mixed operation types)
  /// - High operation count (tests scalability and performance)
  ///
  /// # Operation Pattern
  ///
  /// For each iteration i (0 to 799):
  /// 1. Set key1 = "value{i}"
  /// 2. Set key2 = "value{i}"
  /// 3. If i is divisible by 10, delete key2
  ///
  /// This creates a pattern where:
  /// - key1 is overwritten 800 times (testing update persistence)
  /// - key2 is set 800 times but deleted 81 times (at i=0,10,20,...,790, plus final)
  /// - The log grows continuously without compaction
  ///
  /// # Why This Test Matters
  ///
  /// Production systems often have "hot" keys that are updated frequently.
  /// This test ensures:
  /// - The log doesn't corrupt under high write volume
  /// - Overwrites don't cause data loss or inconsistency
  /// - Mixed set/delete patterns are handled correctly
  /// - The physical log file actually receives the data
  ///
  /// # Expected Behavior
  ///
  /// After all operations:
  /// - key1 should have the last value ("value799")
  /// - key2 should be deleted (None)
  /// - Log file should be non-empty and substantial in size
  ///
  /// The log file will contain approximately:
  /// - 800 Set operations for key1
  /// - 800 Set operations for key2
  /// - 81 Delete operations for key2
  /// Total: ~1,681 log entries
  ///
  /// # My additional comments: What is not covered in this test:
  /// 1. Log compaction: Would need to create a container with limited disk space beyond scope
  /// 2. Large value sizes:  Not really necessary for this exercise but would do this for a prod impl
  /// 3. Memory usage: I'm trusting the hadhmap implementation here but two keys would not cut it otherwise
  /// 4. Recovery after 1MM, 10MM writes (I would use a binary format)
  ///

  #[test]
  fn test_persistence_verification() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    let rt = Runtime::new().unwrap();
    const ITERATION_COUNT: i32 = 800;
    let test_val = format!("value{}", ITERATION_COUNT - 1);

    rt.block_on(async {
      let kv: KVLog<String, String> = KVLog::load(path);

      // Perform many operations
      for i in 0..ITERATION_COUNT {
        KVStore::set(&kv, "key1".to_string(), format!("value{}", i)).await;
        KVStore::set(&kv, "key2".to_string(), format!("value{}", i)).await;
        if i % 10 == 0 {
          KVStore::delete(&kv, "key2".to_string()).await;
        }
      }

      // Delete key2 one final time to ensure it's None
      KVStore::delete(&kv, "key2".to_string()).await;

      // Verify final state
      assert_eq!(KVStore::get(&kv, "key1".to_string()).await, Some(test_val));
      assert_eq!(KVStore::get(&kv, "key2".to_string()).await, None);
    });

    // Verify log file exists and contains entries
    let log_size = std::fs::metadata(path).unwrap().len();
    assert!(log_size > 0);
  }

  /// (AI generated doc)
  /// Tests reentrant access patterns where KVLog operations are called from nested async contexts.
  ///
  /// Reentrancy occurs when a function or system can be safely called while it's already
  /// executing. For async systems, this means handling nested async tasks that all access
  /// the same shared resource without deadlocking or corrupting state.
  ///
  /// # Test Architecture
  ///
  /// The test creates a nested task hierarchy:
  /// ```
  /// Main Task
  ///   └── Outer Task (writes OUTER key)
  ///         └── Inner Task (writes INNER key, reads OUTER key)
  ///         └── Reads INNER key after inner completes
  ///   └── Verifies both keys exist
  /// ```
  ///
  /// This pattern tests that:
  /// - Arc<KVLog> can be cloned and used in nested contexts
  /// - No deadlocks occur when tasks wait on other tasks using the same KVLog
  /// - Write operations from nested contexts are properly serialized
  /// - Read operations can see writes from parent contexts
  ///
  /// # Why Reentrancy Matters
  ///
  /// In real applications, you might have:
  /// - HTTP handlers that spawn background tasks
  /// - Background tasks that spawn sub-tasks for parallel work
  /// - Recursive algorithms implemented with async tasks
  /// - Event handlers that trigger other events
  ///
  /// All these patterns require reentrant access to shared resources.
  ///
  /// # What This Test Validates
  ///
  /// 1. **No Deadlocks**: Parent tasks can wait on child tasks without hanging
  /// 2. **Proper Synchronization**: Nested writes don't corrupt each other
  /// 3. **Value Propagation**: Child tasks see parent writes immediately
  /// 4. **Arc Safety**: Multiple Arc clones work correctly across task boundaries
  #[test]
  fn test_reentrancy() {
    let rt = Runtime::new().unwrap();
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    const VALUE1: &str = "FIRST_VALULE";
    const VALUE2: &str = "SECOMND VALUE";
    const INNER: &str = "inner_Thread";
    const OUTER: &str = "Outer Thread";

    rt.block_on(async {
      let kv = Arc::new(KVLog::<String, String>::load(path));

      // Test reentrancy - calling KV operations from within async context
      let kv_clone = Arc::clone(&kv);
      let handle = task::spawn(async move {
        KVStore::set(&*kv_clone, OUTER.to_string(), VALUE1.to_string()).await;

        // Nested task that also uses the KV store
        let kv_inner = Arc::clone(&kv_clone);
        let inner_handle = task::spawn(async move {
          KVStore::set(&*kv_inner, INNER.to_string(), VALUE2.to_string()).await;
          KVStore::get(&*kv_inner, OUTER.to_string()).await
        });

        let outer_value = inner_handle.await.unwrap();
        assert_eq!(outer_value, Some(VALUE1.to_string()));

        KVStore::get(&*kv_clone, INNER.to_string()).await
      });

      let inner_value = handle.await.unwrap();
      assert_eq!(inner_value, Some(VALUE2.to_string()));

      // Verify both values are present
      assert_eq!(KVStore::get(&*kv, OUTER.to_string()).await, Some(VALUE1.to_string()));
      assert_eq!(KVStore::get(&*kv, INNER.to_string()).await, Some(VALUE2.to_string()));
    });
  }
}
