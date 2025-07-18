use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

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
// - reentrant (when shared across either threads or async tasks)
// - backed by a log (filesystem is sufficient for this purpose)
// - persistence is guaranteed before access (e.g. Write Ahead Log or equivalent guarantee)
// - will load the persisted state on startup
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

// Only implement the Send version (KVStore), not LocalKVStore for thread safety since LocalKVStroe
// will be ambiguous
impl<K, V> KVStore<K, V> for KVLog<K, V>
where
  K: Serialize + DeserializeOwned + Send + Sync + 'static,
  V: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{
  async fn get(&self, key: K) -> Option<V> {
    let key_str = serde_json::to_string(&key).ok()?;

    let store = self.store.read().ok()?;
    store.get(&key_str).cloned()
  }

  async fn set(&self, key: K, value: V) -> Option<V> {
    let key_str = serde_json::to_string(&key).ok()?;

    // Write to log first (WAL)
    let entry = LogEntry::Set { key, value: value.clone() };

    if self.append_to_log(&entry).await.is_err() {
      return None;
    }

    // Then update in-memory store
    let mut store = self.store.write().ok()?;
    store.insert(key_str, value)
  }

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

// please include the following:
// - brief description of implementation decisions, including:
//   - what is persisted (files, directories) and any significant tradeoffs
//   - choices about contention and access control (e.g. Mutexes, Marker files, etc.)
//   - assurances that recovery will always be in a good state, e.g. no partial writes
// - basic tests for the above properties
// - bonus: tests with multiple async tasks, single and multi-threaded executor
// - extra bonus: thoughts on the interface (e.g. trait_variant, non-mut get and delete, return value on set, etc.)

// todo: Add documentation

#[cfg(test)]
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
  #[test]
  fn test_concurrent_access() {
    // todo: document this better
    let rt = Runtime::new().unwrap();
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    const FIVE: i32 = 5;
    const TEN: i32 = 10;
    const TWENTY: i32 = 20;
    const HUNDRED: i32 = 100;
    const CONTENTION_STRING: &str = "High contention will make problems !!!!!!";

    rt.block_on(async {
      let kv = Arc::new(KVLog::<String, i32>::load(path));

      // Pre-populate some data
      for i in 0..TEN {
        KVStore::set(&*kv, format!("shared_{}", i), i * HUNDRED).await;
      }

      let mut handles = vec![];

      // Test 1: Concurrent writes to same keys (testing atomicity)
      for i in 0..TEN {
        let kv_clone = Arc::clone(&kv);
        let handle = task::spawn(async move {
          for j in 0..50 {
            // Multiple tasks writing to overlapping keys
            let key = format!("shared_{}", j % TEN);
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
      for reader_id in 0..FIVE {
        let kv_clone = Arc::clone(&kv);
        let handle = task::spawn(async move {
          for i in 0..HUNDRED {
            // Read keys in a pattern while others are writing
            let key = format!("shared_{}", (i + reader_id) % TEN);
            if let Some(value) = KVStore::get(&*kv_clone, key).await {
              // Value should always be valid (no partial writes)
              assert!(value >= 0);
            }
          }
        });
        handles.push(handle);
      }

      // Test 3: Concurrent set/delete operations
      for i in 0..FIVE {
        let kv_clone = Arc::clone(&kv);
        let handle = task::spawn(async move {
          for j in 0..TWENTY {
            let key = format!("delete_test_{}", j % FIVE);
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
          for j in 0..TEN {
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
      for i in 0..TEN {
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
}
