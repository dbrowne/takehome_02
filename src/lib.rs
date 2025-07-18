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

        //todo: implement reader.lines().flatten()
        for line in reader.lines() {
          if let Ok(line) = line {
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
    let entry = LogEntry::Set { key: key, value: value.clone() };

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

  #[test]
  fn test_set_get_delete() {}
  #[test]
  fn test_rude_restart() {}
}
