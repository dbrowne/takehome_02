use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;

// Given the following trait representing a key-value store 
#[trait_variant::make(KVStore: Send)]
pub trait LocalKVStore<K, V>
where K: Serialize + DeserializeOwned + Send + Sync,
      V: Serialize + DeserializeOwned + Send + Sync,
{
    async fn get(&self, key: K) -> Option<V>;
    // should return the prior value if it exists
    async fn set(&self, key: K, value: V) -> Option<V>;
    async fn delete(&self, key: K);
}

// implement a version of struct KVLog that implements KVStore with the properties:
// - reentrant (when shared across either threads or async tasks)
// - backed by a log (filesystem is sufficient for this purpose)
// - persistence is guaranteed before access (e.g. Write Ahead Log or equivalent guarantee)
// - will load the persisted state on startup
pub struct KVLog<K, V> {
    pub _key: PhantomData<K>,
    pub _value: PhantomData<V>,
}

impl <K, V> KVLog<K, V> {
    pub fn load(_path: &str) -> Self {
        KVLog {
            _key: PhantomData,
            _value: PhantomData,
        }
    }   
}

impl<K, V> KVStore<K, V> for KVLog<K, V>
where K: Serialize + DeserializeOwned + Send + Sync,
      V: Serialize + DeserializeOwned + Send + Sync {
    async fn get(&self, _key: K) -> Option<V> {
        // Implement the logic to get a value by key
        None
    }

    async fn set(&self, _key: K, _value: V) -> Option<V> {
        // Implement the logic to set a value by key
        None
    }

    async fn delete(&self, _key: K) {
        // Implement the logic to delete a value by key
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
#[cfg(test)]
mod tests {

    #[test]
    fn test_set_get_delete() {
    }
    #[test]
    fn test_rude_restart() {
    }
}
