use std::collections::HashMap;

/// The 'KvStore' stores string key/value pairs.
/// 
/// key/value pairs are stored in a 'HashMap' in memory and not persisted to disk.
/// 
/// Example:
/// 
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// 
/// ```

pub struct KvStore {
    inner: HashMap<String, String>,
}


impl KvStore {

    /// Create a 'KvStore'
    pub fn new() -> Self {
        KvStore { inner: Default::default() }
    }

    /// Sets the value of a string key to a sting
    /// if the keys already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) {
        let _ = self.inner.insert(key, value);
    }

    /// Get the value of a given sting key.
    pub fn get(&self, key: String) -> Option<String> {
        self.inner.get(&key).map(|res| res.to_owned())
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) {
        let _ = self.inner.remove(&key);
    }

}