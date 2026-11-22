//! Where a resource's bytes live (DESIGN.md §17.5). The sync algorithm
//! ([`SyncStrategy`](crate::sync::strategy::SyncStrategy)) only ever sees `&[u8]` — it doesn't
//! care whether those bytes are mirrored to a file on disk or held purely in
//! RAM. That choice is this seam:
//!
//!   * [`FileBacking`]   — read/write a path (the original mirror behavior).
//!   * [`MemoryBacking`] — hold bytes in process memory, never touching disk.
//!
//! The two **coexist**: a run (or, later, a per-resource manifest entry) picks
//! which a resource uses. In-memory resources register with a
//! [`MemoryRegistry`] so the `process` observability file (DESIGN.md §17.6) can
//! report what we're holding — size + hash — out of band.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// A resource's byte store. `load` reads the current local bytes (to feed
/// `SyncStrategy::observe`); `store` materializes merged bytes back.
pub trait Backing: Send {
    fn name(&self) -> &str;
    fn load(&self) -> Vec<u8>;
    fn store(&mut self, bytes: &[u8]);
    /// True for backings that mirror a file on disk (only those can be deleted
    /// out from under us, and only those are persisted in the folder index).
    fn on_disk(&self) -> bool {
        false
    }
    /// Whether the resource currently exists in its store. File: is the file
    /// on disk (missing = a local delete). Memory: always true.
    fn exists(&self) -> bool {
        true
    }
    /// Delete the resource's bytes from its store (applying a peer's delete).
    fn remove(&mut self);
}

/// File-backed: the resource is a file on disk (today's mirror behavior).
pub struct FileBacking {
    name: String,
    path: PathBuf,
}

impl FileBacking {
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        FileBacking {
            name: name.into(),
            path: path.into(),
        }
    }
}

impl Backing for FileBacking {
    fn name(&self) -> &str {
        &self.name
    }
    fn load(&self) -> Vec<u8> {
        std::fs::read(&self.path).unwrap_or_default()
    }
    fn store(&mut self, bytes: &[u8]) {
        // A peer-discovered path may live in a not-yet-existing subdirectory.
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(&self.path, bytes);
    }
    fn on_disk(&self) -> bool {
        true
    }
    fn exists(&self) -> bool {
        self.path.is_file()
    }
    fn remove(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Memory-backed: bytes live in RAM behind a shared handle so the `process`
/// observer can read them without going through the sync loop.
pub struct MemoryBacking {
    name: String,
    buf: Arc<Mutex<Vec<u8>>>,
}

impl MemoryBacking {
    pub fn new(name: impl Into<String>) -> Self {
        MemoryBacking {
            name: name.into(),
            buf: Arc::new(Mutex::new(Vec::new())),
        }
    }
    /// A shared handle to the bytes (for the process observer).
    pub fn handle(&self) -> Arc<Mutex<Vec<u8>>> {
        self.buf.clone()
    }
}

impl Backing for MemoryBacking {
    fn name(&self) -> &str {
        &self.name
    }
    fn load(&self) -> Vec<u8> {
        self.buf.lock().unwrap().clone()
    }
    fn store(&mut self, bytes: &[u8]) {
        *self.buf.lock().unwrap() = bytes.to_vec();
    }
    fn remove(&mut self) {
        self.buf.lock().unwrap().clear();
    }
}

/// Tracks every in-memory resource so the `process` file can report **all** of
/// them in one place (DESIGN.md §17.6). Cloneable (shared inner) so the sync
/// side and the observer side hold the same view.
#[derive(Clone, Default)]
pub struct MemoryRegistry {
    inner: Arc<Mutex<Vec<(String, Arc<Mutex<Vec<u8>>>)>>>,
}

impl MemoryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a memory backing for `name` and register it for observation.
    pub fn backing(&self, name: impl Into<String>) -> MemoryBacking {
        let b = MemoryBacking::new(name);
        self.inner
            .lock()
            .unwrap()
            .push((b.name().to_string(), b.handle()));
        b
    }

    /// A point-in-time copy of every registered resource's `(name, bytes)`.
    pub fn snapshot(&self) -> Vec<(String, Vec<u8>)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(n, h)| (n.clone(), h.lock().unwrap().clone()))
            .collect()
    }

    /// Stop reporting `name` (its resource was deleted).
    pub fn remove(&self, name: &str) {
        self.inner.lock().unwrap().retain(|(n, _)| n != name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_backing_round_trips() {
        let mut b = MemoryBacking::new("r");
        assert!(b.load().is_empty());
        b.store(b"hello");
        assert_eq!(b.load(), b"hello");
        // the shared handle reflects writes
        assert_eq!(&*b.handle().lock().unwrap(), b"hello");
    }

    #[test]
    fn registry_reports_all_in_memory_resources() {
        let reg = MemoryRegistry::new();
        let mut a = reg.backing("a.bin");
        let mut c = reg.backing("c.bin");
        a.store(b"aaa");
        c.store(b"cccc");
        let mut snap = reg.snapshot();
        snap.sort();
        assert_eq!(
            snap,
            vec![
                ("a.bin".to_string(), b"aaa".to_vec()),
                ("c.bin".to_string(), b"cccc".to_vec()),
            ]
        );
    }

    #[test]
    fn file_backing_exists_and_remove() {
        let dir = std::env::temp_dir().join(format!("riftpipe-backing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.bin");
        std::fs::write(&path, b"hi").unwrap();

        let mut b = FileBacking::new("f.bin", path.clone());
        assert!(b.on_disk());
        assert!(b.exists());
        b.remove();
        assert!(!b.exists());
        assert!(!path.exists());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn memory_backing_remove_and_registry_remove() {
        let reg = MemoryRegistry::new();
        let mut a = reg.backing("a");
        a.store(b"x");
        a.remove();
        assert!(a.load().is_empty());
        reg.remove("a");
        assert!(reg.snapshot().is_empty());
        assert!(!MemoryBacking::new("m").on_disk());
    }
}
