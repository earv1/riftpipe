//! The `process` file (DESIGN.md §17.6) — a single sidecar that reports **all**
//! in-memory resources at once: one line per resource with its byte **size** and
//! content **hash**. Like the metrics file, it's decoupled from the sync loop —
//! a side-car task refreshes it; you `cat`/watch it whenever you want. No
//! payload bytes are written, only size + hash, so it's cheap and safe to leave
//! running.
//!
//! Format (one line per resource, tab-separated):
//!   <name>\t<size-bytes>\t<blake3-hex-16>

use std::sync::Arc;
use std::time::Duration;

use crate::sync::backing::MemoryRegistry;

/// Short content fingerprint (first 16 bytes of blake3, hex).
fn short_hash(bytes: &[u8]) -> String {
    let h = blake3::hash(bytes);
    h.as_bytes()[..16].iter().map(|b| format!("{b:02x}")).collect()
}

/// Render the whole in-memory set as the file's contents.
pub fn format(entries: &[(String, Vec<u8>)]) -> String {
    let mut out = String::new();
    for (name, bytes) in entries {
        out.push_str(&format!("{}\t{}\t{}\n", name, bytes.len(), short_hash(bytes)));
    }
    out
}

/// Write the current snapshot once.
pub fn write_once(path: &str, registry: &MemoryRegistry) {
    let _ = std::fs::write(path, format(&registry.snapshot()));
}

/// Spawn a decoupled task that refreshes `path` every ~1s from the registry.
/// Independent of the sync loop, so it never blocks syncing.
pub fn spawn(path: String, registry: MemoryRegistry) {
    let registry = Arc::new(registry);
    tokio::spawn(async move {
        loop {
            write_once(&path, &registry);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_size_and_hash_per_resource() {
        let entries = vec![
            ("a.bin".to_string(), b"hello".to_vec()),
            ("b.bin".to_string(), b"".to_vec()),
        ];
        let s = format(&entries);
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("a.bin\t5\t"));
        assert!(lines[1].starts_with("b.bin\t0\t"));
        // hash column is 32 hex chars (16 bytes)
        assert_eq!(lines[0].split('\t').nth(2).unwrap().len(), 32);
    }

    #[test]
    fn reflects_the_memory_registry() {
        use crate::sync::backing::Backing;
        let reg = MemoryRegistry::new();
        let mut a = reg.backing("doc");
        a.store(b"in memory bytes");
        let s = format(&reg.snapshot());
        assert!(s.starts_with("doc\t15\t"));
    }
}
