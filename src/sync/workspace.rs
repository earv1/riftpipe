//! A folder **workspace** (DESIGN.md §17): a directory tree where each file is a
//! resource bound to a sync algorithm (per the [`Manifest`]) and a byte backing
//! (file on disk, or in-memory). The multiplexed session ([`super::folder`])
//! drives every resource over one link.
//!
//! Resources appear two ways: by **scanning** the directory (local files) and by
//! **discovery** (a frame arrives for a path we don't have yet — the peer has a
//! file we don't). Both funnel through [`Workspace::ensure`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::sync::backing::{Backing, FileBacking, MemoryRegistry};
use crate::sync::manifest::Manifest;
use crate::sync::syncer::{Kind, Syncer};

/// One synced thing: its algorithm + where its bytes live.
pub struct Resource {
    pub kind: Kind,
    pub syncer: Box<dyn Syncer>,
    pub backing: Box<dyn Backing>,
}

pub struct Workspace {
    root: PathBuf,
    manifest: Manifest,
    /// In-memory mode: hold bytes in RAM (and surface them via the `process`
    /// file) instead of mirroring to disk.
    memory: bool,
    registry: MemoryRegistry,
    resources: HashMap<String, Resource>,
}

impl Workspace {
    /// Open `root` with `manifest`. Seeds a resource for every file currently on
    /// disk (in memory mode, their bytes are read once into RAM).
    pub fn new(root: impl Into<PathBuf>, manifest: Manifest, memory: bool) -> std::io::Result<Self> {
        let mut ws = Workspace {
            root: root.into(),
            manifest,
            memory,
            registry: MemoryRegistry::new(),
            resources: HashMap::new(),
        };
        for rel in ws.scan_disk()? {
            ws.create(&rel);
        }
        Ok(ws)
    }

    /// The in-memory registry (for the `process` observability file).
    pub fn registry(&self) -> MemoryRegistry {
        self.registry.clone()
    }

    /// The workspace root directory (so the folder session can watch it for
    /// filesystem events instead of polling).
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Relative paths of all currently-known resources (sorted, for stable
    /// iteration / advertising).
    pub fn paths(&self) -> Vec<String> {
        let mut v: Vec<String> = self.resources.keys().cloned().collect();
        v.sort();
        v
    }

    pub fn get_mut(&mut self, rel: &str) -> Option<&mut Resource> {
        self.resources.get_mut(rel)
    }

    /// Ensure a resource exists for `rel`, creating it from the manifest if new.
    /// Returns `None` if the assigned algorithm isn't implemented yet (the
    /// resource is skipped rather than panicking on a stub).
    pub fn ensure(&mut self, rel: &str) -> Option<&mut Resource> {
        if !self.resources.contains_key(rel) {
            self.create(rel);
        }
        self.resources.get_mut(rel)
    }

    /// File mode only: pick up files created on disk since the last scan.
    /// Returns the newly-added paths.
    pub fn refresh_disk(&mut self) -> std::io::Result<Vec<String>> {
        if self.memory {
            return Ok(Vec::new());
        }
        let mut added = Vec::new();
        for rel in self.scan_disk()? {
            if !self.resources.contains_key(&rel) {
                self.create(&rel);
                if self.resources.contains_key(&rel) {
                    added.push(rel);
                }
            }
        }
        Ok(added)
    }

    /// Build a resource for `rel`. Skips (with a warning) algorithms that aren't
    /// implemented, so a manifest referencing a stub fails loudly but safely.
    fn create(&mut self, rel: &str) {
        let kind = self.manifest.kind_for(rel);
        if !kind.is_implemented() {
            eprintln!("[riftpipe] skipping {rel}: {kind:?} not implemented yet");
            return;
        }
        let syncer = kind.build(rel);
        let backing: Box<dyn Backing> = if self.memory {
            let mut mb = self.registry.backing(rel);
            // Seed from disk once, if the file exists (sharing a dir into RAM).
            if let Ok(bytes) = std::fs::read(self.root.join(rel)) {
                mb.store(&bytes);
            }
            Box::new(mb)
        } else {
            Box::new(FileBacking::new(rel, self.root.join(rel)))
        };
        self.resources.insert(
            rel.to_string(),
            Resource {
                kind,
                syncer,
                backing,
            },
        );
    }

    /// Recursively list files under `root` as `/`-separated relative paths,
    /// skipping dotfiles/dirs, the manifest, and `.ticket` sidecars.
    fn scan_disk(&self) -> std::io::Result<Vec<String>> {
        let mut out = Vec::new();
        if self.root.is_dir() {
            walk(&self.root, &self.root, &mut out)?;
        }
        out.sort();
        Ok(out)
    }
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') || name == "riftpipe.toml" || name.ends_with(".ticket") {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            walk(root, &path, out)?;
        } else if path.is_file() {
            if let Ok(rel) = path.strip_prefix(root) {
                out.push(rel.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_dir(tag: &str) -> PathBuf {
        // A test-local temp dir; no rng needed — pid + tag is unique enough here.
        let d = std::env::temp_dir().join(format!("riftpipe-ws-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(d.join("docs")).unwrap();
        std::fs::create_dir_all(d.join("state")).unwrap();
        d
    }

    #[test]
    fn scans_and_assigns_algorithms_from_the_manifest() {
        let root = unique_dir("scan");
        std::fs::write(root.join("docs/readme.md"), b"# hi").unwrap();
        std::fs::write(root.join("blob.bin"), b"\x00\x01\x02").unwrap();
        std::fs::write(root.join("riftpipe.toml"), b"ignored").unwrap();
        std::fs::write(root.join(".hidden"), b"skip").unwrap();

        let manifest = Manifest::parse(
            r#"
            default = "rsync-file"
            [[rule]]
            glob = "**/*.md"
            algo = "text-crdt"
            "#,
        )
        .unwrap();

        let ws = Workspace::new(&root, manifest, false).unwrap();
        let paths = ws.paths();
        assert!(paths.contains(&"docs/readme.md".to_string()));
        assert!(paths.contains(&"blob.bin".to_string()));
        assert!(!paths.iter().any(|p| p.contains("riftpipe.toml") || p.contains("hidden")));

        let mut ws = ws;
        assert_eq!(ws.get_mut("docs/readme.md").unwrap().kind, Kind::TextCrdt);
        assert_eq!(ws.get_mut("blob.bin").unwrap().kind, Kind::RsyncFile);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn ensure_discovers_a_peer_only_path() {
        let root = unique_dir("discover");
        let ws = Workspace::new(&root, Manifest::default(), false);
        let mut ws = ws.unwrap();
        assert!(ws.get_mut("new/from/peer.bin").is_none());
        assert!(ws.ensure("new/from/peer.bin").is_some()); // created on demand
        assert_eq!(ws.get_mut("new/from/peer.bin").unwrap().kind, Kind::RsyncFile);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn memory_mode_seeds_from_disk_and_registers() {
        let root = unique_dir("mem");
        std::fs::write(root.join("a.bin"), b"hello").unwrap();
        let ws = Workspace::new(&root, Manifest::default(), true).unwrap();
        let snap = ws.registry().snapshot();
        assert_eq!(snap, vec![("a.bin".to_string(), b"hello".to_vec())]);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn unimplemented_kind_is_skipped_not_panicked() {
        let root = unique_dir("stub");
        std::fs::write(root.join("save.db"), b"x").unwrap();
        let manifest = Manifest::parse(
            r#"
            default = "rsync-file"
            [[rule]]
            glob = "*.db"
            algo = "wal-db"
            "#,
        )
        .unwrap();
        let ws = Workspace::new(&root, manifest, false).unwrap();
        assert!(!ws.paths().contains(&"save.db".to_string())); // skipped
        std::fs::remove_dir_all(&root).ok();
    }
}
