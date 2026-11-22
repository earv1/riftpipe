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

use serde::{Deserialize, Serialize};

use crate::sync::backing::{Backing, FileBacking, MemoryRegistry};
use crate::sync::manifest::{BackingChoice, Manifest};
use crate::sync::strategy::{Kind, SyncStrategy};

/// Recent fingerprints a resource / tombstone remembers (oldest first).
pub const SEEN_CAP: usize = 8;
/// Deletion ids a re-created resource remembers having superseded.
pub const SUPERSEDED_CAP: usize = 8;
pub type Hash = [u8; 32];

/// Make `h` the most-recent entry (moving it if present), keeping ≤ SEEN_CAP.
pub fn push_seen(seen: &mut Vec<Hash>, h: Hash) {
    seen.retain(|x| *x != h);
    seen.push(h);
    if seen.len() > SEEN_CAP {
        let excess = seen.len() - SEEN_CAP;
        seen.drain(..excess);
    }
}

/// Push a superseded deletion id, keeping ≤ SUPERSEDED_CAP (oldest dropped).
fn push_superseded(superseded: &mut Vec<u64>, id: u64) {
    superseded.retain(|x| *x != id);
    superseded.push(id);
    if superseded.len() > SUPERSEDED_CAP {
        let excess = superseded.len() - SUPERSEDED_CAP;
        superseded.drain(..excess);
    }
}

/// A deleted path: what a peer needs to judge delete-vs-concurrent-edit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tombstone {
    pub id: u64,
    pub seen: Vec<Hash>,
    /// Loop guard: one re-assert per heartbeat (in-memory only).
    #[serde(skip)]
    pub reasserted: bool,
}

/// One synced thing: its algorithm + where its bytes live.
pub struct Resource {
    pub kind: Kind,
    pub strategy: Box<dyn SyncStrategy>,
    pub backing: Box<dyn Backing>,
    /// Fingerprints after each local observe/merge. Empty ⇔ never materialized.
    pub seen: Vec<Hash>,
    /// Deletion ids this resource overrode by being re-created locally.
    pub superseded: Vec<u64>,
}

impl Resource {
    /// Record the strategy's current fingerprint into `seen`.
    pub fn note_state(&mut self) {
        push_seen(&mut self.seen, self.strategy.fingerprint());
    }
    /// Materialize merged bytes and note them.
    pub fn store(&mut self, bytes: &[u8]) {
        self.backing.store(bytes);
        self.note_state();
    }
    /// A file-backed resource we had materialized is gone from disk.
    pub fn locally_deleted(&self) -> bool {
        self.backing.on_disk() && !self.seen.is_empty() && !self.backing.exists()
    }
}

pub struct Workspace {
    root: PathBuf,
    manifest: Manifest,
    /// Global in-memory mode (`--memory`): hold bytes in RAM (and surface them
    /// via the `process` file) instead of mirroring to disk. A rule-level
    /// `backing` key in the manifest overrides this per glob.
    memory: bool,
    registry: MemoryRegistry,
    resources: HashMap<String, Resource>,
    tombstones: HashMap<String, Tombstone>,
    dirty: bool,
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
            tombstones: HashMap::new(),
            dirty: false,
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
    /// resource is skipped rather than panicking on a stub). `None` also for
    /// tombstoned paths — a peer delta for a deleted path goes through
    /// `resurrect`.
    pub fn ensure(&mut self, rel: &str) -> Option<&mut Resource> {
        if self.tombstones.contains_key(rel) {
            return None;
        }
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

    /// Whether `rel` is currently tombstoned (deleted, not yet superseded).
    pub fn is_tombstoned(&self, rel: &str) -> bool {
        self.tombstones.contains_key(rel)
    }

    /// The tombstone for `rel`, if any.
    pub fn tombstone(&self, rel: &str) -> Option<&Tombstone> {
        self.tombstones.get(rel)
    }

    /// A mutable handle to the tombstone for `rel`, if any.
    pub fn tombstone_mut(&mut self, rel: &str) -> Option<&mut Tombstone> {
        self.tombstones.get_mut(rel)
    }

    /// Tombstoned paths, sorted.
    pub fn tombstone_paths(&self) -> Vec<String> {
        let mut v: Vec<String> = self.tombstones.keys().cloned().collect();
        v.sort();
        v
    }

    /// Mark the index dirty (needs persisting).
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Clear every tombstone's re-assert guard — called once per heartbeat.
    pub fn rearm_tombstones(&mut self) {
        for t in self.tombstones.values_mut() {
            t.reasserted = false;
        }
    }

    /// A fresh (empty) strategy for `rel` per the manifest; `None` if unimplemented.
    pub fn fresh_strategy(&self, rel: &str) -> Option<Box<dyn SyncStrategy>> {
        let k = self.manifest.kind_for(rel);
        k.is_implemented().then(|| k.build(rel))
    }

    /// Local delete observed: drop the resource, mint a tombstone (random id,
    /// the resource's `seen`), return a copy for the wire.
    pub fn record_local_delete(&mut self, rel: &str) -> Option<Tombstone> {
        let res = self.resources.remove(rel)?;
        self.registry.remove(rel);
        let t = Tombstone {
            id: rand::random::<u64>(),
            seen: res.seen,
            reasserted: false,
        };
        self.tombstones.insert(rel.to_string(), t.clone());
        self.dirty = true;
        Some(t)
    }

    /// Apply a peer's delete: remove the bytes, drop the resource, adopt the
    /// peer's `id`; `seen` = ours, then theirs pushed on top (capped).
    pub fn apply_remote_delete(&mut self, rel: &str, id: u64, seen: &[Hash]) {
        let mut merged = Vec::new();
        if let Some(mut res) = self.resources.remove(rel) {
            res.backing.remove();
            self.registry.remove(rel);
            merged = res.seen;
        }
        for h in seen {
            push_seen(&mut merged, *h);
        }
        self.tombstones.insert(
            rel.to_string(),
            Tombstone { id, seen: merged, reasserted: false },
        );
        self.dirty = true;
    }

    /// Bring a tombstoned path back with `strategy` (already holding the
    /// peer's merged state) and `bytes` as its content. Edit-wins resurrection
    /// does NOT mark the deletion superseded.
    pub fn resurrect(&mut self, rel: &str, strategy: Box<dyn SyncStrategy>, bytes: &[u8]) {
        self.tombstones.remove(rel);
        let kind = self.manifest.kind_for(rel);
        let backing = self.make_backing(rel);
        let mut res = Resource {
            kind,
            strategy,
            backing,
            seen: Vec::new(),
            superseded: Vec::new(),
        };
        res.store(bytes);
        self.resources.insert(rel.to_string(), res);
        self.dirty = true;
    }

    /// The rule-level/global memory choice plus seeding from disk — the
    /// backing-construction half of `create`, shared with `resurrect`.
    fn make_backing(&self, rel: &str) -> Box<dyn Backing> {
        // A rule-level `backing` key wins over the global --memory flag; absent,
        // the resource inherits the run's mode.
        let use_memory = match self.manifest.backing_for(rel) {
            Some(BackingChoice::Memory) => true,
            Some(BackingChoice::File) => false,
            None => self.memory,
        };
        if use_memory {
            let mut mb = self.registry.backing(rel);
            // Seed from disk once, if the file exists (sharing a dir into RAM).
            if let Ok(bytes) = std::fs::read(self.root.join(rel)) {
                mb.store(&bytes);
            }
            Box::new(mb)
        } else {
            Box::new(FileBacking::new(rel, self.root.join(rel)))
        }
    }

    /// Build a resource for `rel`. Skips (with a warning) algorithms that aren't
    /// implemented, so a manifest referencing a stub fails loudly but safely.
    fn create(&mut self, rel: &str) {
        let kind = self.manifest.kind_for(rel);
        if !kind.is_implemented() {
            eprintln!("[riftpipe] skipping {rel}: {kind:?} not implemented yet");
            return;
        }
        let strategy = kind.build(rel);
        let backing = self.make_backing(rel);
        // A creation of a tombstoned path is a local re-creation.
        let mut superseded = Vec::new();
        if let Some(t) = self.tombstones.remove(rel) {
            push_superseded(&mut superseded, t.id);
        }
        self.resources.insert(
            rel.to_string(),
            Resource {
                kind,
                strategy,
                backing,
                seen: Vec::new(),
                superseded,
            },
        );
        self.dirty = true;
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
    fn rule_level_backing_overrides_the_global_flag() {
        let manifest = Manifest::parse(
            r#"
            default = "rsync-file"
            [[rule]]
            glob = "state/**"
            algo = "rsync-file"
            backing = "memory"
            [[rule]]
            glob = "**/*.md"
            algo = "text-crdt"
            backing = "file"
            "#,
        )
        .unwrap();

        // File mode (--memory absent): the memory-ruled glob still lands in RAM
        // AND registers with the MemoryRegistry (the `process` sidecar's view).
        let root = unique_dir("backing-file");
        std::fs::write(root.join("state/live.bin"), b"ram").unwrap();
        std::fs::write(root.join("docs/keep.md"), b"# disk").unwrap();
        let ws = Workspace::new(&root, manifest.clone(), false).unwrap();
        assert_eq!(
            ws.registry().snapshot(),
            vec![("state/live.bin".to_string(), b"ram".to_vec())],
            "memory-ruled resource is in RAM (seeded) and registered; file-ruled is not",
        );
        std::fs::remove_dir_all(&root).ok();

        // Memory mode (--memory): the file-ruled glob still writes through to disk.
        let root = unique_dir("backing-mem");
        std::fs::write(root.join("state/live.bin"), b"ram").unwrap();
        std::fs::write(root.join("docs/keep.md"), b"# disk").unwrap();
        let mut ws = Workspace::new(&root, manifest, true).unwrap();
        let names: Vec<String> = ws.registry().snapshot().into_iter().map(|(n, _)| n).collect();
        assert!(names.contains(&"state/live.bin".to_string()));
        assert!(!names.contains(&"docs/keep.md".to_string()), "file-ruled stays off the registry");
        ws.get_mut("docs/keep.md").unwrap().backing.store(b"# still disk");
        assert_eq!(std::fs::read(root.join("docs/keep.md")).unwrap(), b"# still disk");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn unimplemented_kind_is_skipped_not_panicked() {
        let root = unique_dir("stub");
        std::fs::write(root.join("photo.png"), b"x").unwrap();
        let manifest = Manifest::parse(
            r#"
            default = "rsync-file"
            [[rule]]
            glob = "*.png"
            algo = "image"
            "#,
        )
        .unwrap();
        let ws = Workspace::new(&root, manifest, false).unwrap();
        assert!(!ws.paths().contains(&"photo.png".to_string())); // skipped
        std::fs::remove_dir_all(&root).ok();
    }

    /// A manifest `wal-db` glob now gets a real adapter — including on an
    /// empty/new file (empty log → empty replica, no panic).
    #[test]
    fn wal_db_resources_are_created_not_skipped() {
        let root = unique_dir("wal");
        std::fs::write(root.join("state/save.db"), b"").unwrap();
        let manifest = Manifest::parse(
            r#"
            default = "rsync-file"
            [[rule]]
            glob = "state/**"
            algo = "wal-db"
            "#,
        )
        .unwrap();
        let mut ws = Workspace::new(&root, manifest, false).unwrap();
        let res = ws.get_mut("state/save.db").expect("wal-db resource created");
        assert_eq!(res.kind, Kind::WalDb);
        assert!(!res.strategy.observe(b""), "empty file is an empty replica");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn push_seen_caps_and_moves_to_end() {
        let mut seen = Vec::new();
        for i in 0..10u8 {
            push_seen(&mut seen, [i; 32]);
        }
        assert_eq!(seen.len(), 8);
        assert_eq!(seen.first(), Some(&[2u8; 32]));
        assert_eq!(seen.last(), Some(&[9u8; 32]));
        push_seen(&mut seen, [4u8; 32]);
        assert_eq!(seen.len(), 8);
        assert_eq!(seen.last(), Some(&[4u8; 32]));
    }

    #[test]
    fn discovered_path_is_not_locally_deleted() {
        let root = unique_dir("peer-only");
        let mut ws = Workspace::new(&root, Manifest::default(), false).unwrap();
        let res = ws.ensure("peer.bin").unwrap();
        assert!(res.seen.is_empty());
        assert!(!res.locally_deleted());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_delete_then_recreate_supersedes() {
        let root = unique_dir("del-recreate");
        std::fs::write(root.join("a.bin"), b"hi").unwrap();
        let mut ws = Workspace::new(&root, Manifest::default(), false).unwrap();
        ws.get_mut("a.bin").unwrap().note_state();

        std::fs::remove_file(root.join("a.bin")).unwrap();
        assert!(ws.get_mut("a.bin").unwrap().locally_deleted());

        let t = ws.record_local_delete("a.bin").expect("tombstoned");
        assert!(!ws.paths().contains(&"a.bin".to_string()));
        assert!(ws.is_tombstoned("a.bin"));
        assert!(ws.ensure("a.bin").is_none());

        std::fs::write(root.join("a.bin"), b"back again").unwrap();
        let added = ws.refresh_disk().unwrap();
        assert!(added.contains(&"a.bin".to_string()));
        assert!(!ws.is_tombstoned("a.bin"));
        assert_eq!(ws.get_mut("a.bin").unwrap().superseded, vec![t.id]);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn apply_remote_delete_removes_file_and_adopts_id() {
        let root = unique_dir("remote-del");
        std::fs::write(root.join("b.bin"), b"hi").unwrap();
        let mut ws = Workspace::new(&root, Manifest::default(), false).unwrap();
        ws.get_mut("b.bin").unwrap().note_state();
        let original_fp = ws.get_mut("b.bin").unwrap().seen.clone();

        ws.apply_remote_delete("b.bin", 42, &[[9u8; 32]]);
        assert!(!root.join("b.bin").exists());
        let t = ws.tombstone("b.bin").unwrap();
        assert_eq!(t.id, 42);
        assert_eq!(t.seen.last(), Some(&[9u8; 32]));
        for h in &original_fp {
            assert!(t.seen.contains(h));
        }

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn resurrect_restores_a_live_resource() {
        let root = unique_dir("resurrect");
        std::fs::write(root.join("c.bin"), b"hi").unwrap();
        let mut ws = Workspace::new(&root, Manifest::default(), false).unwrap();
        ws.get_mut("c.bin").unwrap().note_state();
        std::fs::remove_file(root.join("c.bin")).unwrap();
        ws.record_local_delete("c.bin").unwrap();

        let mut s = ws.fresh_strategy("c.bin").unwrap();
        s.observe(b"back");
        ws.resurrect("c.bin", s, b"back");

        assert_eq!(std::fs::read(root.join("c.bin")).unwrap(), b"back");
        assert!(!ws.is_tombstoned("c.bin"));
        assert!(!ws.get_mut("c.bin").unwrap().seen.is_empty());

        std::fs::remove_dir_all(&root).ok();
    }
}
