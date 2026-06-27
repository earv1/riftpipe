//! The workspace manifest — `riftpipe.toml` (DESIGN.md §17). Maps resource
//! paths to sync algorithms ([`Kind`]) by glob, with a fallback `default`. This
//! is how "different algorithms for different things" is expressed:
//!
//! ```toml
//! default = "rsync-file"        # anything not matched below
//!
//! [[rule]]
//! glob = "**/*.md"              # prose merges char-by-char
//! algo = "text-crdt"
//!
//! [[rule]]
//! glob = "state/**"            # game/db state: append-only log
//! algo = "wal-db"
//! ```
//!
//! Globs are matched against the resource's `/`-separated relative path. `*`
//! matches within a path segment, `**` crosses segments, `?` is one non-`/`
//! char. First matching rule wins; otherwise `default`.

use serde::Deserialize;

use crate::sync::syncer::Kind;

#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    #[serde(default = "default_kind")]
    pub default: Kind,
    #[serde(default, rename = "rule")]
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Rule {
    pub glob: String,
    pub algo: Kind,
}

fn default_kind() -> Kind {
    // rsync works on any bytes, so it's the safe catch-all; opt specific globs
    // into text-crdt / wal-db / image as needed.
    Kind::RsyncFile
}

impl Default for Manifest {
    fn default() -> Self {
        Manifest {
            default: default_kind(),
            rules: Vec::new(),
        }
    }
}

impl Manifest {
    /// Parse a manifest from TOML source.
    pub fn parse(src: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(src)
    }

    /// Load `riftpipe.toml` from `path`; fall back to the default manifest
    /// (all-rsync) if it's missing. A present-but-broken file is a hard error.
    pub fn load_or_default(path: &std::path::Path) -> Result<Self, String> {
        match std::fs::read_to_string(path) {
            Ok(src) => Self::parse(&src).map_err(|e| format!("{}: {e}", path.display())),
            Err(_) => Ok(Self::default()),
        }
    }

    /// The algorithm assigned to `rel_path` — first matching rule, else default.
    pub fn kind_for(&self, rel_path: &str) -> Kind {
        self.rules
            .iter()
            .find(|r| glob_match(&r.glob, rel_path))
            .map(|r| r.algo)
            .unwrap_or(self.default)
    }
}

/// Glob match against a `/`-separated path. `*` = any run of non-`/`; `**` = any
/// run including `/`; `?` = one non-`/`.
pub fn glob_match(pattern: &str, path: &str) -> bool {
    glob_rec(pattern.as_bytes(), path.as_bytes())
}

fn glob_rec(p: &[u8], t: &[u8]) -> bool {
    if p.is_empty() {
        return t.is_empty();
    }
    match p[0] {
        b'*' if p.get(1) == Some(&b'*') => {
            // `**` matches any run of characters, including `/`. Consume an
            // optional trailing `/` in the pattern (so `state/**` and `**/x`
            // behave naturally).
            let rest = if p.get(2) == Some(&b'/') { &p[3..] } else { &p[2..] };
            if glob_rec(rest, t) {
                return true;
            }
            (0..t.len()).any(|i| glob_rec(rest, &t[i + 1..]))
        }
        b'*' => {
            // `*` matches any run of non-`/` characters.
            let rest = &p[1..];
            if glob_rec(rest, t) {
                return true;
            }
            let mut i = 0;
            while i < t.len() && t[i] != b'/' {
                if glob_rec(rest, &t[i + 1..]) {
                    return true;
                }
                i += 1;
            }
            false
        }
        b'?' => !t.is_empty() && t[0] != b'/' && glob_rec(&p[1..], &t[1..]),
        c => !t.is_empty() && t[0] == c && glob_rec(&p[1..], &t[1..]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_segment_wildcard() {
        assert!(glob_match("*.md", "notes.md"));
        assert!(!glob_match("*.md", "notes.txt"));
        // `*` does not cross `/`
        assert!(!glob_match("*.md", "dir/notes.md"));
        assert!(glob_match("dir/*.md", "dir/notes.md"));
        assert!(glob_match("?.txt", "a.txt"));
        assert!(!glob_match("?.txt", "ab.txt"));
    }

    #[test]
    fn glob_double_star_crosses_segments() {
        assert!(glob_match("**/*.md", "a/b/c/notes.md"));
        assert!(glob_match("**/*.md", "notes.md"));
        assert!(glob_match("state/**", "state/save1.bin"));
        assert!(glob_match("state/**", "state/deep/save.bin"));
        assert!(!glob_match("state/**", "other/save.bin"));
    }

    #[test]
    fn manifest_resolves_first_match_then_default() {
        let m = Manifest::parse(
            r#"
            default = "rsync-file"
            [[rule]]
            glob = "**/*.md"
            algo = "text-crdt"
            [[rule]]
            glob = "state/**"
            algo = "wal-db"
            "#,
        )
        .unwrap();
        assert_eq!(m.kind_for("docs/readme.md"), Kind::TextCrdt);
        assert_eq!(m.kind_for("state/save.bin"), Kind::WalDb);
        assert_eq!(m.kind_for("assets/logo.png"), Kind::RsyncFile); // default
    }

    #[test]
    fn missing_manifest_defaults_to_rsync() {
        let m = Manifest::load_or_default(std::path::Path::new("/no/such/riftpipe.toml")).unwrap();
        assert_eq!(m.kind_for("anything.xyz"), Kind::RsyncFile);
    }
}
