//! Live admin commands for a running `connect … --restricted` peer. The filesystem is
//! the interface: `riftpipe revoke|grant <dir> <id>` appends a line to the dotfile
//! `<dir>/.riftpipe-control` (machine-local — dot-paths never sync) and the running
//! mesh loop polls it.
use std::io::Write;
use std::path::{Path, PathBuf};

pub const CONTROL_FILE: &str = ".riftpipe-control";

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Grant([u8; 32]),
    Revoke([u8; 32]),
}

/// Parse 64 hex chars into a 32-byte array.
fn hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// `revoke <64 hex>` | `grant <64 hex>` (surrounding whitespace ok).
pub fn parse_line(line: &str) -> Option<Command> {
    let line = line.trim();
    let mut parts = line.split_whitespace();
    let verb = parts.next()?;
    let id = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let id = hex32(id)?;
    match verb {
        "revoke" => Some(Command::Revoke(id)),
        "grant" => Some(Command::Grant(id)),
        _ => None,
    }
}

/// Append `"{verb} {hex_id}\n"` to `<dir>/.riftpipe-control` (create if absent).
pub fn append(dir: &Path, verb: &str, hex_id: &str) -> std::io::Result<()> {
    let path = dir.join(CONTROL_FILE);
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(f, "{verb} {hex_id}")
}

/// Tails the control file from its length at open (old lines ignored).
pub struct Control {
    path: PathBuf,
    offset: u64,
}

impl Control {
    pub fn open(dir: &Path) -> Self {
        let path = dir.join(CONTROL_FILE);
        let offset = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        Control { path, offset }
    }

    /// New complete lines since last poll (malformed skipped); a trailing partial line
    /// waits; if the file shrank, restart from 0.
    pub fn poll(&mut self) -> Vec<Command> {
        let Ok(bytes) = std::fs::read(&self.path) else { return Vec::new() };
        if (bytes.len() as u64) < self.offset {
            self.offset = 0;
        }
        let tail = &bytes[self.offset as usize..];
        let Ok(text) = std::str::from_utf8(tail) else { return Vec::new() };
        let mut consumed = 0usize;
        let mut out = Vec::new();
        for line in text.split_inclusive('\n') {
            if !line.ends_with('\n') {
                // Trailing partial line: wait for the rest.
                break;
            }
            consumed += line.len();
            if let Some(cmd) = parse_line(line) {
                out.push(cmd);
            }
        }
        self.offset += consumed as u64;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_line_accepts_revoke_and_grant_rejects_garbage() {
        let id = "a".repeat(64);
        assert_eq!(parse_line(&format!("revoke {id}")), Some(Command::Revoke(hex32(&id).unwrap())));
        assert_eq!(parse_line(&format!("  grant {id}  ")), Some(Command::Grant(hex32(&id).unwrap())));
        assert_eq!(parse_line("revoke"), None);
        assert_eq!(parse_line("revoke nothex"), None);
        assert_eq!(parse_line(&format!("frobnicate {id}")), None);
        assert_eq!(parse_line(&format!("revoke {id} extra")), None);
        assert_eq!(parse_line(""), None);
    }

    #[test]
    fn poll_only_sees_lines_appended_after_open_and_waits_for_newline() {
        let dir = std::env::temp_dir().join(format!("riftpipe-control-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let old_id = "b".repeat(64);
        append(&dir, "grant", &old_id).unwrap();

        let mut control = Control::open(&dir);
        assert_eq!(control.poll(), Vec::new());

        let revoke_id = "c".repeat(64);
        append(&dir, "revoke", &revoke_id).unwrap();
        // Write a partial line directly (no trailing newline).
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.join(CONTROL_FILE))
                .unwrap();
            write!(f, "grant abc").unwrap();
        }
        assert_eq!(control.poll(), vec![Command::Revoke(hex32(&revoke_id).unwrap())]);

        // Complete the partial line with a valid id and a trailing newline.
        let grant_id = format!("abc{}", "d".repeat(61));
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.join(CONTROL_FILE))
                .unwrap();
            writeln!(f, "{}", &grant_id[3..]).unwrap();
        }
        assert_eq!(control.poll(), vec![Command::Grant(hex32(&grant_id).unwrap())]);

        std::fs::remove_dir_all(&dir).ok();
    }
}
