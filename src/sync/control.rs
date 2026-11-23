//! Live admin commands for a running `connect … --restricted` peer. The filesystem is
//! the interface: `riftpipe revoke|grant <dir> <id>` appends a line to the dotfile
//! `<dir>/.riftpipe-control` (machine-local — dot-paths never sync) and the running
//! mesh loop polls it. The running peer also writes [`STATE_FILE`] (its pending
//! requests and members), which the `requests`/`members`/`approve` verbs read.

use std::io::Write;
use std::path::{Path, PathBuf};

pub const CONTROL_FILE: &str = ".riftpipe-control";

/// Machine-local snapshot of a running restricted peer's auth state (device ids,
/// names, roles and safety codes — never keys, never synced). Rewritten by the
/// mesh loop about once a second when something changed; read by the
/// `requests`/`members`/`approve` CLI verbs.
pub const STATE_FILE: &str = ".riftpipe-auth.json";

/// Display names are self-asserted and shown to an Admin before approving, so
/// cap how much garbage one device can push.
pub const NAME_MAX_CHARS: usize = 64;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Grant([u8; 32]),
    Revoke([u8; 32]),
    Approve([u8; 32]),
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

/// `revoke <64 hex>` | `grant <64 hex>` | `approve <64 hex>` (surrounding whitespace ok).
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
        "approve" => Some(Command::Approve(id)),
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

/// Trim, replace control characters with `?`, and cap to [`NAME_MAX_CHARS`] chars.
pub fn clean_name(s: &str) -> String {
    s.trim().chars().map(|c| if c.is_control() { '?' } else { c }).take(NAME_MAX_CHARS).collect()
}

/// Drop whitespace and `-` and lowercase — so a safety code or device id typed
/// by hand (with spaces, dashes, or mixed case) still compares equal.
pub fn normalize_code(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace() && *c != '-').collect::<String>().to_lowercase()
}

/// Write `json` to `<dir>/.riftpipe-auth.json` atomically: write a sibling
/// `.tmp` file, then rename it over the real one.
pub fn write_state(dir: &Path, json: &str) -> std::io::Result<()> {
    let tmp = dir.join(format!("{STATE_FILE}.tmp"));
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, dir.join(STATE_FILE))
}

/// Read and parse `<dir>/.riftpipe-auth.json`; `None` on any I/O or parse error
/// (including "no such file" — there's no running restricted peer here).
pub fn read_state(dir: &Path) -> Option<serde_json::Value> {
    let text = std::fs::read_to_string(dir.join(STATE_FILE)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Delete `<dir>/.riftpipe-auth.json`, if present (clears a stale snapshot once
/// this device stops being restricted-and-unauthorized).
pub fn remove_state(dir: &Path) {
    let _ = std::fs::remove_file(dir.join(STATE_FILE));
}

/// The `(name, safety)` of the request from `device_hex` in `state["requests"]`, if any.
fn request_by_device(state: &serde_json::Value, device_hex: &str) -> Option<(String, String)> {
    state.get("requests")?.as_array()?.iter().find_map(|r| {
        if r.get("device")?.as_str()? == device_hex {
            Some((r.get("name")?.as_str()?.to_string(), r.get("safety")?.as_str()?.to_string()))
        } else {
            None
        }
    })
}

/// All `(device, name, safety)` in `state["requests"]` whose safety code normalizes to `code`.
fn requests_by_safety(state: &serde_json::Value, code: &str) -> Vec<(String, String, String)> {
    let Some(arr) = state.get("requests").and_then(|v| v.as_array()) else { return Vec::new() };
    arr.iter()
        .filter_map(|r| {
            let device = r.get("device")?.as_str()?.to_string();
            let name = r.get("name")?.as_str()?.to_string();
            let safety = r.get("safety")?.as_str()?.to_string();
            (normalize_code(&safety) == code).then_some((device, name, safety))
        })
        .collect()
}

fn label_for(name: &str, safety: &str) -> String {
    format!("{} (safety {safety})", serde_json::to_string(name).unwrap_or_default())
}

/// Resolve what `riftpipe approve <dir> <arg>` means: `arg` is either a 64-hex
/// device id, or an 8-hex safety code looked up against `state`'s pending
/// requests. Returns the device id, plus a human label when a matching
/// request's name/safety is known.
pub fn resolve_approve_target(
    state: Option<&serde_json::Value>,
    arg: &str,
) -> Result<([u8; 32], Option<String>), String> {
    let t = normalize_code(arg);
    if t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()) {
        let id = hex32(&t).ok_or_else(|| "expected a 64-hex device id or an 8-hex safety code".to_string())?;
        let label = state.and_then(|s| request_by_device(s, &t)).map(|(name, safety)| label_for(&name, &safety));
        return Ok((id, label));
    }
    if t.len() == 8 && t.chars().all(|c| c.is_ascii_hexdigit()) {
        let Some(state) = state else {
            return Err("no auth state file — is `riftpipe connect … --restricted` running on this dir?".to_string());
        };
        let matches = requests_by_safety(state, &t);
        return match matches.len() {
            1 => {
                let (device, name, safety) = &matches[0];
                let id =
                    hex32(device).ok_or_else(|| "expected a 64-hex device id or an 8-hex safety code".to_string())?;
                Ok((id, Some(label_for(name, safety))))
            }
            0 => Err(format!("no pending request with safety code {t} — see `riftpipe requests <dir>`")),
            _ => Err(format!("safety code {t} matches several requests — approve by device id")),
        };
    }
    Err("expected a 64-hex device id or an 8-hex safety code".to_string())
}

/// Human lines for `riftpipe requests <dir>`: `"{safety}  {device}  {name}"`,
/// one per pending request. An empty name prints as `(no name)`.
pub fn format_requests(state: &serde_json::Value) -> Vec<String> {
    let Some(arr) = state.get("requests").and_then(|v| v.as_array()) else { return Vec::new() };
    arr.iter()
        .map(|r| {
            let device = r.get("device").and_then(|v| v.as_str()).unwrap_or_default();
            let safety = r.get("safety").and_then(|v| v.as_str()).unwrap_or_default();
            let name = r.get("name").and_then(|v| v.as_str()).unwrap_or_default();
            let name = if name.is_empty() { "(no name)" } else { name };
            format!("{safety}  {device}  {name}")
        })
        .collect()
}

/// Human lines for `riftpipe members <dir>`: `"{device}  {role}"`, plus
/// `"  founder"` and/or `"  (this device)"` when those flags are set.
pub fn format_members(state: &serde_json::Value) -> Vec<String> {
    let Some(arr) = state.get("members").and_then(|v| v.as_array()) else { return Vec::new() };
    arr.iter()
        .map(|m| {
            let device = m.get("device").and_then(|v| v.as_str()).unwrap_or_default();
            let role = m.get("role").and_then(|v| v.as_str()).unwrap_or_default();
            let founder = m.get("founder").and_then(|v| v.as_bool()).unwrap_or(false);
            let me = m.get("me").and_then(|v| v.as_bool()).unwrap_or(false);
            let mut line = format!("{device}  {role}");
            if founder {
                line.push_str("  founder");
            }
            if me {
                line.push_str("  (this device)");
            }
            line
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_line_accepts_revoke_and_grant_rejects_garbage() {
        let id = "a".repeat(64);
        assert_eq!(parse_line(&format!("revoke {id}")), Some(Command::Revoke(hex32(&id).unwrap())));
        assert_eq!(parse_line(&format!("  grant {id}  ")), Some(Command::Grant(hex32(&id).unwrap())));
        assert_eq!(parse_line(&format!("approve {id}")), Some(Command::Approve(hex32(&id).unwrap())));
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

    #[test]
    fn clean_name_strips_controls_and_truncates() {
        assert_eq!(clean_name("  Bob\u{1b}[31m "), "Bob?[31m");
        assert_eq!(clean_name(&"x".repeat(100)).len(), 64);
        assert_eq!(clean_name(""), "");
    }

    #[test]
    fn normalize_code_accepts_spaces_dashes_case() {
        assert_eq!(normalize_code("A1B2 C3D4"), "a1b2c3d4");
        assert_eq!(normalize_code("a1b2-c3d4"), "a1b2c3d4");
    }

    #[test]
    fn resolve_approve_target_by_code_or_id() {
        let device = "ab".repeat(32);
        let state = json!({"requests": [{"device": device, "name": "Bob", "safety": "a1b2 c3d4"}]});
        let want = hex32(&device).unwrap();

        assert_eq!(resolve_approve_target(Some(&state), "a1b2 c3d4").unwrap().0, want);
        assert_eq!(resolve_approve_target(Some(&state), "a1b2c3d4").unwrap().0, want);
        assert_eq!(resolve_approve_target(Some(&state), "A1B2 C3D4").unwrap().0, want);

        let err = resolve_approve_target(Some(&state), "deadbeef").unwrap_err();
        assert!(err.contains("no pending request"), "{err}");

        let all_c = "c".repeat(64);
        assert_eq!(resolve_approve_target(None, &all_c).unwrap().0, hex32(&all_c).unwrap());

        assert!(resolve_approve_target(Some(&state), "zz").is_err());

        let err = resolve_approve_target(None, "a1b2c3d4").unwrap_err();
        assert!(err.contains("no auth state"), "{err}");

        let two = json!({"requests": [
            {"device": "ab".repeat(32), "name": "Bob", "safety": "a1b2 c3d4"},
            {"device": "cd".repeat(32), "name": "Eve", "safety": "a1b2 c3d4"},
        ]});
        let err = resolve_approve_target(Some(&two), "a1b2c3d4").unwrap_err();
        assert!(err.contains("several"), "{err}");
    }

    #[test]
    fn state_file_roundtrip() {
        let dir = std::env::temp_dir().join(format!("riftpipe-state-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();

        let json = json!({"version": 1, "requests": [], "members": []}).to_string();
        write_state(&dir, &json).unwrap();
        assert_eq!(read_state(&dir).unwrap(), serde_json::from_str::<serde_json::Value>(&json).unwrap());

        remove_state(&dir);
        assert_eq!(read_state(&dir), None);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn format_lines() {
        let device = "ab".repeat(32);
        let state = json!({"requests": [{"device": device, "name": "Bob", "safety": "a1b2 c3d4"}]});
        assert_eq!(format_requests(&state), vec![format!("a1b2 c3d4  {device}  Bob")]);

        let noname = json!({"requests": [{"device": device, "name": "", "safety": "a1b2 c3d4"}]});
        assert_eq!(format_requests(&noname), vec![format!("a1b2 c3d4  {device}  (no name)")]);

        let id = "ff".repeat(32);
        let members = json!({"members": [{"device": id, "role": "admin", "founder": true, "me": true}]});
        assert_eq!(format_members(&members), vec![format!("{id}  admin  founder  (this device)")]);
    }
}
