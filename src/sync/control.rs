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
/// mesh loop when something changed and at least every [`STATE_HEARTBEAT_SECS`],
/// stamped with the writer's `pid` and `updatedAt` (unix seconds); removed when
/// the loop ends. Read by the `requests`/`members`/`approve` CLI verbs.
pub const STATE_FILE: &str = ".riftpipe-auth.json";

/// The running peer rewrites [`STATE_FILE`] at least this often (a heartbeat).
pub const STATE_HEARTBEAT_SECS: u64 = 5;

/// A [`STATE_FILE`] whose `updatedAt` is older than this (three missed
/// heartbeats) probably belongs to a peer that crashed or was killed.
pub const STATE_STALE_SECS: u64 = 15;

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

/// Longest join-request name (in bytes) a peer even looks at: a canonical name
/// is at most [`NAME_MAX_CHARS`] chars of at most 4 UTF-8 bytes each. Checked
/// before any per-char work, so a huge name costs nothing to reject.
pub const NAME_MAX_BYTES: usize = NAME_MAX_CHARS * 4;

/// Invisible characters that change how the text around them renders: Unicode
/// format characters (category Cf — bidi overrides/isolates, zero-width
/// joiners/spaces, the BOM, soft hyphen, tag characters, …) plus the line and
/// paragraph separators. `char::is_control` covers only Cc.
fn is_format_char(c: char) -> bool {
    matches!(
        c,
        '\u{00AD}'
            | '\u{0600}'..='\u{0605}'
            | '\u{061C}'
            | '\u{06DD}'
            | '\u{070F}'
            | '\u{0890}'..='\u{0891}'
            | '\u{08E2}'
            | '\u{180E}'
            | '\u{200B}'..='\u{200F}'
            | '\u{2028}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206F}'
            | '\u{FEFF}'
            | '\u{FFF9}'..='\u{FFFB}'
            | '\u{110BD}'
            | '\u{110CD}'
            | '\u{13430}'..='\u{1343F}'
            | '\u{1BCA0}'..='\u{1BCA3}'
            | '\u{1D173}'..='\u{1D17A}'
            | '\u{E0001}'
            | '\u{E0020}'..='\u{E007F}'
    )
}

/// Trim, replace control and format characters (see [`is_format_char`]) with
/// `?`, and cap to [`NAME_MAX_CHARS`] chars. Idempotent: a cleaned name cleans
/// to itself, which is what [`is_canonical_name`] relies on.
pub fn clean_name(s: &str) -> String {
    let capped: String = s
        .trim()
        .chars()
        .map(|c| if c.is_control() || is_format_char(c) { '?' } else { c })
        .take(NAME_MAX_CHARS)
        .collect();
    capped.trim_end().to_string()
}

/// Is `name` exactly what [`clean_name`] would produce? A peer only accepts a
/// join request whose signed name is canonical: the safety code hashes the raw
/// name while Admins are shown the cleaned one, so a non-canonical name would let
/// a requester grind invisible variations of one displayed name for a chosen
/// safety code. A joiner cleans its name before signing (`--name`), and the
/// browser sends `""`, so honest requests always pass.
pub fn is_canonical_name(name: &str) -> bool {
    name.len() <= NAME_MAX_BYTES && clean_name(name) == name
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

/// Seconds since the unix epoch (the clock `updatedAt` is stamped with).
pub fn unix_now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// A warning when `state` looks like a dead peer's: its `updatedAt` is more than
/// [`STATE_STALE_SECS`] before `now_secs`. `None` when it's fresh or unstamped.
pub fn stale_warning(state: &serde_json::Value, now_secs: u64) -> Option<String> {
    let updated = state.get("updatedAt")?.as_u64()?;
    let age = now_secs.saturating_sub(updated);
    if age <= STATE_STALE_SECS {
        return None;
    }
    let pid = state.get("pid").and_then(|v| v.as_u64()).map(|p| format!(" by pid {p}")).unwrap_or_default();
    Some(format!(
        "[riftpipe] warning: {STATE_FILE} was last updated {age}s ago{pid} — the peer may not be running, so this \
         is out of date, and an `approve` queued now is only seen by a peer that is running on this dir"
    ))
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

/// Did the running peer drop a request because its list was full (`inboxOverflow`)?
pub fn inbox_overflowed(state: &serde_json::Value) -> bool {
    state.get("inboxOverflow").and_then(|v| v.as_bool()).unwrap_or(false)
}

fn label_for(name: &str, safety: &str) -> String {
    format!("{} (safety {safety})", serde_json::to_string(name).unwrap_or_default())
}

/// Resolve what `riftpipe approve <dir> <arg>` means: `arg` is either a 64-hex
/// device id, or an 8-hex safety code looked up against `state`'s pending
/// requests. Returns the device id, plus a human label when a matching
/// request's name/safety is known. Codes are refused once the peer's request
/// list overflowed ([`inbox_overflowed`]): a dropped request isn't listed, so a
/// listed one could have been ground to share its 32-bit code. A state whose
/// `role` isn't `"admin"` is refused outright: that peer can't approve.
pub fn resolve_approve_target(
    state: Option<&serde_json::Value>,
    arg: &str,
) -> Result<([u8; 32], Option<String>), String> {
    if let Some(role) = state.and_then(|s| s.get("role")) {
        if role.as_str() != Some("admin") {
            return Err("the peer running on this dir is not an Admin of the board, so it can't approve — \
                        run `riftpipe approve` on an Admin's dir"
                .to_string());
        }
    }
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
        if inbox_overflowed(state) {
            return Err(format!(
                "the request list overflowed this session, so a dropped request could share safety code {t} \
                 with a listed one — confirm the device id out-of-band and approve by id (see `riftpipe requests <dir>`)"
            ));
        }
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
    fn clean_name_strips_bidi_and_zero_width() {
        assert_eq!(clean_name("Bob\u{202E}gnp.exe"), "Bob?gnp.exe");
        assert_eq!(clean_name("B\u{200B}o\u{200D}b\u{2066}x\u{2069}"), "B?o?b?x?");
        assert_eq!(clean_name("\u{FEFF}Bob\u{00AD}"), "?Bob?");
        assert_eq!(clean_name("Bob\u{2028}Eve"), "Bob?Eve");
        assert_eq!(clean_name("Zoë 🚀"), "Zoë 🚀");
    }

    #[test]
    fn clean_name_is_idempotent_even_when_truncating_at_a_space() {
        let cut_at_space = format!("{} b", "a".repeat(NAME_MAX_CHARS - 1));
        let once = clean_name(&cut_at_space);
        assert_eq!(once, "a".repeat(NAME_MAX_CHARS - 1));
        assert_eq!(clean_name(&once), once);
        for s in ["  Bob\u{1b}[31m ", "Bob\u{202E}x", &"é".repeat(100), " \u{2028} x \u{2029} "] {
            let c = clean_name(s);
            assert_eq!(clean_name(&c), c, "{s:?}");
            assert!(is_canonical_name(&c), "{s:?}");
        }
    }

    #[test]
    fn canonical_names_are_exactly_cleaned_and_bounded() {
        assert!(is_canonical_name(""));
        assert!(is_canonical_name("cli"));
        assert!(is_canonical_name("e2e joiner"));
        assert!(!is_canonical_name("Bob "), "trailing whitespace is hidden by the display");
        assert!(!is_canonical_name("Bob\u{3000}"), "so is trailing Unicode whitespace");
        assert!(!is_canonical_name("Bob\u{200B}"));
        assert!(!is_canonical_name("Bob\u{7}"));
        assert!(!is_canonical_name(&"x".repeat(NAME_MAX_CHARS + 1)));
        assert!(!is_canonical_name(&" ".repeat(10_000_000)), "byte cap rejects before cleaning");
        assert!(is_canonical_name(&"🚀".repeat(NAME_MAX_CHARS)), "64 four-byte chars fit the byte cap");
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
    fn resolve_approve_target_refuses_a_non_admin_peer() {
        let device = "ab".repeat(32);
        for role in [json!("member"), json!(null)] {
            let state = json!({"role": role,
                "requests": [{"device": device, "name": "Bob", "safety": "a1b2 c3d4"}]});
            for arg in ["a1b2c3d4", device.as_str()] {
                let err = resolve_approve_target(Some(&state), arg).unwrap_err();
                assert!(err.contains("not an Admin"), "{role} {arg}: {err}");
            }
        }
        let admin = json!({"role": "admin",
            "requests": [{"device": device, "name": "Bob", "safety": "a1b2 c3d4"}]});
        assert!(resolve_approve_target(Some(&admin), "a1b2c3d4").is_ok());
    }

    #[test]
    fn resolve_approve_target_refuses_codes_after_an_overflow() {
        let device = "ab".repeat(32);
        let state = json!({"inboxOverflow": true,
            "requests": [{"device": device, "name": "Bob", "safety": "a1b2 c3d4"}]});
        let err = resolve_approve_target(Some(&state), "a1b2 c3d4").unwrap_err();
        assert!(err.contains("overflowed") && err.contains("approve by id"), "{err}");
        assert_eq!(resolve_approve_target(Some(&state), &device).unwrap().0, hex32(&device).unwrap());
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
    fn stale_warning_after_three_missed_heartbeats() {
        let state = json!({"pid": 4242, "updatedAt": 1_000});
        assert_eq!(stale_warning(&state, 1_000), None);
        assert_eq!(stale_warning(&state, 1_000 + STATE_STALE_SECS), None);
        let w = stale_warning(&state, 1_100).unwrap();
        assert!(w.contains("100s ago") && w.contains("pid 4242"), "{w}");
        assert_eq!(stale_warning(&json!({"requests": []}), 1_100), None, "unstamped: no claim either way");
        assert_eq!(stale_warning(&state, 0), None, "a clock behind the writer's isn't stale");
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
