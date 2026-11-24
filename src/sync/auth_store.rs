//! Persisted auth state of a restricted mesh board — `<dir>/.riftpipe-sync/<hex topic>/auth.state`:
//! the trust log entries and the ACCEPTED epoch announces, never a raw epoch key (an announce
//! carries the key only wrapped to each recipient; this device re-opens its own wraps with its
//! `.device` seed on restore — the native mirror of the browser's `riftpipe:trust:v2:<topic>` /
//! `riftpipe:epochs:<topic>`). File = postcard(Body) ‖ 32-byte keyed-BLAKE3 tag (key derived
//! from the device seed), written owner-only (0600) via tmp + rename, read-union-write.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use riftpipe_core::epoch::EpochAnnounce;
use riftpipe_core::identity::{DeviceKey, TrustEntry};

use crate::sync::mesh::write_owner_only;
use crate::sync::tree::SYNC_DIR;

pub const AUTH_FILE: &str = "auth.state";
const FORMAT: u32 = 1;
const MAC_CONTEXT: &str = "riftpipe auth-state mac v1";
const MAC_LEN: usize = 32;

#[derive(Serialize, Deserialize)]
struct Body {
    format: u32,
    board: [u8; 32],
    device: [u8; 32],
    trust: Vec<TrustEntry>,
    epochs: Vec<EpochAnnounce>,
}

/// What a restricted peer persists for one board.
#[derive(Clone, Debug, Default)]
pub struct AuthState {
    pub trust: Vec<TrustEntry>,
    pub epochs: Vec<EpochAnnounce>,
}

#[derive(Debug)]
pub enum Loaded {
    Absent,
    Found(AuthState),
    Corrupt(String),
}

/// `<dir>/.riftpipe-sync/<lowercase hex board>/auth.state`.
pub fn auth_path(dir: &Path, board: &[u8; 32]) -> PathBuf {
    let hex: String = board.iter().map(|b| format!("{b:02x}")).collect();
    dir.join(SYNC_DIR).join(hex).join(AUTH_FILE)
}

/// Content-addressed dedup id for a trust entry — the same as the browser's
/// `trust_entry_id` and `TreePeer::trust_seen`.
pub fn trust_id(e: &TrustEntry) -> [u8; 32] {
    *blake3::hash(&postcard::to_allocvec(e).unwrap_or_default()).as_bytes()
}

/// Content-addressed dedup id for an epoch announce.
pub fn announce_id(a: &EpochAnnounce) -> [u8; 32] {
    *blake3::hash(&a.sig).as_bytes()
}

fn mac_key(key: &DeviceKey) -> [u8; 32] {
    blake3::derive_key(MAC_CONTEXT, &key.seed())
}

fn encode(board: &[u8; 32], key: &DeviceKey, s: &AuthState) -> Vec<u8> {
    let body = Body {
        format: FORMAT,
        board: *board,
        device: key.id(),
        trust: s.trust.clone(),
        epochs: s.epochs.clone(),
    };
    let mut out = postcard::to_allocvec(&body).expect("encode");
    let tag = blake3::keyed_hash(&mac_key(key), &out);
    out.extend_from_slice(tag.as_bytes());
    out
}

fn decode(bytes: &[u8], board: &[u8; 32], key: &DeviceKey) -> Result<AuthState, String> {
    if bytes.len() < MAC_LEN {
        return Err("truncated".to_string());
    }
    let (body, tag) = bytes.split_at(bytes.len() - MAC_LEN);
    let tag: [u8; MAC_LEN] = tag.try_into().expect("exact length");
    if blake3::keyed_hash(&mac_key(key), body) != tag {
        return Err("integrity check failed (corrupt, edited, or written for a different .device)".to_string());
    }
    let b: Body = postcard::from_bytes(body).map_err(|e| format!("undecodable: {e}"))?;
    if b.format != FORMAT {
        return Err(format!("unsupported format {}", b.format));
    }
    if b.board != *board {
        return Err("written for another board".to_string());
    }
    if b.device != key.id() {
        return Err("written for another device".to_string());
    }
    Ok(AuthState { trust: b.trust, epochs: b.epochs })
}

/// Load `path`'s persisted auth state for `board`/`key`. `Absent` if there's no
/// file yet; a corrupt, foreign-board, or foreign-device file is `Corrupt` (the
/// whole file is rejected, never partially trusted).
pub fn load(path: &Path, board: &[u8; 32], key: &DeviceKey) -> Loaded {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Loaded::Absent,
        Err(e) => return Loaded::Corrupt(format!("unreadable: {e}")),
    };
    // A resumed file loosened by hand (or by an older build) goes back to owner-only.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(path).is_ok_and(|m| m.permissions().mode() & 0o077 != 0) {
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
    }
    match decode(&bytes, board, key) {
        Ok(s) => Loaded::Found(s),
        Err(e) => Loaded::Corrupt(e),
    }
}

/// `stored` plus every item of `ours` not already in it (by `id`) — the
/// persisted sets are content-addressed, so a union never loses anything.
fn union_by<T: Clone>(mut stored: Vec<T>, ours: &[T], id: impl Fn(&T) -> [u8; 32]) -> Vec<T> {
    let have: std::collections::HashSet<[u8; 32]> = stored.iter().map(&id).collect();
    stored.extend(ours.iter().filter(|e| !have.contains(&id(e))).cloned());
    stored
}

/// Read-union-write: the valid stored file (a corrupt one is replaced) unioned
/// with `trust`/`epochs`, then written owner-only, atomically.
pub fn save(
    path: &Path,
    board: &[u8; 32],
    key: &DeviceKey,
    trust: &[TrustEntry],
    epochs: &[EpochAnnounce],
) -> std::io::Result<()> {
    let stored = match load(path, board, key) {
        Loaded::Found(s) => s,
        Loaded::Absent | Loaded::Corrupt(_) => AuthState::default(),
    };
    let merged = AuthState {
        trust: union_by(stored.trust, trust, trust_id),
        epochs: union_by(stored.epochs, epochs, announce_id),
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    write_owner_only(path, &encode(board, key, &merged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use riftpipe_core::identity::Role;

    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("riftpipe-authstore-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn roundtrip_is_owner_only_and_leaves_no_tmp() {
        let d = dir("roundtrip");
        let board = [0xaa; 32];
        let f = DeviceKey::from_seed(&[1; 32]);
        let trust = vec![f.genesis(), f.grant([2; 32], Role::Member, [1; 16])];
        let epochs = vec![EpochAnnounce::create(&f, 1, [7; 8], vec![])];
        let path = auth_path(&d, &board);

        save(&path, &board, &f, &trust, &epochs).unwrap();
        let loaded = load(&path, &board, &f);
        let Loaded::Found(s) = loaded else { panic!("expected Found, got {loaded:?}") };
        assert_eq!(s.trust.iter().map(trust_id).collect::<Vec<_>>(), trust.iter().map(trust_id).collect::<Vec<_>>());
        assert_eq!(
            s.epochs.iter().map(announce_id).collect::<Vec<_>>(),
            epochs.iter().map(announce_id).collect::<Vec<_>>()
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "owner-only");
        }
        assert!(!PathBuf::from(format!("{}.tmp", path.display())).exists(), "no tmp left behind");
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn absent_file_is_absent() {
        let d = dir("absent");
        let board = [0xaa; 32];
        let key = DeviceKey::from_seed(&[1; 32]);
        let path = auth_path(&d, &board);
        assert!(matches!(load(&path, &board, &key), Loaded::Absent));
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn save_unions_with_what_is_stored() {
        let d = dir("union");
        let board = [0xaa; 32];
        let f = DeviceKey::from_seed(&[1; 32]);
        let path = auth_path(&d, &board);
        let g = f.genesis();
        let grant = f.grant([2; 32], Role::Member, [1; 16]);
        let revoke = f.revoke([2; 32], [3; 16]);

        save(&path, &board, &f, &[g.clone(), grant.clone()], &[]).unwrap();
        save(&path, &board, &f, &[g.clone(), revoke.clone()], &[]).unwrap();

        let Loaded::Found(s) = load(&path, &board, &f) else { panic!("expected Found") };
        assert_eq!(
            s.trust.iter().map(trust_id).collect::<Vec<_>>(),
            vec![trust_id(&g), trust_id(&grant), trust_id(&revoke)]
        );

        let a1 = EpochAnnounce::create(&f, 1, [1; 8], vec![]);
        let a2 = EpochAnnounce::create(&f, 2, [2; 8], vec![]);
        save(&path, &board, &f, &[], std::slice::from_ref(&a1)).unwrap();
        save(&path, &board, &f, &[], std::slice::from_ref(&a2)).unwrap();
        let Loaded::Found(s) = load(&path, &board, &f) else { panic!("expected Found") };
        assert_eq!(s.epochs.iter().map(announce_id).collect::<Vec<_>>(), vec![announce_id(&a1), announce_id(&a2)]);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn tampered_truncated_or_garbage_files_are_rejected() {
        let d = dir("tampered");
        let board = [0xaa; 32];
        let f = DeviceKey::from_seed(&[1; 32]);
        let path = auth_path(&d, &board);
        save(&path, &board, &f, &[f.genesis()], &[]).unwrap();
        let good = std::fs::read(&path).unwrap();

        let mut flipped_body = good.clone();
        flipped_body[3] ^= 0x01;
        std::fs::write(&path, &flipped_body).unwrap();
        assert!(matches!(load(&path, &board, &f), Loaded::Corrupt(_)), "flipped body byte");

        let mut flipped_tag = good.clone();
        let last = flipped_tag.len() - 1;
        flipped_tag[last] ^= 0x01;
        std::fs::write(&path, &flipped_tag).unwrap();
        assert!(matches!(load(&path, &board, &f), Loaded::Corrupt(_)), "flipped tag byte");

        std::fs::write(&path, &good[..10]).unwrap();
        assert!(matches!(load(&path, &board, &f), Loaded::Corrupt(_)), "truncated");

        std::fs::write(&path, b"").unwrap();
        assert!(matches!(load(&path, &board, &f), Loaded::Corrupt(_)), "empty");

        std::fs::write(&path, b"garbage").unwrap();
        assert!(matches!(load(&path, &board, &f), Loaded::Corrupt(_)), "garbage");

        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn another_device_or_board_is_rejected() {
        let d = dir("another");
        let board = [0xaa; 32];
        let f = DeviceKey::from_seed(&[1; 32]);
        let path = auth_path(&d, &board);
        save(&path, &board, &f, &[f.genesis()], &[]).unwrap();

        let other_device = DeviceKey::from_seed(&[9; 32]);
        let Loaded::Corrupt(msg) = load(&path, &board, &other_device) else { panic!("expected Corrupt") };
        assert!(msg.contains("integrity"), "got: {msg}");

        let other_board = [0xbb; 32];
        let Loaded::Corrupt(msg) = load(&path, &other_board, &f) else { panic!("expected Corrupt") };
        assert!(msg.contains("another board"), "got: {msg}");

        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn a_corrupt_file_is_replaced_on_save() {
        let d = dir("replace-corrupt");
        let board = [0xaa; 32];
        let f = DeviceKey::from_seed(&[1; 32]);
        let path = auth_path(&d, &board);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"garbage").unwrap();

        save(&path, &board, &f, &[f.genesis()], &[]).unwrap();
        assert!(matches!(load(&path, &board, &f), Loaded::Found(_)));
        std::fs::remove_dir_all(&d).ok();
    }

    #[cfg(unix)]
    #[test]
    fn loosened_file_is_tightened_on_load() {
        use std::os::unix::fs::PermissionsExt;
        let d = dir("tighten");
        let board = [0xaa; 32];
        let f = DeviceKey::from_seed(&[1; 32]);
        let path = auth_path(&d, &board);
        save(&path, &board, &f, &[f.genesis()], &[]).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let _ = load(&path, &board, &f);
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        std::fs::remove_dir_all(&d).ok();
    }
}
