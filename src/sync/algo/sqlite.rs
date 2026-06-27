//! SQLite row sync (docs/planned/db-integration.md §2) — rows as documents.
//!
//! The realization that shapes this: a riftpipe app has only **one local writer**
//! per replica, so DB concurrency control / locking is a non-issue. The only hard
//! problem is cross-replica conflict — so we resolve it **per cell**. Each cell is
//! `(table, id, column)`, stamped with a `(lamport, site)` version; it's an
//! independent last-writer-wins register.
//!
//! Applying a remote change does exactly:
//!   `UPDATE <table> SET <column> = ? WHERE id = ?`
//! — only the relevant column, keyed on id + table (the user's two requirements).
//! So two replicas editing *different* columns of the same row both survive, and
//! edits to the *same* cell converge on the larger `(lamport, site)`.
//!
//! This is the kanban model generalized: a row is an id-keyed document, a column
//! is an LWW field. SQLite is just the local store (Shape B) — riftpipe owns the
//! merge. Synced tables must have a TEXT primary key column named `id`.
//!
//! Status: standalone engine + tests. Wiring into the `Syncer`/folder seam (DB
//! resources are driven by `set()`, not file snapshots) is the next step.

use std::collections::HashMap;
use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

/// Errors from any DB / (de)serialization step.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// A SQLite value — the content of one cell.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Cell {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// A change to a single cell — the unit of sync.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Change {
    pub table: String,
    pub id: String,
    pub column: String,
    pub value: Cell,
    pub lamport: u64,
    pub site: String,
}

/// A SQLite database whose rows sync per-cell, last-writer-wins.
pub struct SqliteSync {
    conn: Connection,
    site: String,
    lamport: u64,
}

impl SqliteSync {
    /// Open an in-memory database with the given stable replica id.
    pub fn memory(site: &str) -> Result<Self> {
        Self::from_conn(Connection::open_in_memory()?, site)
    }

    /// Open (or create) a file-backed database.
    pub fn open(path: impl AsRef<Path>, site: &str) -> Result<Self> {
        Self::from_conn(Connection::open(path)?, site)
    }

    fn from_conn(conn: Connection, site: &str) -> Result<Self> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _rp_meta (k TEXT PRIMARY KEY, v TEXT);
             CREATE TABLE IF NOT EXISTS _rp_cell (
                 tbl TEXT, id TEXT, col TEXT, lamport INTEGER, site TEXT,
                 PRIMARY KEY (tbl, id, col));
             CREATE TABLE IF NOT EXISTS _rp_log (
                 seq INTEGER PRIMARY KEY AUTOINCREMENT,
                 site TEXT, lamport INTEGER, tbl TEXT, id TEXT, col TEXT, val BLOB);",
        )?;
        // The site id is sticky: first one wins, so a replica keeps its identity
        // across reopens regardless of what's passed.
        let site: String = match conn
            .query_row("SELECT v FROM _rp_meta WHERE k='site'", [], |r| r.get(0))
            .optional()?
        {
            Some(s) => s,
            None => {
                conn.execute("INSERT INTO _rp_meta (k, v) VALUES ('site', ?1)", params![site])?;
                site.to_string()
            }
        };
        let lamport: u64 = conn
            .query_row("SELECT v FROM _rp_meta WHERE k='lamport'", [], |r| r.get::<_, String>(0))
            .optional()?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        Ok(SqliteSync { conn, site, lamport })
    }

    /// Run schema DDL (the app creates its tables; each must have a TEXT `id` PK).
    pub fn execute(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql)?;
        Ok(())
    }

    /// A local write to one cell. Stamps a fresh (highest) lamport, so it wins.
    pub fn set(&mut self, table: &str, id: &str, column: &str, value: Cell) -> Result<()> {
        self.lamport += 1;
        let ch = Change {
            table: table.to_string(),
            id: id.to_string(),
            column: column.to_string(),
            value,
            lamport: self.lamport,
            site: self.site.clone(),
        };
        self.record(&ch)?;
        Ok(())
    }

    /// Read one cell's current value (`None` if the row/cell doesn't exist).
    pub fn get(&self, table: &str, id: &str, column: &str) -> Result<Option<Cell>> {
        if !is_ident(table) || !is_ident(column) {
            return Ok(None);
        }
        let sql = format!("SELECT \"{column}\" FROM \"{table}\" WHERE id = ?1");
        let cell = self
            .conn
            .query_row(&sql, params![id], |r| r.get::<_, rusqlite::types::Value>(0))
            .optional()?
            .map(value_to_cell);
        Ok(cell)
    }

    /// Our version vector: highest lamport seen per site. "Here's what I hold."
    pub fn state_vector(&self) -> Result<Vec<u8>> {
        let mut stmt = self.conn.prepare("SELECT site, MAX(lamport) FROM _rp_log GROUP BY site")?;
        let mut vv: HashMap<String, u64> = HashMap::new();
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64)))?;
        for row in rows {
            let (s, l) = row?;
            vv.insert(s, l);
        }
        Ok(serde_json::to_vec(&vv)?)
    }

    /// The cell changes a peer is missing, given their version vector. We send the
    /// *current winning* value per cell (LWW means older history is irrelevant).
    /// `None` when they're already caught up.
    pub fn delta_since(&self, theirs: &[u8]) -> Result<Option<Vec<u8>>> {
        let theirs: HashMap<String, u64> = serde_json::from_slice(theirs).unwrap_or_default();
        let mut stmt = self
            .conn
            .prepare("SELECT site, lamport, tbl, id, col, val FROM _rp_log ORDER BY seq")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)? as u64,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Vec<u8>>(5)?,
            ))
        })?;
        // Collapse the log to the latest change per cell.
        let mut latest: HashMap<(String, String, String), Change> = HashMap::new();
        for row in rows {
            let (site, lamport, table, id, column, val) = row?;
            let value: Cell = postcard::from_bytes(&val)?;
            let key = (table.clone(), id.clone(), column.clone());
            let ch = Change { table, id, column, value, lamport, site };
            match latest.get(&key) {
                Some(e) if (e.lamport, e.site.as_str()) >= (ch.lamport, ch.site.as_str()) => {}
                _ => {
                    latest.insert(key, ch);
                }
            }
        }
        let mut out: Vec<Change> = latest
            .into_values()
            .filter(|ch| ch.lamport > *theirs.get(&ch.site).unwrap_or(&0))
            .collect();
        if out.is_empty() {
            return Ok(None);
        }
        out.sort_by(|a, b| (a.lamport, &a.site).cmp(&(b.lamport, &b.site)));
        Ok(Some(serde_json::to_vec(&out)?))
    }

    /// Merge a peer's changes; returns how many were actually applied (won LWW).
    pub fn merge(&mut self, delta: &[u8]) -> Result<usize> {
        let changes: Vec<Change> = serde_json::from_slice(delta)?;
        let mut applied = 0;
        for ch in &changes {
            if self.record(ch)? {
                applied += 1;
            }
        }
        Ok(applied)
    }

    /// Apply one change with per-cell LWW: only if its (lamport, site) beats the
    /// stored cell version. Updates only that column, keyed on id + table.
    fn record(&mut self, ch: &Change) -> Result<bool> {
        if !is_ident(&ch.table) || !is_ident(&ch.column) {
            return Ok(false); // never interpolate an unvalidated identifier
        }
        // Keep our clock ahead of anything we've seen (Lamport rule).
        if ch.lamport > self.lamport {
            self.lamport = ch.lamport;
        }

        let stored: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT lamport, site FROM _rp_cell WHERE tbl=?1 AND id=?2 AND col=?3",
                params![ch.table, ch.id, ch.column],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let wins = match &stored {
            None => true,
            Some((l, s)) => (ch.lamport, ch.site.as_str()) > (*l as u64, s.as_str()),
        };
        if !wins {
            self.persist_lamport()?;
            return Ok(false);
        }

        // Ensure the row exists, then set only the changed column — keyed on id.
        self.conn.execute(
            &format!("INSERT OR IGNORE INTO \"{}\" (id) VALUES (?1)", ch.table),
            params![ch.id],
        )?;
        self.conn.execute(
            &format!("UPDATE \"{}\" SET \"{}\" = ?1 WHERE id = ?2", ch.table, ch.column),
            params![cell_to_value(&ch.value), ch.id],
        )?;
        // Record the new cell version + append to the change log.
        self.conn.execute(
            "INSERT INTO _rp_cell (tbl, id, col, lamport, site) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(tbl, id, col) DO UPDATE SET lamport=excluded.lamport, site=excluded.site",
            params![ch.table, ch.id, ch.column, ch.lamport as i64, ch.site],
        )?;
        self.conn.execute(
            "INSERT INTO _rp_log (site, lamport, tbl, id, col, val) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![ch.site, ch.lamport as i64, ch.table, ch.id, ch.column, postcard::to_allocvec(&ch.value)?],
        )?;
        self.persist_lamport()?;
        Ok(true)
    }

    fn persist_lamport(&self) -> Result<()> {
        self.conn.execute(
            "INSERT INTO _rp_meta (k, v) VALUES ('lamport', ?1)
             ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            params![self.lamport.to_string()],
        )?;
        Ok(())
    }
}

fn cell_to_value(c: &Cell) -> rusqlite::types::Value {
    use rusqlite::types::Value;
    match c {
        Cell::Null => Value::Null,
        Cell::Int(i) => Value::Integer(*i),
        Cell::Real(r) => Value::Real(*r),
        Cell::Text(s) => Value::Text(s.clone()),
        Cell::Blob(b) => Value::Blob(b.clone()),
    }
}

fn value_to_cell(v: rusqlite::types::Value) -> Cell {
    use rusqlite::types::Value;
    match v {
        Value::Null => Cell::Null,
        Value::Integer(i) => Cell::Int(i),
        Value::Real(r) => Cell::Real(r),
        Value::Text(s) => Cell::Text(s),
        Value::Blob(b) => Cell::Blob(b),
    }
}

/// A safe SQL identifier (we must interpolate table/column names — params can't
/// bind identifiers — so reject anything but `[A-Za-z_][A-Za-z0-9_]*`).
fn is_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .enumerate()
            .all(|(i, c)| c == '_' || (c.is_ascii_alphanumeric() && !(i == 0 && c.is_ascii_digit())))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str =
        "CREATE TABLE cards (id TEXT PRIMARY KEY, title TEXT, lane TEXT, done INTEGER)";

    fn pair() -> (SqliteSync, SqliteSync) {
        let a = SqliteSync::memory("a").unwrap();
        let b = SqliteSync::memory("b").unwrap();
        a.execute(SCHEMA).unwrap();
        b.execute(SCHEMA).unwrap();
        (a, b)
    }

    /// Sync everything both ways until stable.
    fn reconcile(a: &mut SqliteSync, b: &mut SqliteSync) {
        for _ in 0..2 {
            if let Some(d) = a.delta_since(&b.state_vector().unwrap()).unwrap() {
                b.merge(&d).unwrap();
            }
            if let Some(d) = b.delta_since(&a.state_vector().unwrap()).unwrap() {
                a.merge(&d).unwrap();
            }
        }
    }

    /// Requirement 1: concurrent edits to *different columns* of the same row both
    /// survive (per-column update, no whole-row clobber).
    #[test]
    fn different_columns_of_same_row_both_survive() {
        let (mut a, mut b) = pair();
        a.set("cards", "x", "title", Cell::Text("Hello".into())).unwrap();
        b.set("cards", "x", "lane", Cell::Text("Doing".into())).unwrap();
        reconcile(&mut a, &mut b);
        for s in [&a, &b] {
            assert_eq!(s.get("cards", "x", "title").unwrap(), Some(Cell::Text("Hello".into())));
            assert_eq!(s.get("cards", "x", "lane").unwrap(), Some(Cell::Text("Doing".into())));
        }
    }

    /// Same cell edited on both sides converges to one deterministic winner.
    #[test]
    fn same_cell_resolves_last_writer_wins() {
        let (mut a, mut b) = pair();
        a.set("cards", "y", "title", Cell::Text("A".into())).unwrap();
        b.set("cards", "y", "title", Cell::Text("B".into())).unwrap();
        reconcile(&mut a, &mut b);
        let av = a.get("cards", "y", "title").unwrap();
        assert_eq!(av, b.get("cards", "y", "title").unwrap(), "diverged");
        // equal lamport (1) -> tiebreak on larger site id: "b" > "a".
        assert_eq!(av, Some(Cell::Text("B".into())));
    }

    /// Requirement 2: updates are keyed on id — different rows are independent.
    #[test]
    fn updates_are_keyed_on_id() {
        let (mut a, mut b) = pair();
        a.set("cards", "r1", "title", Cell::Text("one".into())).unwrap();
        a.set("cards", "r2", "title", Cell::Text("two".into())).unwrap();
        reconcile(&mut a, &mut b);
        assert_eq!(b.get("cards", "r1", "title").unwrap(), Some(Cell::Text("one".into())));
        assert_eq!(b.get("cards", "r2", "title").unwrap(), Some(Cell::Text("two".into())));
    }

    /// A later edit to one column leaves the row's other columns untouched.
    #[test]
    fn updating_one_column_preserves_the_others() {
        let (mut a, mut b) = pair();
        a.set("cards", "z", "title", Cell::Text("T".into())).unwrap();
        a.set("cards", "z", "done", Cell::Int(0)).unwrap();
        reconcile(&mut a, &mut b);
        a.set("cards", "z", "done", Cell::Int(1)).unwrap();
        reconcile(&mut a, &mut b);
        assert_eq!(b.get("cards", "z", "title").unwrap(), Some(Cell::Text("T".into())));
        assert_eq!(b.get("cards", "z", "done").unwrap(), Some(Cell::Int(1)));
    }

    /// Once converged, neither side has anything left to send.
    #[test]
    fn nothing_to_send_when_in_sync() {
        let (mut a, mut b) = pair();
        a.set("cards", "x", "title", Cell::Text("hi".into())).unwrap();
        reconcile(&mut a, &mut b);
        assert!(a.delta_since(&b.state_vector().unwrap()).unwrap().is_none());
        assert!(b.delta_since(&a.state_vector().unwrap()).unwrap().is_none());
    }

    #[test]
    fn rejects_unsafe_identifiers() {
        let mut a = SqliteSync::memory("a").unwrap();
        a.execute(SCHEMA).unwrap();
        // a bogus table/column name is refused, not interpolated into SQL.
        assert!(a.get("cards; DROP TABLE cards", "x", "title").unwrap().is_none());
        a.set("cards", "x", "title); DROP TABLE cards--", Cell::Text("x".into())).unwrap();
        // table still intact
        assert!(a.get("cards", "x", "title").unwrap().is_none());
    }
}
