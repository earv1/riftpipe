# riftpipe examples

Generic CLI showcases — one small script per sync **mode**. They're app-agnostic:
each drives riftpipe against plain data (a folder, a db) and never knows what an
app makes of it. Use them to sync a notes directory between two laptops, or to
join a folder an app hosts (e.g. a browser board) as a live file tree.

| Script | Mode | Status | What it syncs |
|--------|------|--------|---------------|
| [`folder.sh`](folder.sh) | folder | works | a directory tree — `*.md` merges as a CRDT, the rest last-writer-wins |
| [`db.sh`](db.sh) | db | planned | a write-ahead log — frames linearized into one order (`wal-db`) |

Both use the same generic verbs:

```sh
./folder.sh host <dir>                       # print a ticket, wait for a peer
./folder.sh join <ticket|link|id> <dir>      # sync <dir> with that peer
```

riftpipe ships the sync; the app supplies the meaning. See
[`../docs/planned/wal-db.md`](../docs/planned/wal-db.md) for the db mode's design.
