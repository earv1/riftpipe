# Wire up folder sync

The board directory is synced by riftpipe in folder mode. `card.md` and
`board.md` use text-crdt; `meta.toml` uses rsync (last-writer-wins).
