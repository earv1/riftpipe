# agent.md — durable working rules for AI agents in this repo

## Project principles (locked)

- **Generic core, kanban as showcase.** The riftpipe binary and crates contain
  **no app-specific (kanban) code**. Apps live under `projects/<app>/` (their
  own crate if Rust, depending on `riftpipe-core` by path) and compose with
  riftpipe through the filesystem / the core sync protocol. The binary exposes
  only generic verbs (`share`, `join`, `connect`, `signal`). Known violations
  to burn down are tracked in the planning docs.
- **No homegrown editor.** Never build an editor into riftpipe or its apps;
  integrate with existing ones (Neovim `--pipe` bridge, `$EDITOR`, any program
  speaking the JSON-lines pipe protocol).
- **Snapshot is the interface.** "Pipe me bytes, I converge" is the thesis.
  Don't add per-producer op protocols; invest in the one diff path
  (snapshot → Myers diff → eg-walker ops).
- **DB rows as documents.** Future DB sync = each row an id-keyed document
  with per-row conflict resolution; one local writer, so locking is moot —
  the kanban file-per-entity pattern generalized.

## Repo practicalities

- **Never commit or push to the default branch**; a hook also blocks any shell
  command containing the word for that branch — reference files like
  `src/<entrypoint>.rs` via Read/Edit tools, never in Bash command strings,
  and use `HEAD~N` style git refs.
- Work lands on feature branches; the user pushes and merges PRs.
- `tests/networking.rs::capability_negotiation_over_real_iroh` is flaky (real
  n0-relay dependency); a failure there alone is not a regression — rerun it.
- `web/` is a separate wasm crate excluded from the workspace; verify it with
  `web/test-headless.sh`, not `cargo test`.
- Status/tracking docs to keep current when work lands:
  `docs/planned/roadmap.md` (core roadmap), `PROJECT.md` (resume notes + TODO),
  `projects/kanban/docs/planned.md` (kanban app plans),
  `docs/architecture.md` (diagrams — validate mermaid with `mmdc`).
- **Background agents get a 10-minute timer, max.** When spawning a background
  subagent, also start a ~600 s background timer; when it fires and the agent
  is still running, ping it to wrap up and report partial state — and stop it
  (TaskStop) rather than letting it run unbounded. Prefer scoping tasks so
  they fit inside 10 minutes (split big jobs into phases).
- Record new durable rules in **this file**, not in session memory.
