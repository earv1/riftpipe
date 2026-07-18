// riftpipe — typed TypeScript glue over the riftpipe CLI. The seams are
// language-agnostic (JSON lines on stdio for `--pipe`, `SYNCED:` breadcrumbs
// from `connect`, HTTP+SSE from `serve`); this package spawns the prebuilt
// binary and types those protocols, so building on riftpipe needs no Rust.

export { riftpipeBinary } from "./binary.js";
export { PipeSession, applyEditOp, type EditOp, type PipeSessionOpts } from "./pipe.js";
export { TreeSession, type TreeSessionOpts } from "./tree.js";
export { Serve, type ChangeEvent, type ServeOpts } from "./serve.js";
export { spawnRiftpipe, type SpawnHandle } from "./process.js";
