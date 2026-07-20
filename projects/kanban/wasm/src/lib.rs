//! `kanban-wasm` — the app's single wasm bundle. It is a **thin passthrough**:
//! it links `riftpipe-web` as an rlib so wasm-bindgen surfaces that crate's
//! exports (the generic file API — `readFile`/`writeFile`/`listDir` — plus
//! `irohConnect`, the auth surface, `RiftDoc`, …) from this one cdylib.
//!
//! There is deliberately **nothing kanban-specific here**. The board is a plain
//! file tree (`board.md`, `tickets/<id>/{card.md,meta.toml}`, comments), and the
//! whole domain model — parsing that layout, computing card positions, etc. —
//! lives in TypeScript (`src/board.ts`) on top of the generic file API. Any
//! file-based app reuses the same wasm bundle; only its JS differs.
//!
//! Build with `wasm-pack build --target web`.

// Re-export the generic library so its `#[wasm_bindgen]` exports are linked into
// this cdylib (a referenced dependency is what keeps them in the final wasm).
pub use riftpipe_web::*;
