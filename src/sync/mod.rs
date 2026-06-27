//! Text reconciliation: the editor edit-stream protocol over --pipe (pipe), with
//! version-vector desync recovery, and the file-mirror loop (mirror).

pub mod mirror;
pub mod pipe;
