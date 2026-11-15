//! Todo Server: a REST + SSE API over a Jazz app, from Rust.
//!
//! The app is `jazz.app.json` (its tables) and `jazz.permissions.json`
//! (who may do what). `src/jazz_app.rs` holds the typed bindings
//! `jazz-tools generate` generates from `jazz.app.json`; the service opens
//! the app as its backend with them (see `main.rs`).

pub mod jazz_app;
mod routes;

pub use routes::{CreateTodoRequest, Stopping, Todo, UpdateTodoRequest, router, router_until};
