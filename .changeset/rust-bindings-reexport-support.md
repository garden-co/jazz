---
"jazz-tools": patch
---

**Rust: the generated bindings re-export `jazz-app-support`.** `src/jazz_app.rs` now starts with `pub use ::jazz_app_support::*;`, so app code names everything through one module: `jazz_app::Jazz`, `jazz_app::Config`, `jazz_app::Result`, `use jazz_app::{Jazz, Todo, todos};`. The bindings' own items win where a name is the same (a table `errors` is still `jazz_app::Error`; the crate's stays `jazz_app_support::Error`). Existing bindings keep working; the next `npx jazz-tools generate` adds the line. `jazz_app_support::Result` now defaults its error (`Result<T, E = Error>`), so std's two-parameter `Result<(), String>` still works where the bindings or the crate are glob-imported. The bindings format is unchanged.
