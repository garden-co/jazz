---
"jazz-tools": patch
---

**Breaking (Rust `jazz` crate):** rows delivered by `JazzClient` subscriptions (`jazz::tools::Row`) now always carry their decoded `fields`, not only with the `testing` feature. As a result, `Row` no longer implements `Eq` (it is still `PartialEq`), and code that builds a `Row` with a struct literal must set `fields` (or use `Row::new`).
