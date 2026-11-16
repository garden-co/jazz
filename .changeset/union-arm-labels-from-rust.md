---
"jazz-tools": patch
---

Unnamed `union([...])` arms are labelled once, in Rust (`jazz.union-arm-label.v2`), so TypeScript, Rust and FFI clients give the same union the same shape id. TypeScript no longer hashes arms itself; Rust's `Query::union` and FFI unions no longer number arms and refuse two identical unnamed arms (name them with `union({...})`, `Query::union_named` or an FFI arm `"label"`). Shape ids of unnamed unions change once.
