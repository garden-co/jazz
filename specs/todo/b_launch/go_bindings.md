# Go Bindings — TODO

Go client library for jazz2 via C FFI.

## Overview

Expose groove's core functionality to Go programs:

- C FFI layer over groove (Rust → `extern "C"` → Go `cgo`)
- Go-idiomatic wrapper: `db.Query()`, `db.Exec()`, iterators
- Embedded mode (in-process SurrealKV storage) and client mode (sync to server)
- Schema definition in Go structs with codegen or reflection

## Open Questions

- CGo vs. pure Go reimplementation of the sync protocol?
- How to handle the reactive query model in Go (channels? callbacks?)
- Cross-compilation: how to ship pre-built Rust `.a`/`.so` for common platforms?
- Error handling: Go errors vs. panics for Rust panics?
- Testing: Go test suite that exercises the same scenarios as Rust tests?
