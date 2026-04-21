---
name: jazz-go-bindings
description: Build or extend the generator-first Go bindings for Jazz2 using exported schema JSON and the stable app-scoped transport routes.
version: 1
---

# Jazz Go Bindings

Use this skill when the task is about Go integration with Jazz2, especially for
backend or daemon code such as `prom-tip-sync`.

## Model

Do not start by wrapping the whole Rust runtime.

Prefer this boundary:

1. export schema with `jazz-tools schema export`
2. generate typed Go code from the exported schema JSON
3. use the stable server routes for runtime interaction

That keeps the Go surface aligned with existing Jazz contracts instead of
creating a second hidden runtime.

## Current module

- module root: `go/jazzgo`
- generator CLI: `go/jazzgo/cmd/jazzgo-gen`
- stable client surface:
  - `Health`
  - `SchemaHashes`
  - `SchemaByHash`
  - `AppWSURL`

## Workflow

1. Read `go/jazzgo/README.md`.
2. Inspect the schema/transport sources only as needed:
   - `packages/jazz-tools/src/drivers/types.ts`
   - `packages/jazz-tools/src/schema-loader.ts`
   - `crates/jazz-tools/src/routes.rs`
   - `specs/status-quo/http_transport.md`
3. Write tests first in `go/jazzgo/*_test.go`.
4. Keep the first slice generator-first and transport-thin.
5. Verify with:

```bash
cd go/jazzgo && go test ./...
```

## Good next tasks

- schema export JSON normalization
- Go code generation for typed rows/inserts/enums
- route clients for app-scoped schema/admin APIs
- Go-side relation-IR/query helpers that mirror the existing TS query adapter

## Avoid

- hand-maintained Go structs for each app schema
- direct daemon logic that assumes `~/agents/sync/readme.md` owns external repo freshness
- broad Rust FFI unless the generator-first surface is proven insufficient
