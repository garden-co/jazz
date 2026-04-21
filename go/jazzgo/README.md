# jazzgo

Generator-first Go bindings for Jazz2.

This module is meant for trusted backend/control-plane code such as
`prom-tip-sync`, not for embedding the full Jazz runtime in Go.

Current scope:

- parse exported Jazz schema JSON
- generate typed Go models and table metadata
- talk to the stable server routes:
  - `/health`
  - `/apps/:app_id/ws`
  - `/apps/:app_id/schemas`
  - `/apps/:app_id/schema/:hash`

## Why this shape

Jazz2 already has two stable surfaces that are good binding seams:

- compiled schema JSON from `jazz-tools schema export`
- a small app-scoped HTTP/WebSocket transport

That makes a generator-first Go module a better first step than trying to wrap
the Rust runtime directly.

## Generate code

First export the schema:

```bash
node packages/jazz-tools/bin/jazz-tools.js schema export --schema-dir ./examples/world-tour > /tmp/world-tour.schema.json
```

Then generate Go:

```bash
cd go/jazzgo
go run ./cmd/jazzgo-gen -schema /tmp/world-tour.schema.json -pkg worldtour -out /tmp/worldtour_gen.go
```

The generated file contains:

- row structs with `ID` plus typed columns
- insert structs
- enum types for enum columns
- table/column metadata constants

## Use the client

```go
client := jazzgo.NewClient("http://localhost:4200", "313aa802-8598-5165-bb91-dab72dcb9d46")

health, err := client.Health(ctx)
hashes, err := client.SchemaHashes(ctx)
stored, err := client.SchemaByHash(ctx, hashes.Hashes[0])
wsURL := client.AppWSURL()
```

## Intended daemon path

For `~/agents/sync/readme.md` style control-plane work, the intended stack is:

1. export the app schema once
2. generate typed Go models/metadata with `jazzgo-gen`
3. use `jazzgo.Client` for health/schema discovery
4. add query/mutation/subscription support on top of the same generated schema

## Next slices

- generate typed relation-IR query helpers from the schema
- add Go encoders/decoders for Jazz `Value` and row payloads
- add WebSocket sync client support for trusted backend subscriptions
- add admin client helpers for schema/migration/permissions publication
