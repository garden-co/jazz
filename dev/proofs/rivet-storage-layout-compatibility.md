# Rivet actor storage layout v1

## Actor identity

On each wake, the Jazz core derives its app ID with `AppId::from_name(ctx.actor_id())`, UUIDv5 over the UTF-8 bytes of the Rivet actor ID in the DNS namespace. The deployment key lets the backend look up the actor. The actor ID returned by Rivet determines the Jazz app ID. The Durable Object host persists its app ID in `_jazz_host_app`; the Rivet host recomputes it whenever it opens the core. [Rivet actor](../../crates/jazz-rivet/src/actor.rs) · [App ID derivation](../../crates/jazz/layers/types/src/app_id.rs)

Sleep and wake preserve the actor ID and SQLite database, so the same Jazz app reopens. If Rivet creates a replacement actor with a different ID, even under the same deployment key, Jazz derives a different app ID. A fresh actor starts with its own database and default blob prefix. Copying the old database into the replacement would preserve any recorded blob destination, but the new app ID would fail the ownership check on the old actor’s claimed prefix. Restoring under the same Jazz identity requires preserving the actor ID. Moving that identity to a different actor ID would require an explicit migration.

## SQLite and the blob destination

Jazz uses its existing SQLite layout for rows, schema catalogue, accounts and large-value metadata in the actor’s database. With `blobs: "sqlite"`, the large-value bytes live there too. The Rivet host adds one table to record an external destination. [SQLite adapter](../../crates/jazz-rivet/src/storage.rs) · [Destination encoding and byte fixtures](../../crates/jazz-rivet/src/blobs.rs)

```sql
CREATE TABLE IF NOT EXISTS jazz_rivet_blobs_v1 (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    destination TEXT NOT NULL
)
```

On startup the host creates the table if absent. An empty table means SQLite, including for actors created by an earlier SQLite-only runner. When moving to external storage, the core validates the destination, drains any open runtime and inserts row `1` before the new runtime can offload chunks. The JSON records the resolved S3 bucket, region, endpoint and prefix, or the Vercel Blob store ID and prefix. Credentials come from the runner environment. Reopening with the same resolved destination succeeds. Selecting another external destination or switching back to SQLite is refused.

An upgrade from SQLite can therefore move existing chunks out over time. Once chunks have moved, a downgrade to a runner that only reads SQLite cannot recover those bytes from the actor database alone.

## External objects

The default prefix is `jazz/<appId>/`. A nonempty explicit prefix overrides it. Under that prefix, `.jazz-owner` contains the app ID in UTF-8 followed by one LF byte. Each chunk’s key is the 64-character lowercase hexadecimal SHA-256 digest of its encoded node bytes, and the object body contains those bytes. Both adapters check ownership before accessing chunks. [Chunk keys](../../crates/jazz/layers/db/src/chunk_store.rs) · [S3 adapter](../../crates/jazz-server/src/s3.rs) · [Vercel Blob adapter](../../crates/jazz-server/src/vercel_blob.rs)

The [Rivet integration test](../../packages/jazz-tools/src/backend/rivet-store.integration.test.ts) writes a large value to SQLite, moves it to S3, sleeps and wakes the actor, checks that switching back is refused, and reads the original value from S3. Future changes to app ID derivation, destination encoding or object naming must preserve the ability to reopen v1 actors and read and reclaim their chunks, or provide a migration before retiring the v1 layout.
