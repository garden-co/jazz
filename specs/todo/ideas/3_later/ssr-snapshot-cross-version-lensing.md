# SSR Snapshot Cross-Version Lensing

## What

Make SSR snapshot hydration work when the client is on a different schema version than the server that produced the snapshot, by having the client reinterpret the snapshot through the lenses it holds — and only fall back to a live fetch when it genuinely can't.

## Status quo

SSR snapshot seeding only works when the client schema matches the server schema. `applySnapshot` (`packages/jazz-tools/src/react-core/apply-snapshot.ts`) compares the snapshot's `schemaFingerprint` against the client's and, on any mismatch, discards the snapshot and falls back to a live fetch. So a client on an older or newer build than the server gets no first-paint seeding — it pays a network round-trip even though the data is right there in the snapshot.

## Target behaviour

The server always emits one snapshot at its own schema version; each client adapts it to its own version.

- Server is on schema v3; client A is on v1, client B is on v2.
- A makes a write. The core lenses A's v1 rows forward to v3, so the server-generated snapshot is uniformly v3-shaped.
- B receives the v3 snapshot. If B holds the lens chain between v3 and v2, it applies the backward lens (v3 → v2) to the snapshot rows and seeds its cache with no network. If B lacks the chain (too far behind, missing migration) or an entry's shape can't be lensed, it discards and falls back to a live fetch.

This mirrors the live read path, where the auto-generated backward lens already lets older clients read newer data; the SSR snapshot path is the one place that bypasses it.

## Approach

Delegate the reinterpretation to the core. Hand the snapshot rows plus the target schema to the WASM core and let it apply the same canonical lens logic the live path uses, rather than re-implementing lens ops in TS. The core already fetches and holds the published migration chain, and the transform is synchronous and in-memory, so the no-flash first-paint benefit is preserved.

## Gaps to close

- **Locate the snapshot in version-space.** The envelope carries `schemaFingerprint` (FNV-1a, `drivers/schema-wire.ts`), but migrations are keyed by the structural `computeSchemaHash`. The envelope needs to carry the structural hash so the client can find the lens chain from the snapshot's version to its own.
- **Reinterpret instead of discard.** `applySnapshot` must locate the lens chain (snapshot version → client version), apply it to each entry's rows via the core, and seed; falling back to a live fetch only when the chain is unavailable, or when an entry's result shape can't be lensed (lenses are table-shaped, so results with joins/includes need handling or exclusion).

## Related

- [Lens Hardening](../1_mvp/lens-hardening.md) — mixed-version lens semantics (deterministic path selection, lens revisions, type-changing migrations). Cross-version snapshot lensing leans on those edge cases being solid.
