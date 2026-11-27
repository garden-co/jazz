# Durable Object external blob layout v1

R2, S3, and Vercel Blob use the Durable Object's existing R2 layout. A deployment upgrading with the same R2 bucket and prefix can keep reading and reclaiming its objects in place. The additional adapters write the same keys and bytes. Selecting a different destination does not copy existing data.

The default prefix is `jazz/<app-id>/`, where the Durable Object keeps its app ID in SQLite. An explicit nonempty prefix replaces the default, with a trailing slash appended if needed. Keys below are relative to that prefix.

| Key                                    | Body                                                                                                                                      |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `.jazz-owner`                          | UTF-8 app ID followed by one LF byte (`0a`). The first claimant creates this object with overwrites disabled and checks the stored owner. |
| Lowercase, 64-character SHA-256 digest | Exact encoded large-value node bytes from the engine. The digest covers the entire encoded node, including its framing.                   |

SQLite records the claimed prefix. Reopening checks the owner before accessing chunks and refuses a changed prefix, a missing recorded owner, or another app's claim. Reclamation deletes the digest key under the claimed prefix.

The name **v1** covers this existing layout. Its keys have no version segment. A future incompatible change to key construction, ownership, or stored bytes must specify how a deployment with v1 data can still read and reclaim its chunks, either by retaining v1 support or by migrating its existing references and objects before retiring v1.

The [layout fixture](../../crates/jazz-wasm/tests/durable-objects/blob-layout.test.mjs) reads pre-existing objects and checks the keys and bytes written through each adapter. Its leaf is the [engine's v1 codec fixture](../../crates/jazz/layers/types/src/large_values/mod.rs) from `v1_codec_golden_bytes_decode_semantically_and_reject_alternates`.
