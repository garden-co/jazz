# Text editor

A minimal CodeMirror + Yjs editor persisted and synced through Jazz. One document
row holds an append-only byte log. The adapter subscribes directly to that column
and uses a local offset to apply only newly appended Yjs records. Jazz handles
chunk-level sync; the JavaScript subscription still receives the complete byte
value. Opening a document replays its full log.

```sh
pnpm --filter text-editor dev
```

Create a document, then open the same URL in another browser. Documents are public
scratchpads: anyone can read and edit them. Take turns editing. Concurrent appends
still use Jazz's ordinary column merge rules and can lose edits from the visible
log. This example does not implement compaction or concurrent-writer reconciliation.

## Stored format: Jazz Yjs Log v1

The eight-byte header is `4a 59 4c 47 00 00 00 01` (`JYLG`, followed by version 1
as an unsigned big-endian 32-bit integer). Each following record contains an
unsigned big-endian 32-bit payload length and one Yjs **v1** binary update, emitted
by `doc.on("update")`. Empty records and truncated records are rejected.

Each record is appended with Jazz's byte-splice API, using the current value's
length as its offset. Loading tags updates with a provider origin
so they are not appended again. Writes are serialized and wait for local persistence.

Compatibility: this format uses Yjs's v1 update API, not its experimental v2 update
encoding. Readers reject unknown headers/versions. Any incompatible framing or
payload change requires a new version and an explicit migration; existing logs must
not be silently reinterpreted. `src/log.test.ts` pins header, framing, and a complete
Yjs update with literal byte fixtures.

```sh
pnpm --filter text-editor typecheck
pnpm --filter text-editor test
pnpm --filter text-editor test:e2e
pnpm --filter text-editor build
```
