# BandChat

BandChat is the first canonical app in the examples-and-benchmarks program: a small, polished React room chat with local-first writes, identity, membership permissions, and byte attachments.

```bash
pnpm --dir examples/band-chat install
pnpm --dir examples/band-chat dev
```

Open the demo room, send a message (or attach a small file), then briefly disconnect/reconnect: the UI writes locally and Jazz syncs when connectivity returns.

## Contract

- `schema.ts` and `permissions.ts` define the identity and membership boundary. A user can only read a room after becoming a member, and can write messages only in a member room.
- `src/fixture.ts` is the versioned, deterministic, public/name-blind smoke fixture.
- `src/scenario.ts` is the framework-neutral workload contract. A headless check verifies its determinism; UI/E2E runners consume the same operation names.

`s.bytes()` is used for the initial attachment path, which is supported by the current stable public schema API. Attachments are allow-listed (PNG/JPEG/WebP/text/PDF) and capped at 256 KB before bytes are read. They are inline bytes, not large-value/file streaming; larger uploads and image transforms remain a follow-up.

The browser receipt proves a local write is retained through a reconnect attempt against a deployed local Jazz server. A fresh-store, server-delivery assertion is currently blocked by the room bootstrap/replay path and is intentionally not claimed as coverage. SharedWorker/two-context, worker restart, and native persistence remain follow-up topology coverage rather than simulated support.

`roomMembers` and `messages` intentionally have no update policy: Jazz's enforcing runtime defaults those operations to deny. This app treats membership changes as owner-controlled insert/delete operations and messages as immutable.

## Checks

```bash
pnpm --dir examples/band-chat test:headless
pnpm --dir examples/band-chat build
```
