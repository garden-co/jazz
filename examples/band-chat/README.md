# BandChat

BandChat is the first canonical app in the examples-and-benchmarks program: a small, self-contained Next.js room chat with Better Auth, local-first writes, membership permissions, and byte attachments.

```bash
pnpm --dir examples/band-chat install
pnpm --dir examples/band-chat dev
```

Open the demo room, send a message (or attach a small file), then briefly disconnect/reconnect: the UI writes locally and Jazz syncs when connectivity returns.

The production path uses external auth end to end. Better Auth owns the cookie
session and publishes JWT/JWKS endpoints. `/api/bootstrap` validates that session
server-side and idempotently creates the Jazz profile through the app's sole
backend-secret context, keyed by Better Auth's stable user id. Only after that
request succeeds does the client obtain a JWT and mount the ordinary Jazz UI.
Rooms remain explicit user-created state. No backend secret reaches normal UI
reads or writes.

## Contract

The React structure is intentionally split by data ownership: the session shell
only resolves identity, the room workspace/list owns room queries, the selected
conversation owns its room/message queries, and the composer owns the current
profile query plus writes. Reads remain declarative and have no provisioning
side effects. The empty-state button invokes a separate idempotent provisioning
function; this makes normal subscription renders safe to replay or remount.

Room ownership and message time are Jazz provenance metadata, not user-written
columns: policies use `$createdBy`, and the conversation explicitly selects and
orders by `$createdAt` for display. Writes never supply either value.

- `schema.ts` and `permissions.ts` define the identity and membership boundary. A user can only read a room after becoming a member, and can write messages only in a member room.
- `src/fixture.ts` is the versioned, deterministic, public/name-blind smoke fixture.
- `src/scenario.ts` is the framework-neutral workload contract. A headless check verifies its determinism; UI/E2E runners consume the same operation names.

`s.bytes()` is used for the initial attachment path, which is supported by the current stable public schema API. Attachments are allow-listed (PNG/JPEG/WebP/text/PDF) and capped at 256 KB before bytes are read. They are inline bytes, not large-value/file streaming; larger uploads and image transforms remain a follow-up.

The browser receipt uses Jazz's canonical browser test-server RPC harness: it blocks HTTP and WebSocket traffic, creates the room/message offline, restores connectivity, then proves a fresh IndexedDB store receives the server-delivered result. Keep app browser tests on that harness rather than starting a server in app-local `globalSetup`; the latter is not controlled by the browser context and cannot inject deterministic network faults. SharedWorker/two-context, worker restart, and native persistence remain follow-up topology coverage rather than simulated support.

`roomMembers` and `messages` intentionally have no update policy: Jazz's enforcing runtime defaults those operations to deny. This app treats membership changes as owner-controlled insert/delete operations and messages as immutable.

## Checks

```bash
pnpm --dir examples/band-chat test:headless
pnpm --dir examples/band-chat build
```
