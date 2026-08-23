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

`s.bytes()` is used for the initial attachment path, which is supported by the current stable public schema API. This tranche deliberately does not claim file streaming, image transforms, native persistence, SharedWorker, or worker-restart coverage. Those topology paths need a reusable app harness and are recorded as follow-up coverage rather than simulated in the UI.

## Checks

```bash
pnpm --dir examples/band-chat test:headless
pnpm --dir examples/band-chat build
```
