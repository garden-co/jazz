# Jazz on Rivet: shared todos

A browser todo app using `createJazz({ store: rivet(...) })`. The TypeScript
backend authenticates local-first guests and publishes the schema and row
permissions. Rivet hosts the Jazz core database in an actor, using its unmodified
async SQLite worker. Browser storage remains local-first.

```text
Browser → Vite /jazz backend → Rivet gateway → Jazz actor → Rivet SQLite / WarpDrive
```

The shared list is readable by everyone. Each guest can modify only their own
tasks. Authentication is configured in `jazz.server.ts`; policies live in
`permissions.ts`. Rivet credentials and the core secret stay on the server.

## Run against an existing Rivet engine

Requirements: Node 22.12+, pnpm, Rustup, and a Rivet 3.0+ engine with an existing
`jazz` runner pool and an API token. The actor SDK is pinned to 3.0.7; the Rust
crate selects Rust 1.99.1. This example does not install or start the Rivet engine.

From the repository root, install dependencies and build the SDK:

```sh
pnpm install
pnpm exec turbo run build --filter=jazz-tools
cd examples/rivet-todos
cp .env.example .env.local
```

Set the engine endpoint, token, namespace and pool in `.env.local`. Generate
`JAZZ_CORE_SECRET` with the command shown there. Both the actor process and the
TS backend use that same secret. Use a distinct `JAZZ_RIVET_DATABASE` for each
independent database; keep the key stable across restarts.

In two terminals, from this directory:

```sh
pnpm actor
```

```sh
pnpm dev
```

Open the URL Vite prints. Starting the backend calls `jazz.ready()`, which finds
or creates the keyed actor, wakes it, and publishes the schema and permissions.
The browser connects to `/jazz` on the same origin.

Add a task, then restart the actor process and refresh the page. For a persistence
check that cannot be satisfied from the first browser's local cache, open the app
in a separate browser profile: it should receive the shared task from the actor.
That second guest can read the task but cannot modify it. Keep the engine's
storage intact between runs.

Active WebSockets keep the actor awake. After clients disconnect, the host asks
Rivet to sleep an actor idle for five minutes. Admitted writes drain before
storage closes. There is no WebSocket hibernation.

## Let createJazz provision a Cloud runner pool

Build the actor image from the repository root (Docker must be running):

```sh
sh crates/jazz-rivet/build-image.sh
```

Set `RIVET_RUNTIME=managed`, `JAZZ_RIVET_IMAGE_JSON` to the generated absolute
`image.json` path, and the Cloud endpoint, token, cluster and region values in
`.env.local`. `RIVET_POOL` becomes the managed pool prefix. Run `pnpm dev` without
`pnpm actor`: `createJazz` verifies and uploads the archive, provisions the pool,
then opens the keyed database actor. This creates Cloud resources. The archive
must be accessible to the backend on the first start; an existing matching pool
can be reused without it. No prebuilt image is published by this repository.

## Check and build

```sh
pnpm typecheck
pnpm build
```

`pnpm preview` serves the built browser app with the same Rivet backend and reads
`.env.local`. Neither build nor typecheck needs Rivet credentials. Rust adapter
and full-server tests live in `crates/jazz-rivet`; they use the real Rivet SQLite
worker with a deterministic page service, not a live WarpDrive cluster.
