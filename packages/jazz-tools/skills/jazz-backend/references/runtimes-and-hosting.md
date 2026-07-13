# Backend runtimes and hosting

## Node.js runtime

Install `jazz-napi` directly alongside `jazz-tools`; runtime detection does not replace a declared
dependency.

Choose a driver:

- `{ type: "persistent", dataPath }` stores local server state through the native runtime.
- `{ type: "memory" }` is ephemeral and requires `serverUrl` so an upstream peer can persist data.

Create a persistent context once and shut it down with the process. A context connected to a sync
server normally uses the edge durability tier by default, but endpoint contracts should still wait
explicitly when confirmation matters.

## Secrets and roles

| Credential                        | Purpose                                                                    |
| --------------------------------- | -------------------------------------------------------------------------- |
| End-user JWT or local-first token | caller identity and row-policy evaluation                                  |
| `BACKEND_SECRET`                  | backend-authenticated sync and trusted/request-scoped backend handles      |
| `JAZZ_ADMIN_SECRET`               | schema catalogue, permissions, migrations, and edge-to-core administration |

Only app IDs and server URLs belong in public-prefixed framework variables. Keep backend/admin
secrets in server-only modules and environment variables.

## Self-hosted sync server

Use the installed CLI and inspect `--help` for the exact version. Typical options include:

```bash
pnpm exec jazz-tools server <appId> \
  --port 1625 \
  --data-dir ./data \
  --backend-secret "$BACKEND_SECRET" \
  --admin-secret "$JAZZ_ADMIN_SECRET"
```

Relevant choices:

- `--in-memory` loses server data when the process exits.
- `--jwks-url` or `--jwt-public-key` enables external JWT validation; choose one.
- `--allow-local-first-auth` is required when production policy permits self-signed local-first
  identities.
- `--upstream-url` creates an edge connected to another Jazz server and requires the admin secret.

Cookie-based application auth is not a separate sync-server flag. Resolve cookies in the
application server and exchange them for supported auth or a resolved `Session`.

`startLocalJazzServer(...)` is suitable for development and tests, not a replacement for an
explicit production hosting plan.

## Framework server code

- Put Next.js backend contexts in a `server-only` module and keep `BACKEND_SECRET` unprefixed.
- Use Jazz's Next.js plugin when the project already has it; it configures `jazz-napi` as a server
  external. Do not invent additional package externalization without checking the installed setup.
- Keep SvelteKit contexts in server modules such as hooks or `+server.ts`, not shared client code.
- Reuse one context across route handlers and server renders.
- The framework dev plugin can start a local server and inject public app/server values, but
  production schema and permission deployment remains an explicit external operation.

## Edge-style Wasm runtimes

Node's NAPI runtime is not available in edge runtimes. Use the public `createDb(...)` runtime source
configuration and provide the Wasm bytes or precompiled module supplied by the platform:

```ts
const db = await createDb({
  appId,
  runtimeSources: { wasmModule },
});
```

Do not copy Node-only context or filesystem-driver assumptions into Cloudflare Workers or another
edge runtime. Preserve the platform's module-import and lifecycle conventions, and shut down the
database when the owner is actually terminated if the runtime exposes such a boundary.
