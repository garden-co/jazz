# Durable Object host

Production WASM build inputs for the packaged Jazz Durable Object host.
Local tests and commands live in the [integration test README](../../../packages/jazz-tools/tests/durable-objects/README.md).

## Worker-hosted createJazz

`jazz-tools/backend` exports `createJazz`, `durableObjects`, `remote`, `schema` and the
shared authentication helpers. Enable Cloudflare's `nodejs_compat` flag; this
entry uses supported Node crypto and Buffer APIs, but no NAPI, local database,
Node listener or Vercel SDK. Import the schema DSL from this entry as well to
avoid pulling browser-runtime initialization from the general package entry.

The package's `workerd` condition selects internal socket and lifecycle
adapters; Node selects its native adapters by default. Wrangler resolves this
automatically. With another bundler, enable the `workerd` condition. Set TypeScript
`compilerOptions.customConditions` to `["workerd"]` in the Worker application to
select matching types. Node-only `attach()` and `nodeHandler()` are absent there.
The previous `jazz-tools/backend/worker` entry is replaced by this common import.

`durableObjects({ name, credentials, secret })` provisions or reuses the core
from the application Worker, using the same provisioning checks as Node.
Pass credentials from Worker bindings; the API token deploys the host, while
the separate stable core secret authenticates database requests and sockets.
The application Worker itself is deployed by your normal application tooling.

Both Node and Workers load the same generated `artifact.js`, containing the
compressed Worker/WASM bytes and version manifest. The provider verifies
its hashes and package version, and uses Worker `fetch()` for HTTP and socket
upgrades. This increases application bundle size; `remote({ url, appId, secret })`
remains available when provisioning happens elsewhere and does not need those
host bytes or account credentials. Connections use the core's URL, not a
namespace/service binding.

Create and close the client within each invocation. Share schema/configuration,
not a live Jazz client or its sockets across requests:

```ts
import { createJazz, durableObjects } from "jazz-tools/backend";
import { app, permissions } from "./app.js";

export default {
  async fetch(request, env, ctx) {
    const jazz = createJazz({
      app,
      permissions,
      store: durableObjects({
        name: "my-jazz-core",
        credentials: {
          accountId: env.CLOUDFLARE_ACCOUNT_ID,
          apiToken: env.CLOUDFLARE_API_TOKEN,
        },
        secret: env.JAZZ_CORE_SECRET,
      }),
      // Supply your application's authentication callback here.
    });
    let upgraded = false;
    try {
      // Application handlers can also await jazz.db.all(...), forReqUser(...), etc.
      const response = await jazz.handle(request, ctx);
      upgraded = response.status === 101;
      return response;
    } finally {
      if (!upgraded) await jazz.close();
    }
  },
};
```

`handle(request, ctx)` adapts incoming WebSocketPair events into the existing
Jazz admission/protocol path. A successful upgrade retains its client until
socket close/error and schedules cleanup with `ctx.waitUntil`. Ordinary HTTP
requests and custom database handlers await their work and close before returning.
The HTTP-stream fallback needs state across requests and is explicitly refused
(501); WebSocket capability probes do not advertise it. Backend transactions
and durable large values retain their existing limitations.
Worker text-anchor helpers are unsupported. Node attachment APIs remain available
only from the Node entry.

Publication, authentication, account routes, user scopes and thin request
handling are shared with Node. Node retains local native validation; the Worker
relies on the authority's atomic schema/migration/permission validation during
publication. It does not run a second schema validator or database engine.
No durable or wire encoding changes are introduced by this host extraction.

Worker integration coverage runs through `pnpm --filter jazz-tools test:durable-objects`.
These local checks do not establish deployed application-Worker behavior or performance.

## Release packaging

The release WASM job runs the optimized, locked DO build and uploads its two
generated files. Package assembly includes them in `pkg-jazz-tools`; publishing
reuses that artifact, including when promoting a release preview. Release
candidates pass their version into the build before the manifest is generated.

`prepack` only verifies presence, package version and decompressed content hashes.
For local packaging, run `pnpm --filter jazz-tools build:durable-objects` first.
Release and candidate packaging run `dev/artifacts/verify-packed-durable-object.mjs`
on the npm tarball: its embedded host must initialize in local workerd and serve
`createJazz` readiness and a read/write. No Cloudflare deployment is involved.
