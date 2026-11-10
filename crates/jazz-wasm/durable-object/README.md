# Durable Object host

Production WASM build inputs for the packaged Jazz Durable Object host.
Local tests and commands live in the [integration test README](../../../packages/jazz-tools/tests/durable-objects/README.md).

## Worker-hosted createJazz

`jazz-tools/backend/worker` exports `createJazz`, `remote`, `schema` and the
shared authentication helpers. Enable Cloudflare's `nodejs_compat` flag; this
entry uses supported Node crypto and Buffer APIs, but no NAPI, local database,
Node listener or Vercel SDK. Import the schema DSL from this entry as well to
avoid pulling browser-runtime initialization from the general package entry.

Provision the core separately with the Node `durableObjects()` provider. Inside
the application Worker, `remote({ url, appId, secret })` connects to that existing
core. Its authenticated socket upgrade uses Worker `fetch()` and the returned
WebSocket. Cloudflare provisioning credentials are not needed in this Worker.
This implementation uses the core's URL, not a namespace/service binding.

Create and close the client within each invocation. Share schema/configuration,
not a live Jazz client or its sockets across requests:

```ts
import { createJazz, remote } from "jazz-tools/backend/worker";
import { app, permissions } from "./app.js";

export default {
  async fetch(request, env, ctx) {
    const jazz = createJazz({
      app,
      permissions,
      store: remote({
        url: env.JAZZ_CORE_URL,
        appId: env.JAZZ_APP_ID,
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
