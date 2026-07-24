# Backend request scoping

## Create and reuse the context

Create one context in a server-only module:

```ts
import { createJazzContext } from "jazz-tools/backend";
import permissions from "./permissions";
import { app } from "./schema";

export const jazz = createJazzContext({
  appId: process.env.JAZZ_APP_ID!,
  app,
  permissions,
  driver: { type: "persistent", dataPath: "./jazz-data" },
  serverUrl: process.env.JAZZ_SERVER_URL,
  backendSecret: process.env.BACKEND_SECRET,
  env: process.env.NODE_ENV === "production" ? "prod" : "dev",
  userBranch: "main",
});
```

The context initializes lazily and reuses one runtime/client. A context cannot be reused with a
different schema after initialization; create a separate context for another app or schema.

## Select the database role

| API                                 | Permission identity    | Authorship    | Use for                                                   |
| ----------------------------------- | ---------------------- | ------------- | --------------------------------------------------------- |
| `context.asBackend()`               | backend authority      | `jazz:system` | jobs, trusted administration, Better Auth adapter         |
| `await context.forRequest(req)`     | authenticated caller   | caller        | API endpoints whose caller must pass row policies         |
| `context.forSession(session)`       | supplied session       | session user  | cookie or framework auth already resolved by trusted code |
| `context.withAttribution(userId)`   | backend authority      | supplied user | trusted work that should preserve user provenance         |
| `withAttributionForRequest/Session` | backend authority      | resolved user | authenticated provenance without applying user policies   |
| `context.db()`                      | unscoped local runtime | `jazz:system` | embedded or local-only database access                    |

`forRequest` and `forSession` change both permission evaluation and authorship. `withAttribution*`
changes authorship only.

## Request-scoped route

```ts
export async function GET(request: Request) {
  const db = await jazz.forRequest(request);
  const todos = await db.all(app.todos, { tier: "edge" });
  return Response.json(todos);
}
```

Standard Fetch `Request`, Express/Hono-style request objects, and objects exposing ordinary headers
are accepted. The request must carry a bearer token. Configure `jwksUrl` or `jwtPublicKey` on the
context for external tokens; without either, only supported Jazz self-signed tokens are accepted
unless local-first auth is disabled.

## Resolved cookie session

Resolve application cookies before calling Jazz:

```ts
const providerSession = await auth.api.getSession({ headers: request.headers });
const db = jazz.forSession({
  user_id: providerSession.user.jazzId,
  claims: { role: providerSession.user.role },
  authMode: "external",
});
```

Do not pass raw cookie headers to `forRequest` and expect Jazz to know the application's cookie
format.

## Attribution without user policies

Use attribution for trusted processing that must retain backend access:

```ts
const db = await jazz.withAttributionForRequest(request);
await db.insert(app.auditEntries, { action: "exported" }).wait({ tier: "edge" });
```

This write is stamped as the authenticated user but evaluated with backend authority. If the user
must be authorized by row policy, use `forRequest` instead.

## Lifetime and shutdown

- Call `context.flush()` only when the runtime needs an explicit synchronous flush point.
- Call `await context.shutdown()` during application shutdown to release the native client and
  transport.
- In hot-reloading frameworks, preserve the established singleton pattern so development reloads do
  not accumulate native runtimes.
