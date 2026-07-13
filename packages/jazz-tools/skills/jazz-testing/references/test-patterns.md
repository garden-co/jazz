# Jazz test patterns

Use imports from `jazz-tools` and `jazz-tools/testing`. Adapt lifecycle hooks to the project's test
runner.

## Contents

- [Permission policy tests](#permission-policy-tests)
- [Local server and external JWT](#local-server-and-external-jwt)
- [Deploy schema and permissions](#deploy-schema-and-permissions)
- [Cross-client synchronization](#cross-client-synchronization)
- [Offline behavior](#offline-behavior)
- [Migration compatibility](#migration-compatibility)

## Permission policy tests

```ts
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import permissions from "../permissions";
import { app } from "../schema";

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp.shutdown();
});

it("allows owners and denies other users", async () => {
  const alice = testApp.as({
    user_id: "alice",
    claims: {},
    authMode: "local-first",
  });
  const bob = testApp.as({
    user_id: "bob",
    claims: {},
    authMode: "local-first",
  });

  const todo = alice.insert(app.todos, { title: "Private" }).value;

  alice.expectAllowed((db) => db.update(app.todos, todo.id, { title: "Updated" }));
  bob.expectDenied((db) => db.update(app.todos, todo.id, { title: "Denied" }));

  await expect(bob.all(app.todos.where({ id: todo.id }))).resolves.toEqual([]);
});
```

This example assumes `managedByCreator()` permissions, so inserting through `alice` records Alice in
the `$createdBy` magic column. `as(session)` creates a session-scoped test database. `insert` returns
a write handle, and `.value` is its immediate optimistic row value. `expectAllowed` and
`expectDenied` assert writes; query returned rows directly for read policies. Use `testApp.seed(...)`
only for administrative setup that should bypass the actor being tested.

## Local server and external JWT

```ts
import { createDb, type Db } from "jazz-tools";
import {
  startLocalJazzServer,
  startTestJwtIssuer,
  type LocalJazzServerHandle,
  type TestJwtIssuerHandle,
} from "jazz-tools/testing";

let issuer: TestJwtIssuerHandle;
let server: LocalJazzServerHandle;
let db: Db | undefined;

beforeEach(async () => {
  issuer = await startTestJwtIssuer();
  server = await startLocalJazzServer({ inMemory: true, jwksUrl: issuer.jwksUrl });
});

afterEach(async () => {
  await db?.shutdown();
  await server.stop();
  await issuer.stop();
});

it("authenticates a signed user", async () => {
  db = await createDb({
    appId: server.appId,
    serverUrl: server.url,
    jwtToken: issuer.jwtForUser("alice", { role: "admin" }),
    driver: { type: "memory" },
  });

  expect(db.getAuthState().session).toMatchObject({
    user_id: "alice",
    authMode: "external",
  });
});
```

## Deploy schema and permissions

```ts
import { deploy, startLocalJazzServer } from "jazz-tools/testing";
import permissions from "../permissions";
import { app } from "../schema";

const server = await startLocalJazzServer({ inMemory: true });

await deploy({
  serverUrl: server.url,
  appId: server.appId,
  adminSecret: server.adminSecret,
  schema: app,
  permissions,
});
```

Deploy before creating session clients whose reads or writes depend on server-enforced policies.

## Cross-client synchronization

```ts
const alice = await createDb({
  appId: server.appId,
  serverUrl: server.url,
  adminSecret: server.adminSecret,
  driver: { type: "memory" },
});
const bob = await createDb({
  appId: server.appId,
  serverUrl: server.url,
  adminSecret: server.adminSecret,
  driver: { type: "memory" },
});

const created = await alice
  .insert(app.todos, { title: "Synced", done: false })
  .wait({ tier: "edge" });

await vi.waitFor(async () => {
  const rows = await bob.all(app.todos.where({ id: created.id }), { tier: "edge" });
  expect(rows).toHaveLength(1);
});
```

Using the admin secret isolates synchronization behavior from authorization. For permission-aware
sync tests, use signed JWTs that match the app's auth mode and deploy policies before creating the
clients.

## Offline behavior

```ts
await alice.disconnect();

alice.update(app.todos, todoId, { title: "Alice offline" });
await bob.update(app.todos, todoId, { done: true }).wait({ tier: "edge" });

await alice.reconnect();

await vi.waitFor(async () => {
  const row = await alice.one(app.todos.where({ id: todoId }), { tier: "edge" });
  expect(row).toMatchObject({ done: true });
});
```

Assert the merge behavior the schema declares; do not assume every conflicting field uses the same
strategy.

## Migration compatibility

1. Define old and new schemas with the public DSL.
2. Build `oldApp` and `newApp` with `s.defineApp(...)`.
3. Define a migration with `s.defineMigration(...)` and the installed migration operations.
4. Deploy the old app and permissions.
5. Deploy the new app, permissions, and migration.
6. Connect one client with each app surface.
7. Write through each version and assert the other version reads the translated row.

Always shut down both clients and the server in a `finally` block or teardown hook.
