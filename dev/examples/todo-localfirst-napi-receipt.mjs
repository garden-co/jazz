#!/usr/bin/env node
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { schema as s, createAccountManager } from "../../packages/jazz-tools/dist/index.js";
import { createJazzSession } from "../../packages/jazz-tools/dist/backend/index.js";
import { deploy } from "../../packages/jazz-tools/dist/testing/index.js";

const appId = process.env.JAZZ_TODO_APP_ID ?? "00000000-0000-0000-0000-000000000420";
const serverUrl = process.env.JAZZ_TODO_SERVER_URL ?? "http://127.0.0.1:4200";
const adminSecret = process.env.JAZZ_TODO_ADMIN_SECRET ?? "todo-localfirst-admin";
const backendSecret = process.env.JAZZ_TODO_BACKEND_SECRET ?? adminSecret;

const app = s.defineApp({
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    description: s.string().optional(),
    owner_id: s.string(),
    parentId: s.ref("todos").optional(),
    projectId: s.ref("projects").optional(),
  }),
});

const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.where({ owner_id: session.user.account });
  policy.todos.allowUpdate
    .whereOld({ owner_id: session.user.account })
    .whereNew({ owner_id: session.user.account });
  policy.todos.allowDelete.where({ owner_id: session.user.account });
});

async function waitFor(check, label, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      const value = await check();
      if (value) return value;
    } catch (error) {
      last = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`${label} timed out${last ? `: ${last.message}` : ""}`);
}

const dataRoot = await mkdtemp(join(tmpdir(), "jazz-todo-napi-receipt-"));
const session = await createJazzSession({
  appId,
  app,
  permissions,
  driver: { type: "persistent", dataPath: join(dataRoot, "runtime.db") },
  serverUrl,
  initial: { backendSecret },
  env: "dev",
});

try {
  await deploy({
    appId,
    serverUrl,
    adminSecret,
    schema: app,
    permissions,
  });

  let stored = null;
  const accounts = await createAccountManager({
    appId,
    serverUrl,
    env: "dev",
    store: {
      async read() {
        return stored;
      },
      async update(transform) {
        stored = transform(stored);
      },
    },
  });
  const account = accounts.createLocalFirst();
  const snapshot = session.getSnapshot();
  if (snapshot.status !== "ready" || !snapshot.client) {
    throw snapshot.error ?? new Error("Backend session is not ready");
  }
  const db = await snapshot.client.forAccount(account);
  const backend = snapshot.client.db;
  const title = `napi-receipt-${Date.now()}`;

  const inserted = await db
    .insert(app.todos, {
      title,
      done: false,
      description: "created through jazz-napi",
      owner_id: account.id,
    })
    .wait({ tier: "edge" });

  await waitFor(
    async () => backend.one(app.todos.where({ id: inserted.id }), { tier: "edge" }),
    "backend read after insert",
  );

  await db.update(app.todos, inserted.id, { done: true }).wait({ tier: "edge" });
  await waitFor(async () => {
    const row = await backend.one(app.todos.where({ id: inserted.id }), { tier: "edge" });
    return row?.done === true ? row : null;
  }, "backend read after update");

  await db.delete(app.todos, inserted.id).wait({ tier: "edge" });
  await waitFor(async () => {
    const row = await backend.one(app.todos.where({ id: inserted.id }), { tier: "edge" });
    return row === null ? true : null;
  }, "backend read after delete");

  console.log(
    JSON.stringify(
      {
        ok: true,
        serverUrl,
        appId,
        userId: account.id,
        rowId: inserted.id,
        operations: ["insert", "update", "delete"],
      },
      null,
      2,
    ),
  );
} finally {
  await session.close();
  await rm(dataRoot, { recursive: true, force: true });
}
