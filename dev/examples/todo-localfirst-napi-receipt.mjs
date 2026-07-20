#!/usr/bin/env node
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { schema as s } from "../../packages/jazz-tools/dist/index.js";
import { createJazzContext } from "../../packages/jazz-tools/dist/backend/index.js";
import { deploy } from "../../packages/jazz-tools/dist/testing/index.js";

const require = createRequire(import.meta.url);
const { mintLocalFirstToken, verifyLocalFirstIdentityProof } = require("../../crates/jazz-napi");

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
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate
    .whereOld({ owner_id: session.user_id })
    .whereNew({ owner_id: session.user_id });
  policy.todos.allowDelete.where({ owner_id: session.user_id });
});

function createLocalFirstIdentity(actorName) {
  const seed = Buffer.from(actorName.padEnd(32, "-").slice(0, 32)).toString("base64url");
  const token = mintLocalFirstToken(seed, appId, 60);
  const userId = verifyLocalFirstIdentityProof(token, appId).id;
  return { token, userId };
}

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
const context = createJazzContext({
  appId,
  app,
  permissions,
  driver: { type: "persistent", dataPath: join(dataRoot, "runtime.db") },
  serverUrl,
  backendSecret,
  adminSecret,
  env: "dev",
  userBranch: "main",
});

try {
  await deploy({
    appId,
    serverUrl,
    adminSecret,
    schemaDir: "examples/todo-client-localfirst-react",
  });

  const identity = createLocalFirstIdentity("napi-receipt-user");
  const db = await context.forRequest({
    headers: { authorization: `Bearer ${identity.token}` },
  });
  const backend = context.asBackend();
  const title = `napi-receipt-${Date.now()}`;

  const inserted = await db
    .insert(app.todos, {
      title,
      done: false,
      description: "created through jazz-napi",
      owner_id: identity.userId,
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
        userId: identity.userId,
        rowId: inserted.id,
        operations: ["insert", "update", "delete"],
      },
      null,
      2,
    ),
  );
} finally {
  await context.shutdown();
  await rm(dataRoot, { recursive: true, force: true });
}
