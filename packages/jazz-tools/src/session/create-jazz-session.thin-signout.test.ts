import { afterEach, expect, it } from "vitest";
import { memoryPrivateStore } from "../encryption/index.js";
import type { Db } from "../runtime/db.js";
import { DbReplacedError } from "../runtime/db-replaced-error.js";
import { DbClosedError } from "../runtime/graceful-shutdown-error.js";
import {
  cleanUpTestApp,
  memoryTestAccountStore as memoryStore,
  testApp,
} from "../testing/index.js";
import { openBrowserSessionCore, type BrowserJazzSession } from "./create-jazz-session.js";
import { gatedLoader, serveThinTestApp } from "../../tests/fixtures/thin-session.js";

// What a write waiting for the local-first client gets when the session signs
// out, through the real sign-out: the client shuts down first, and only then
// is its Db marked replaced.

const app = testApp({
  format: 1,
  tables: {
    projects: { columns: { name: "string", ownerId: "uuid" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes", __e2ee_eq_title: "bytes" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: {
        space: "projectId",
        scope: "projects",
        columns: { title: "string" },
        equality: ["title"],
      },
    },
  },
});
const permissions = {
  $schema: "https://jazz.tools/schemas/jazz.permissions.v1.json",
  tables: {
    projects: {
      read: "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
      insert: "$createdBy.account = session.user",
    },
    tasks: { read: "true", insert: "true" },
  },
} as never;

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  await cleanUpTestApp(cleanups);
});

const serve = () => serveThinTestApp(app, permissions, cleanups);

it("ends a write waiting for the local-first client with DbClosedError at the real sign-out, and later calls get DbReplacedError", async () => {
  const host = await serve();
  const gate = gatedLoader();
  const session: BrowserJazzSession = await openBrowserSessionCore({
    scope: host.jazz.appId,
    serverUrl: host.serverUrl,
    store: memoryStore(),
    driver: { type: "memory" },
    bearer: () => "token-alice",
    encryption: { app, store: memoryPrivateStore() },
    client: gate.loader,
  } as never);
  cleanups.push(() => session.close());
  const kept: Db = session.getSnapshot().client!.db;

  const write = kept.insert(app.tasks, { projectId: crypto.randomUUID(), title: "x" } as never);
  const written = write.wait({ tier: "global" }).then(
    () => undefined,
    (error: unknown) => error,
  );
  await expect.poll(() => gate.calls, { timeout: 10_000 }).toBe(1);

  await session.logout();

  // The client shuts down before the session marks its Db replaced, so the wait ends with the shutdown's error.
  const error = await written;
  expect(error).toBeInstanceOf(DbClosedError);
  expect(error).not.toBeInstanceOf(DbReplacedError);
  // Once the sign-out is done, the Db reports that it was replaced.
  await expect(kept.encryption.deviceId()).rejects.toBeInstanceOf(DbReplacedError);
  gate.release();
}, 60_000);
