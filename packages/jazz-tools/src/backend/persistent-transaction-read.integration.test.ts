import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzSession } from "./create-jazz-session.js";

it("opens a persistent transaction before its first read, without E2EE", async () => {
  const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
  const permissions = definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
  });
  const directory = await mkdtemp(join(tmpdir(), "jazz-persistent-transaction-"));
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    owner = await createJazzSession({
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(directory, "database") },
      initial: "local-first",
    });
    const db = owner.getSnapshot().client!.db;
    expect(await db.all(app.notes, { tier: "global" })).toEqual([]);
    const tx = db.beginExclusiveTransaction();
    expect(await tx.all(app.notes, { tier: "local" })).toEqual([]);
    await tx.commit().wait({ tier: "global" });
  } finally {
    await owner?.close();
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
}, 30_000);
