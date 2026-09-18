import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { createDb } from "../runtime/default-create-db.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("resolves the same accepted scope through WASM and native Node clients", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  const permissions = definePermissions(app, ({ policy }) => {
    policy.projects.allowRead.always();
  });
  let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  let wasm: Awaited<ReturnType<typeof createDb>> | undefined;
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
      driver: { type: "memory" },
      initial: "local-first",
    });
    const native = owner.getSnapshot().client!.db;
    wasm = await createDb({
      appId: server.appId,
      serverUrl: server.url,
      account: owner.getSnapshot().account!,
      driver: { type: "memory" },
    });
    await wasm.all(app.projects, { tier: "edge" });
    await native.all(app.projects, { tier: "edge" });
    const expected = await wasm.tableIdentity(app.projects);
    expect(expected).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(await native.tableIdentity(app.projects)).toBe(expected);
    await wasm.shutdown();
    await expect(wasm.tableIdentity(app.projects)).rejects.toThrow();
  } finally {
    await wasm?.shutdown();
    await owner?.close();
    await server.stop();
  }
}, 30_000);
