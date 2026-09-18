import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { spaceSchema } from "../../src/e2ee/spaces.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("updates encrypted data immediately offline and accepts it after reconnect in Chromium", async () => {
  const server = await getJazzServerInfo(`e2ee-offline-${crypto.randomUUID()}`);
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"] }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let saved: string | null = null;
  try {
    await deploy({
      ...server,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...permissions },
    });
    db = await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: await acquireBrowserTestAccount(server),
      driver: { type: "memory" },
      e2ee: {
        app,
        store: {
          async read() {
            return saved;
          },
          async update(transform) {
            saved = transform(saved);
          },
        },
      },
    });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Offline project" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Original" });
    await tx.commit().wait({ tier: "global" });
    // No warming read between acceptance and disconnection.
    await db.disconnect();
    const write = db.update(app.notes, note.id, { title: "Written offline" });
    expect(write).not.toBeInstanceOf(Promise);
    expect(
      await Promise.race([
        write.wait({ tier: "local" }).then(() => "stored locally"),
        new Promise<string>((resolve) => {
          timer = setTimeout(() => resolve("still waiting"), 5_000);
        }),
      ]),
    ).toBe("stored locally");
    clearTimeout(timer);
    const query = app.notes.where({ id: note.id });
    expect(await db.one(query, { tier: "local" })).toEqual({ ...note, title: "Written offline" });
    await db.reconnect();
    await write.wait({ tier: "global" });
    expect(await db.one(query, { tier: "global" })).toEqual({ ...note, title: "Written offline" });
  } finally {
    clearTimeout(timer);
    await db?.shutdown();
    await stopJazzServer(server.serverUrl);
  }
}, 60_000);
