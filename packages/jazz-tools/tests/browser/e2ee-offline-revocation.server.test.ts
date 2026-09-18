import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { spaceSchema } from "../../src/e2ee/spaces.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { withTimeout } from "./support.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it.each(["read", "write"] as const)(
  "refuses an encrypted %s after observing revocation and reopening IndexedDB offline",
  async (access) => {
    const server = await getJazzServerInfo(`e2ee-offline-revocation-${crypto.randomUUID()}`);
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
    const store = () => {
      let saved: string | null = null;
      return {
        async read() {
          return saved;
        },
        async update(transform: (current: string | null) => string) {
          saved = transform(saved);
        },
      };
    };
    let owner: Awaited<ReturnType<typeof createDb>> | undefined;
    let reader: Awaited<ReturnType<typeof createDb>> | undefined;
    let stopped = false;
    try {
      await deploy({
        ...server,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...permissions },
      });
      const alice = await acquireBrowserTestAccount({ ...server, key: "owner" });
      const bob = await acquireBrowserTestAccount({ ...server, key: "reader" });
      owner = await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        account: alice,
        driver: { type: "memory" },
        e2ee: { app, store: store() },
      });
      const readerConfig = {
        appId: server.appId,
        serverUrl: server.serverUrl,
        account: bob,
        driver: { type: "persistent" as const, dbName: `e2ee-revoked-${crypto.randomUUID()}` },
        e2ee: { app, store: store() },
      };
      reader = await createDb(readerConfig);
      await owner.e2ee.devices.list();
      await reader.e2ee.devices.list();
      const tx = owner.beginExclusiveTransaction();
      const project = tx.insert(
        app.projects,
        { title: "Shared project" },
        { initialRecipients: [alice.id, bob.id] },
      );
      const note = tx.insert(app.notes, { projectId: project.id, title: "Before revocation" });
      await tx.commit().wait({ tier: "global" });
      expect(
        await owner.e2ee.explain({ scope: app.projects, identifier: project.id }),
      ).toMatchObject({
        state: "ready",
      });
      const query = app.notes.where({ id: note.id });
      expect(await reader.one(query, { tier: "global" })).toEqual(note);
      expect(await reader.one(app.projects.where({ id: project.id }), { tier: "global" })).toEqual(
        project,
      );
      const space = await reader.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "global",
      });
      expect(space).not.toBeNull();
      await owner.e2ee.spaces.revoke(app.projects, project.id, bob.id).wait();
      // Observe acceptance through ordinary metadata, without refreshing E2EE state.
      expect(await reader.all(app.__e2ee_space_grants, { tier: "global" })).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ operation: "remove", spaceId: space!.id, recipientId: bob.id }),
        ]),
      );
      await reader.shutdown();
      reader = undefined;
      await owner.shutdown();
      owner = undefined;
      await stopJazzServer(server.serverUrl);
      stopped = true;
      reader = await createDb(readerConfig);
      await reader.disconnect();
      // Prove ordinary persisted data is still readable: this is not a missing database.
      expect(await reader.one(app.projects.where({ id: project.id }), { tier: "local" })).toEqual(
        project,
      );
      const operation =
        access === "read"
          ? reader.one(query, { tier: "local" })
          : reader
              .update(app.notes, note.id, { title: "Must not be encrypted" })
              .wait({ tier: "local" });
      await expect(
        withTimeout<unknown>(operation, 5_000, "Revoked offline access stalled"),
      ).rejects.toMatchObject({
        name: "E2eeDataError",
        code: "key-unavailable",
      });
    } finally {
      await reader?.shutdown();
      await owner?.shutdown();
      if (!stopped) await stopJazzServer(server.serverUrl);
    }
  },
  60_000,
);
