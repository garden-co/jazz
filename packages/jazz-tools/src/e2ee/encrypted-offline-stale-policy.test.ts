import { expect, it, vi } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { app, permissions } from "./fixtures/encrypted-offline-fixture-schema.js";

it.each(["explicit disconnect", "unexpected transport loss"] as const)(
  "applies default warning and explicit rejection to known-stale offline writes (%s)",
  async (offlineCause) => {
    const rotationPolicy = definePermissions(app, ({ policy }) => {
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.never();
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    const store = (initial: string | null = null) => {
      let saved = initial;
      return {
        async read() {
          return saved;
        },
        async update(transform: (current: string | null) => string) {
          saved = transform(saved);
        },
      };
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...permissions, ...rotationPolicy },
      });
      const alice = await localAccountConfig(server.appId, server.url);
      const bob = await localAccountConfig(server.appId, server.url);
      const ownerStore = store();
      const owner = await createDb({ ...alice, e2ee: { app, store: ownerStore } });
      clients.push(owner);
      const departing = await createDb({ ...bob, e2ee: { app, store: store() } });
      clients.push(departing);
      await owner.e2ee.devices.list();
      await departing.e2ee.devices.list();
      // Reopen the enrolled owner on an independent replica with the other policy.
      // Copy opaque store bytes; each replica validates history independently.
      const rejecting = await createDb({
        ...alice,
        e2ee: { app, store: store(await ownerStore.read()), staleWrites: "reject" },
      });
      clients.push(rejecting);
      const owners = [owner, rejecting];
      const tx = owner.beginExclusiveTransaction();
      const project = tx.insert(
        app.projects,
        { title: "Stale offline epoch" },
        {
          initialRecipients: [alice.account.id, bob.account.id],
        },
      );
      const note = tx.insert(app.notes, { projectId: project.id, title: "Before removal" });
      await tx.commit().wait({ tier: "global" });
      for (const client of owners)
        expect(await client.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(
          note,
        );
      // The departing member can record removal but cannot rotate for those left behind.
      await departing.e2ee.spaces.revoke(app.projects, project.id, bob.account.id).wait();
      for (const client of owners) {
        expect(await client.all(app.__e2ee_space_grants, { tier: "global" })).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ operation: "remove", recipientId: bob.account.id }),
          ]),
        );
        // Capture complete accepted membership on each replica before going offline.
        // Rotation is forbidden; a changed sibling snapshot alone cannot authorise reuse.
        expect(await client.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
          state: "maintenance-required",
          reason: "recipient-removed",
        });
      }
      if (offlineCause === "explicit disconnect") {
        for (const client of owners) await client.disconnect();
      } else await server.stop();
      warning.mockClear();
      await expect(
        rejecting.update(app.notes, note.id, { title: "Must be rejected" }).wait({ tier: "local" }),
      ).rejects.toMatchObject({ name: "E2eeDataError", code: "maintenance-required" });
      expect(warning).not.toHaveBeenCalled();
      await owner
        .update(app.notes, note.id, { title: "Written with the retained epoch" })
        .wait({ tier: "local" });
      expect(warning).toHaveBeenCalledWith(
        "E2EE: writing offline with a known-stale encryption epoch; removed recipients may still decrypt this write.",
      );
    } finally {
      warning.mockRestore();
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);
