import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import type { Db } from "../runtime/db.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createBrowserKeyEnvelope } from "./browser.js";
import type { KeyEnvelope } from "./types.js";

it.each(["insert", "upsert"] as const)(
  "rejects unsupported branch first-use before preparing encryption (%s)",
  async (operation) => {
    const before = { projects: s.table({ title: s.string() }, {}) };
    const after = {
      ...before,
      notes: s
        .table(
          { projectId: s.uuid(), body: s.string(), branch: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["body"] })
        .branchBy("branch"),
      rootNotes: s
        .table(
          { projectId: s.uuid(), body: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["body"] }),
    };
    const oldApp = s.defineApp(before);
    const app = s.defineApp(after);
    const migration = s.defineMigration({
      from: before,
      to: after,
      createTables: { notes: true, rootNotes: true },
    });
    const oldPermissions = definePermissions(oldApp, ({ policy }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.notes.allowUpdate.always();
      policy.rootNotes.allowRead.always();
      policy.rootNotes.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let creator: Db | undefined;
    let writer: Db | undefined;
    let saved: string | null = null;
    try {
      const target = {
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
      };
      await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
      creator = await createDb(await localAccountConfig(server.appId, server.url));
      const project = await creator
        .insert(oldApp.projects, { title: "Legacy branch scope" })
        .wait({ tier: "global" });
      await creator.shutdown();
      creator = undefined;
      await deploy({ ...target, schema: app, permissions, migration });
      const account = await localAccountConfig(server.appId, server.url);
      const keys = await createBrowserKeyEnvelope();
      let preparedEnvelopes = 0;
      const keyEnvelope: KeyEnvelope = {
        ...keys,
        async wrap(secret, context, plaintext) {
          preparedEnvelopes += 1;
          return keys.wrap(secret, context, plaintext);
        },
      };
      writer = await createDb({
        ...account,
        e2ee: {
          app,
          crypto: { keyEnvelope },
          store: {
            async read() {
              return saved;
            },
            async update(transform: (current: string | null) => string) {
              saved = transform(saved);
            },
          },
        },
      });
      // Complete account/device enrollment before observing space-key preparation.
      await writer.e2ee.devices.list();
      preparedEnvelopes = 0;
      const branch = operation === "insert" ? "draft" : "";
      const data = { projectId: project.id, body: "Rejected branch secret", branch };
      let rejectedId: string;
      let completed: Promise<unknown>;
      if (operation === "insert") {
        const selector = { branch };
        const options: { branch?: { branch: string } } = { branch: selector };
        const handle = writer.insert(app.notes, data, options);
        rejectedId = handle.value.id;
        // Neither mutation of the nested selector nor removal of the target may
        // turn this already-created branch request into a root-target retry.
        selector.branch = "changed";
        options.branch = undefined;
        completed = handle.wait({ tier: "global" });
      } else {
        rejectedId = crypto.randomUUID();
        const handle = writer.upsert(app.notes, rejectedId, data, { branch });
        completed = handle.wait({ tier: "global" });
      }
      const failure = await completed.then(
        () => undefined,
        (error: unknown) => error,
      );
      expect.soft(failure).toBeInstanceOf(Error);
      // Rollback alone cannot prove this: provisional envelopes must never be prepared.
      expect.soft(preparedEnvelopes).toBe(0);
      expect(await writer.one(app.projects.where({ id: project.id }), { tier: "global" })).toEqual(
        project,
      );
      expect(await writer.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
      expect(await writer.all(app.__e2ee_space_grants, { tier: "global" })).toEqual([]);
      expect(await writer.all(app.__e2ee_space_deliveries, { tier: "global" })).toEqual([]);
      expect(await writer.all(app.notes.select("id"), { tier: "global" })).toEqual([]);
      expect(await writer.all(app.rootNotes.select("id"), { tier: "global" })).toEqual([]);
      expect(await writer.all(app.notes.select("id"), { branch, tier: "global" })).toEqual([]);
      if (operation === "insert")
        expect(
          await writer.all(app.notes.select("id"), { branch: "changed", tier: "global" }),
        ).toEqual([]);

      // Reuse this client and scope: a rejected branch must not retain a provisional key.
      // Use a root-capable table sharing the space, not a root row in a branched table.
      const recovered = await writer
        .insert(app.rootNotes, { projectId: project.id, body: "Root recovery" })
        .wait({ tier: "global" });
      expect(
        await writer.one(app.rootNotes.where({ id: recovered.id }), { tier: "global" }),
      ).toEqual({
        id: recovered.id,
        projectId: project.id,
        body: "Root recovery",
      });
      const roots = await writer.all(app.__e2ee_spaces, { tier: "global" });
      expect(roots).toHaveLength(1);
      expect(roots[0]).toMatchObject({ identifier: project.id, accountId: account.account.id });
      const grants = await writer.all(app.__e2ee_space_grants, { tier: "global" });
      expect(grants).toHaveLength(1);
      expect(grants[0]).toMatchObject({
        spaceId: roots[0]!.id,
        operation: "add",
        recipientId: account.account.id,
      });
      expect(
        await writer.all(app.notes.where({ id: rejectedId }).select("id"), {
          branch,
          tier: "global",
        }),
      ).toEqual([]);
    } finally {
      await writer?.shutdown();
      await creator?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
