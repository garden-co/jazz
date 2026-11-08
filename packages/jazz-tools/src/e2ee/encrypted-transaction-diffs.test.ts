import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { definePermissions } from "../permissions/index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { frontE2eeCore } from "./fixtures/app.js";

it.each(["payload", "space"] as const)(
  "refuses a transaction %s replacement combined with plaintext diffs before staging",
  async (replacement) => {
    const app = s.defineApp(
      {
        projects: s.table({ title: s.string() }, {}),
        files: s
          .table(
            { projectId: s.uuid(), payload: s.bytes(), notes: s.string() },
            { project: s.rel("projects", "projectId") },
          )
          .encrypted({ space: "projectId", columns: ["payload"] }),
      },
      { migrations: "none" },
    );
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.files.allowRead.always();
      policy.files.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.files.allowUpdate.where({ "$createdBy.account": session.user.account });
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    });
    let server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let retained: string | null = null;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      server = await frontE2eeCore(server, { schema: app, permissions });
      db = await createDb({
        ...(await localAccountConfig(server.appId, server.url)),
        e2ee: {
          app,
          store: {
            async read() {
              return retained;
            },
            async update(transform: (current: string | null) => string) {
              retained = transform(retained);
            },
          },
        },
      });
      const project = db.insert(app.projects, { title: "Scope" });
      await project.wait({ tier: "global" });
      const file = db.insert(app.files, {
        projectId: project.value.id,
        payload: new Uint8Array([7, 8]),
        notes: "Before",
      });
      await file.wait({ tier: "global" });
      const tx = db.beginSerializableTransaction();
      const diffs = {
        notes: { within: { from: 0, to: 6 }, splices: [{ at: 0, delete: 6, insert: "After" }] },
      };
      try {
        const data =
          replacement === "payload"
            ? { payload: new Uint8Array([9]) }
            : { projectId: project.value.id };
        expect(() => tx.update(app.files, file.value.id, data, { applyDiffs: diffs })).toThrow(
          "Encrypted updates do not support applyDiffs",
        );
        expect(await tx.all(app.files, { tier: "local-first" })).toEqual([file.value]);
        // A plaintext-only splice remains supported in the very same transaction.
        tx.update(app.files, file.value.id, {}, { applyDiffs: diffs });
        await tx.commit().wait({ tier: "global" });
      } catch (error) {
        await tx.rollback();
        throw error;
      }
      expect(await db.all(app.files, { tier: "remote" })).toEqual([
        { ...file.value, notes: "After" },
      ]);
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
