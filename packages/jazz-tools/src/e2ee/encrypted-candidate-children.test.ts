import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserCrypto } from "./browser.js";
import { encodeEnvelope } from "./envelope.js";

it.each(["one-shot", "transaction", "subscription"])(
  "rejects nonmatching candidates before decrypting their included children (%s)",
  async (readKind) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), title: s.string() },
          {
            project: s.rel("projects", "projectId"),
            commentsViaNote: s.reverse("comments", "noteRelation"),
          },
        )
        .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
      comments: s
        .table(
          { projectId: s.uuid(), note: s.uuid(), body: s.string() },
          { project: s.rel("projects", "projectId"), noteRelation: s.rel("notes", "note") },
        )
        .encrypted({ space: "projectId", columns: ["body"] }),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.comments.allowRead.always();
      policy.comments.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const store = () => {
      let value: string | null = null;
      return {
        async read() {
          return value;
        },
        async update(transform: (current: string | null) => string) {
          value = transform(value);
        },
      };
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      const crypto = await createBrowserCrypto();
      const collisionCrypto = {
        ...crypto,
        equalityIndex: {
          mechanism: crypto.equalityIndex!.mechanism,
          async token() {
            return encodeEnvelope(crypto.equalityIndex!.mechanism, new Uint8Array(32));
          },
        },
      };
      const alice = await localAccountConfig(server.appId, server.url);
      const bob = await localAccountConfig(server.appId, server.url);
      const owner = await createDb({
        ...alice,
        e2ee: { app, store: store(), crypto: collisionCrypto },
      });
      clients.push(owner);
      const reader = await createDb({
        ...bob,
        e2ee: { app, store: store(), crypto: collisionCrypto },
      });
      clients.push(reader);
      await owner.e2ee.devices.list();
      await reader.e2ee.devices.list();
      const tx = owner.beginExclusiveTransaction();
      const shared = tx.insert(app.projects, { title: "Shared" });
      const privateProject = tx.insert(app.projects, { title: "Private" });
      const match = tx.insert(app.notes, { projectId: shared.id, title: "Wanted" });
      const nonmatch = tx.insert(app.notes, { projectId: shared.id, title: "Other" });
      const visible = tx.insert(app.comments, {
        projectId: shared.id,
        note: match.id,
        body: "Readable",
      });
      const hidden = tx.insert(app.comments, {
        projectId: privateProject.id,
        note: nonmatch.id,
        body: "Not shared",
      });
      await tx.commit().wait({ tier: "global" });
      await owner.e2ee.spaces.grant(app.projects, shared.id, bob.account.id).wait();
      // Both parent ciphertexts are readable; only the excluded child's space is unavailable.
      expect(
        await reader.all(app.notes.where({ projectId: shared.id }), { tier: "global" }),
      ).toEqual(expect.arrayContaining([match, nonmatch]));
      await expect(
        reader.one(app.comments.where({ id: hidden.id }), { tier: "global" }),
      ).rejects.toMatchObject({ name: "E2eeDataError", code: "key-not-shared" });
      // The transaction reads locally, so first bring its readable child into the replica.
      expect(await reader.one(app.comments.where({ id: visible.id }), { tier: "global" })).toEqual(
        visible,
      );
      const query = app.notes
        .where({ projectId: shared.id, title: "Wanted" })
        .include({ commentsViaNote: true });
      const expected = [{ ...match, commentsViaNote: [visible] }];
      if (readKind === "one-shot") {
        expect(await reader.all(query, { tier: "global" })).toEqual(expected);
      } else if (readKind === "transaction") {
        const read = reader.beginTransaction();
        try {
          expect(await read.all(query, { tier: "local" })).toEqual(expected);
        } finally {
          await read.rollback();
        }
      } else {
        let result: unknown[] | undefined;
        let failure: Error | undefined;
        const stop = reader.subscribe(
          query,
          {
            onUpdate: (rows) => {
              result = rows;
            },
            onError: (error) => {
              failure = error;
            },
          },
          { tier: "global" },
        );
        try {
          await expect.poll(() => failure ?? result, { timeout: 30_000 }).toEqual(expected);
        } finally {
          stop();
        }
      }
    } finally {
      for (const client of clients) await client.shutdown();
      await server.stop();
    }
  },
  120_000,
);
