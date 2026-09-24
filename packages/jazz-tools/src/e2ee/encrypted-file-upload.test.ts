import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, groupSchema, spaceSchema } from "./managed-schema.js";

it("publishes an encrypted file and metadata that a granted recipient can read", async () => {
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    files: s
      .table(
        { projectId: s.uuid(), payload: s.bytes(), name: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["payload", "name"] }),
  });
  const physical = s.defineApp({
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    files: s.table(
      { projectId: s.uuid(), payload: s.bytes(), name: s.bytes() },
      { project: s.rel("projects", "projectId") },
    ),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    // Public ciphertext reads deliberately prove encryption independently of RLS.
    policy.files.allowRead.always();
    policy.files.allowInsert.where({ "$createdBy.account": session.user.account });
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
  const clients: Db[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const accounts = await Promise.all([
      localAccountConfig(server.appId, server.url),
      localAccountConfig(server.appId, server.url),
      localAccountConfig(server.appId, server.url),
    ]);
    for (const [index, account] of accounts.entries()) {
      let retained: string | null = null;
      clients.push(
        await createDb({
          ...account,
          ...(index < 2
            ? {
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
              }
            : {}),
        }),
      );
    }
    const [alice, bob, observer] = clients;
    const project = alice!.insert(app.projects, { title: "Encrypted attachments" });
    await project.wait({ tier: "global" });
    await alice!.e2ee.spaces.grant(app.projects, project.value.id, accounts[1]!.account.id).wait();
    const payload = Uint8Array.from({ length: 131_091 }, (_, index) => index % 251);
    const inserted = await alice!.insertStreaming(app.files, {
      projectId: project.value.id,
      name: "private-photo.png",
      payload: (async function* () {
        yield payload.subarray(0, 31);
        yield payload.subarray(31, 70_000);
        yield payload.subarray(70_000);
      })(),
    });
    await inserted.wait({ tier: "global" });
    expect(await bob!.one(app.files.where({ id: inserted.value.id }), { tier: "global" })).toEqual({
      id: inserted.value.id,
      projectId: project.value.id,
      name: "private-photo.png",
      payload,
    });
    const stored = await observer!.one(physical.files.where({ id: inserted.value.id }), {
      tier: "global",
    });
    expect(stored).not.toBeNull();
    expect(stored!.payload).not.toEqual(payload);
    expect(new TextDecoder().decode(stored!.name)).not.toContain("private-photo.png");
    await expect(
      observer!.one(app.files.where({ id: inserted.value.id }), { tier: "global" }),
    ).rejects.toThrow();
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);
