import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema } from "./device-requests.js";

it.each(["app", "slice"] as const)(
  "enrols a device without importing managed tables or policies (%s)",
  async (kind) => {
    const definition = {
      projects: s.table({ title: s.string() }, {}),
      messages: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), body: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["body"] }),
    };
    const app =
      kind === "app"
        ? s.defineApp(definition)
        : s.defineSliceableApp(definition).slice("projects", "messages", "notes");
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.projects.allowRead.where({ "$createdBy.account": session.user.account });
      policy.messages.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.messages.allowRead.where({ "$createdBy.account": session.user.account });
    });
    // Successful enrolment alone does not prove the deployed schema has the
    // package-owned policies protecting private enrolment records.
    expect(permissions).toHaveProperty("__e2ee_device_requests");
    expect(permissions).toHaveProperty("__e2ee_recovery_protectors");
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
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
      const account = await localAccountConfig(server.appId, server.url);
      db = await createDb({
        ...account,
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
      expect(await db.e2ee.devices.list()).toEqual([expect.objectContaining({ state: "active" })]);
      const message = db.insert(app.messages, { title: "Ordinary permissions" });
      await message.wait({ tier: "global" });
      expect(await db.all(app.messages, { tier: "global" })).toEqual([message.value]);
      await expect(db.e2ee.groups.create().wait()).rejects.toThrow(/permission/i);
      await expect(
        db
          .insert(app.projects, { title: "No space administration policy" })
          .wait({ tier: "global" }),
      ).rejects.toThrow(/permission/i);
      await expect(
        db.delete(app.messages, message.value.id).wait({ tier: "global" }),
      ).rejects.toThrow(/permission/i);
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);

it("rejects application overrides of package-owned device permissions", () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
  expect(() =>
    definePermissions(app, ({ policy }) => {
      policy.__e2ee_device_requests.allowRead.always();
    }),
  ).toThrow(/Cannot override package-owned E2EE permissions/);
});

it("does not add managed tables or policies to an ordinary app", () => {
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  expect(Object.keys(app.wasmSchema)).toEqual(["projects"]);
  const permissions = definePermissions(app, ({ policy }) => {
    policy.projects.allowRead.always();
  });
  expect(Object.keys(permissions)).toEqual(["projects"]);
});

it("rejects application tables that replace a managed E2EE schema", () => {
  expect(() =>
    s.defineApp({
      __e2ee_device_requests: s.table({ comment: s.string() }, {}),
      projects: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), body: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["body"] }),
    }),
  ).toThrow(/Cannot replace managed E2EE table/);
});
