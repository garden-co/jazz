import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createBrowserCrypto } from "./browser.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createJazzSession } from "../backend/create-jazz-session.js";

it.each([
  { runtime: "wasm", mode: "plaintext" },
  { runtime: "wasm", mode: "encrypted" },
  { runtime: "native", mode: "plaintext" },
  { runtime: "native", mode: "encrypted" },
])(
  "reads a nested reference with E2EE configured ($runtime, $mode)",
  async ({ runtime, mode }) => {
    const notes = s.table(
      { project: s.uuid(), title: s.string() },
      { projectRelation: s.rel("projects", "project") },
    );
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table(
        { title: s.string() },
        { notesViaProject: s.reverse("notes", "projectRelation") },
      ),
      notes:
        mode === "encrypted" ? notes.encrypted({ space: "project", columns: ["title"] }) : notes,
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      let saved: string | null = null;
      const store = {
        async read() {
          return saved;
        },
        async update(transform: (value: string | null) => string) {
          saved = transform(saved);
        },
      };
      if (runtime === "native") {
        owner = await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions: { ...deviceRequestPermissions, ...policies },
          driver: { type: "memory" },
          initial: "local-first",
          e2ee: { app, store },
        });
        db = owner.getSnapshot().client!.db;
      } else
        db = await createDb({
          ...(await localAccountConfig(server.appId, server.url)),
          e2ee: {
            app,
            crypto: await createBrowserCrypto(),
            store,
          },
        });
      const write = db.beginExclusiveTransaction();
      const project = write.insert(app.projects, { title: "Project" });
      const note = write.insert(app.notes, { project: project.id, title: "Private note" });
      await write.commit().wait({ tier: "global" });
      const query = app.projects.where({ id: project.id }).include({
        notesViaProject: app.notes.include({ projectRelation: true }),
      });
      const expected = { ...project, notesViaProject: [{ ...note, projectRelation: project }] };
      expect(await db.one(query, { tier: "global" })).toEqual(expected);
      const read = db.beginTransaction();
      try {
        expect(await read.one(query, { tier: "local" })).toEqual(expected);
      } finally {
        await read.rollback();
      }
    } finally {
      if (owner) await owner.close();
      else await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
