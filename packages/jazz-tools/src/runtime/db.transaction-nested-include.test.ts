import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { definePermissions } from "../permissions/index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it.each(["inline", "large"])(
  "reads a nested reference inside a transaction without encryption (%s)",
  async (size) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, { notesViaProject: s.reverse("notes", "project") }),
      notes: s.table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      ),
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: definePermissions(app, ({ policy }) => {
          policy.projects.allowRead.always();
          policy.projects.allowInsert.always();
          policy.notes.allowRead.always();
          policy.notes.allowInsert.always();
        }),
      });
      db = await createDb(await localAccountConfig(server.appId, server.url));
      const project = db.insert(app.projects, { title: "Project" });
      await project.wait({ tier: "global" });
      const note = db.insert(app.notes, {
        projectId: project.value.id,
        title: size === "large" ? "Note".repeat(32_768) : "Note",
      });
      await note.wait({ tier: "global" });
      const query = app.projects.where({ id: project.value.id }).include({
        notesViaProject: app.notes.include({ project: true }),
      });
      const expected = {
        ...project.value,
        notesViaProject: [{ ...note.value, project: project.value }],
      };
      expect(await db.one(query, { tier: "global" })).toEqual(expected);
      const tx = db.beginTransaction();
      try {
        expect(await tx.one(query, { tier: "local" })).toEqual(expected);
      } finally {
        await tx.rollback();
      }
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
