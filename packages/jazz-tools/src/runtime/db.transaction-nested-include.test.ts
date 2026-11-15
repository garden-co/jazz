import { expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startLocalJazzServer, startTestJazz, type TestJazz } from "../testing/index.js";

it.each(["inline", "large"])(
  "reads a nested reference inside a transaction without encryption (%s)",
  async (size) => {
    const app = testApp({
      tables: {
        projects: {
          columns: { title: "string" },
          relations: { notesViaProject: { reverse: "notes", relation: "project" } },
        },
        notes: {
          columns: { projectId: "uuid", title: "string" },
          relations: { project: { rel: "projects", column: "projectId" } },
        },
      },
    });
    const server = await startLocalJazzServer({ inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let front: TestJazz | undefined;
    try {
      front = await startTestJazz({
        core: server,
        app,
        permissions: {
          format: 1,
          tables: {
            projects: { read: "true", insert: "true" },
            notes: { read: "true", insert: "true" },
          },
        },
      });
      db = await createDb(await localAccountConfig(server.appId, front.url));
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
      expect(await db.one(query, { tier: "remote" })).toEqual(expected);
      const tx = db.beginTransaction();
      try {
        expect(await tx.one(query, { tier: "local-first" })).toEqual(expected);
      } finally {
        await tx.rollback();
      }
    } finally {
      await db?.shutdown();
      await front?.stop();
      await server.stop();
    }
  },
  60_000,
);

it.each(["local-first", "remote"] as const)(
  "reads a reverse include of a JSON child inside a serializable transaction (%s)",
  async (tier) => {
    const app = testApp({
      tables: {
        parent: {
          columns: { name: "string" },
          relations: { childViaParent: { reverse: "child", relation: "parent" } },
        },
        child: {
          columns: { parentId: "uuid", name: "string", metadata: { json: true } },
          relations: { parent: { rel: "parent", column: "parentId" } },
        },
      },
    });
    const server = await startLocalJazzServer({ inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let front: TestJazz | undefined;
    try {
      front = await startTestJazz({
        core: server,
        app,
        permissions: {
          format: 1,
          tables: {
            parent: { read: "true", insert: "true" },
            child: { read: "true", insert: "true" },
          },
        },
      });
      db = await createDb(await localAccountConfig(server.appId, front.url));
      const parent = await db.insert(app.parent, { name: "Parent" }).wait({ tier: "global" });
      const child = await db
        .insert(app.child, { parentId: parent.id, name: "Child", metadata: { value: 1 } })
        .wait({ tier: "global" });
      const query = app.parent.where({ id: parent.id }).include({ childViaParent: app.child });
      const expected = { ...parent, childViaParent: [child] };
      expect(await db.one(query, { tier: "remote" })).toEqual(expected);
      const write = await db.serializableTransaction((tx) => tx.one(query, { tier }));
      expect(await write.wait()).toEqual(expected);
    } finally {
      await db?.shutdown();
      await front?.stop();
      await server.stop();
    }
  },
  60_000,
);
