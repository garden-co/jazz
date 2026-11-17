import { expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";

it("matches reference filters inside a transaction as it does outside", async () => {
  const app = testApp({
    tables: {
      projects: { columns: { title: "string" } },
      notes: {
        columns: { projectId: "uuid", projectUuid: "uuid" },
        relations: { project: { rel: "projects", column: "projectId" } },
      },
    },
  });
  const db = await createDb(await localAccountConfig(`transaction-ref-${crypto.randomUUID()}`));
  try {
    const projectWrite = db.insert(app.projects, { title: "Project" });
    await projectWrite.wait({ tier: "local" });
    const project = projectWrite.value;
    const note = db.insert(app.notes, { projectId: project.id, projectUuid: project.id });
    await note.wait({ tier: "local" });
    expect(
      await db.all(app.notes.where({ projectId: project.id }), { tier: "local-first" }),
    ).toEqual([note.value]);
    const tx = db.beginSerializableTransaction();
    try {
      expect(
        await tx.all(app.notes.where({ projectUuid: project.id }), { tier: "local-first" }),
      ).toEqual([note.value]);
      expect(
        await tx.all(app.notes.where({ projectId: project.id }), { tier: "local-first" }),
      ).toEqual([note.value]);
    } finally {
      await tx.rollback();
    }
  } finally {
    await db.shutdown();
  }
});
