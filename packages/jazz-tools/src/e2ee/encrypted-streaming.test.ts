import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { definePermissions } from "../permissions/index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, groupSchema, spaceSchema } from "./managed-schema.js";

it("streams a plaintext-only update without loading encryption keys", async () => {
  const physical = s.defineApp({
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    files: s.table(
      { projectId: s.uuid(), payload: s.bytes(), notes: s.string() },
      { project: s.rel("projects", "projectId") },
    ),
  });
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    files: s
      .table(
        { projectId: s.uuid(), payload: s.bytes(), notes: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["payload"] }),
  });
  const permissions = definePermissions(physical, ({ policy }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.files.allowRead.always();
    policy.files.allowInsert.always();
    policy.files.allowUpdate.always();
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: physical,
      permissions,
    });
    db = await createDb(await localAccountConfig(server.appId, server.url));
    const project = db.insert(physical.projects, { title: "Public" });
    await project.wait({ tier: "global" });
    // Existing opaque bytes must survive without the caller knowing a key.
    const file = db.insert(physical.files, {
      projectId: project.value.id,
      payload: new Uint8Array([7, 8]),
      notes: "Before",
    });
    await file.wait({ tier: "global" });
    const source = (async function* () {
      yield "After";
    })();
    const update = await db.updateStreaming(app.files, file.value.id, { notes: source });
    await update.wait({ tier: "global" });
    expect(await db.one(physical.files.where({ id: file.value.id }), { tier: "global" })).toEqual({
      ...file.value,
      notes: "After",
    });
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);

it.each(["scope-insert", "scope-upsert", "encrypted-scalar", "space-change"])(
  "rejects streaming bypasses before consuming a source (%s)",
  async (operation) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      files: s
        .table(
          { projectId: s.uuid(), payload: s.bytes(), notes: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["payload"] }),
    });
    const db = await createDb(
      await localAccountConfig(`encrypted-stream-bypass-${crypto.randomUUID()}`),
    );
    let consumed = 0;
    const source = (async function* () {
      consumed++;
      yield new Uint8Array([1, 2, 3]);
    })();
    try {
      const id = crypto.randomUUID();
      const pending =
        operation === "scope-insert"
          ? db.insertStreaming(app.projects, { title: source })
          : operation === "scope-upsert"
            ? db.upsertStreaming(app.projects, id, { title: source })
            : operation === "encrypted-scalar"
              ? db.updateStreaming(app.files, id, { notes: source, payload: new Uint8Array([9]) })
              : db.updateStreaming(app.files, id, {
                  notes: source,
                  projectId: crypto.randomUUID(),
                });
      await expect(pending).rejects.toThrow(
        operation.startsWith("scope-")
          ? "Encryption scope streaming creation is not supported"
          : "Encrypted streaming is not supported",
      );
      expect(consumed).toBe(0);
    } finally {
      await db.shutdown();
    }
  },
);

it.each(["insert", "update", "upsert", "partial"])(
  "rejects unsupported encrypted %s without consuming plaintext",
  async (operation) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      files: s
        .table(
          { projectId: s.uuid(), payload: s.bytes() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["payload"] }),
    });
    const db = await createDb(await localAccountConfig(`encrypted-stream-${crypto.randomUUID()}`));
    let consumed = 0;
    const source = (async function* () {
      consumed++;
      yield new Uint8Array([1, 2, 3]);
    })();
    try {
      const id = crypto.randomUUID();
      const data = { projectId: crypto.randomUUID(), payload: source };
      if (operation === "partial") {
        expect(() =>
          db.update(
            app.files,
            id,
            {},
            {
              applyDiffs: {
                payload: {
                  within: { from: 0, to: 1 },
                  splices: [{ at: 0, delete: 1, insert: new Uint8Array([9]) }],
                },
              },
            },
          ),
        ).toThrow("Encrypted partial updates are not supported");
      } else {
        const pending =
          operation === "insert"
            ? db.insertStreaming(app.files, data)
            : operation === "update"
              ? db.updateStreaming(app.files, id, data)
              : db.upsertStreaming(app.files, id, data);
        await expect(pending).rejects.toThrow("Encrypted streaming is not supported");
      }
      expect(consumed).toBe(0);
    } finally {
      await db.shutdown();
    }
  },
);
