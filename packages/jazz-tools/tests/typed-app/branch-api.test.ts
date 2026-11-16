import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import { afterEach, describe, expect, it } from "vitest";
import { createDb } from "../../src/runtime/default-create-db.js";
import type { Db } from "../../src/runtime/db.js";
import { parseSnapshotRef, serializeSnapshotRef } from "../../src/runtime/snapshot-ref.js";

import { app } from "./apps/branch-documents/jazz.generated.js";
import { app as referenceApp } from "./apps/branch-scenarios/jazz.generated.js";

describe("branch API", () => {
  let db: Db | undefined;

  afterEach(async () => {
    await db?.shutdown();
  });

  it("uses scalar selectors for a single string branch column", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-string-shorthand")),
      driver: { type: "memory" },
    });

    const mainDocument = db.insert(
      app.documents,
      { branch: "main", title: "Main title" },
      { branch: "main" },
    ).value;

    expect(await db.all(app.documents, { branch: "draft", base: "main" })).toEqual([
      { ...mainDocument, branch: "draft" },
    ]);

    db.update(
      app.documents,
      mainDocument.id,
      { title: "Draft title" },
      { branch: "draft", base: "main" },
    );

    expect(await db.one(app.documents.where({ id: mainDocument.id }), { branch: "main" })).toEqual(
      mainDocument,
    );
    expect(
      await db.one(app.documents.where({ id: mainDocument.id }), {
        branch: "draft",
        base: "main",
      }),
    ).toMatchObject({ branch: "draft", title: "Draft title" });
  });

  it("rejects updates that try to change a branch column", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-immutable-column")),
      driver: { type: "memory" },
    });

    const document = db.insert(
      app.documents,
      { branch: "draft", title: "Draft title" },
      { branch: "draft" },
    ).value;

    await expect(
      db!
        .update(app.documents, document.id, { branch: "published" }, { branch: "draft" })
        .wait({ tier: "local" }),
    ).rejects.toThrow(
      "Schema: invalid mergeable commit: branch column does not match exact branch key",
    );

    expect(await db.one(app.documents.where({ id: document.id }), { branch: "draft" })).toEqual(
      document,
    );
    expect(
      await db.one(app.documents.where({ id: document.id }), { branch: "published" }),
    ).toBeNull();
  });

  it("captures pending local writes, restores the reference, and freezes the base below a live draft", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-frozen-capture")),
      driver: { type: "memory" },
    });
    const original = db.insert(
      app.documents,
      { branch: "main", title: "Original" },
      { branch: "main" },
    ).value;
    const capture = db.captureSnapshot();
    db.update(app.documents, original.id, { title: "Changed" }, { branch: "main" });
    const snapshot = parseSnapshotRef(
      JSON.parse(JSON.stringify(serializeSnapshotRef(await capture))),
    );
    const frozen = { branch: "draft", base: ["main", snapshot] } as const;
    expect(await db.one(app.documents, frozen)).toMatchObject({
      id: original.id,
      branch: "draft",
      title: "Original",
    });
    expect(await db.one(app.documents, { branch: "draft", base: "main" })).toMatchObject({
      title: "Changed",
    });
    db.update(app.documents, original.id, { title: "Draft" }, frozen);
    expect(await db.one(app.documents, frozen)).toMatchObject({ title: "Draft" });
    expect(await db.one(app.documents, { branch: "main" })).toMatchObject({ title: "Changed" });
  });

  it("does not capture uncommitted transaction writes", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-capture-staged")),
      driver: { type: "memory" },
    });
    const original = db.insert(
      app.documents,
      { branch: "main", title: "Original" },
      { branch: "main" },
    ).value;
    const tx = db.beginTransaction();
    tx.update(app.documents, original.id, { title: "Uncommitted" }, { branch: "main" });
    const snapshot = await db.captureSnapshot();
    tx.commit();
    expect(
      await db.one(app.documents, { branch: "draft", base: ["main", snapshot] }),
    ).toMatchObject({ title: "Original" });
    expect(await db.one(app.documents, { branch: "main" })).toMatchObject({ title: "Uncommitted" });
  });

  it("reads through a branch view inside a mergeable transaction, over its staged writes (jazz#5806)", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-tx-read-mergeable")),
      driver: { type: "memory" },
    });
    const original = db.insert(
      app.documents,
      { branch: "main", title: "Intro" },
      { branch: "main" },
    ).value;

    const tx = db.beginTransaction();
    expect(await tx.all(app.documents, { branch: "draft", base: "main" })).toEqual([
      { ...original, branch: "draft" },
    ]);
    tx.update(app.documents, original.id, { title: "Draft" }, { branch: "draft", base: "main" });
    expect(await tx.one(app.documents, { branch: "draft", base: "main" })).toMatchObject({
      branch: "draft",
      title: "Draft",
    });
    expect(await tx.one(app.documents, { branch: "main" })).toMatchObject({
      branch: "main",
      title: "Intro",
    });
    await tx.commit().wait({ tier: "local" });
    expect(await db.one(app.documents, { branch: "draft", base: "main" })).toMatchObject({
      title: "Draft",
    });
  });

  it("reads through a frozen base inside a mergeable transaction (jazz#5806)", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-tx-read-frozen-base")),
      driver: { type: "memory" },
    });
    const original = db.insert(
      app.documents,
      { branch: "main", title: "Intro" },
      { branch: "main" },
    ).value;
    const snapshot = await db.captureSnapshot();
    db.update(app.documents, original.id, { title: "Changed" }, { branch: "main" });
    const frozen = { branch: "draft", base: ["main", snapshot] } as const;

    const tx = db.beginTransaction();
    // Main as it was at the snapshot, not as it is now.
    expect(await tx.all(app.documents, frozen)).toEqual([{ ...original, branch: "draft" }]);
    tx.update(app.documents, original.id, { title: "Draft" }, frozen);
    // The staged write shows through the frozen view; main keeps moving.
    expect(await tx.one(app.documents, frozen)).toMatchObject({ branch: "draft", title: "Draft" });
    expect(await tx.one(app.documents, { branch: "main" })).toMatchObject({ title: "Changed" });
    await tx.commit().wait({ tier: "local" });
    expect(await db.one(app.documents, frozen)).toMatchObject({ title: "Draft" });
  });

  it("reads through a branch view inside a serializable transaction (jazz#5806)", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-tx-read-serializable")),
      driver: { type: "memory" },
    });
    const original = db.insert(
      app.documents,
      { branch: "main", title: "Intro" },
      { branch: "main" },
    ).value;
    // Also warms up the client, which serializable transactions need.
    expect(await db.all(app.documents, { branch: "main" })).toEqual([original]);

    const tx = db.beginSerializableTransaction();
    expect(await tx.all(app.documents, { branch: "draft", base: "main" })).toEqual([
      { ...original, branch: "draft" },
    ]);
    tx.update(app.documents, original.id, { title: "Draft" }, { branch: "draft", base: "main" });
    expect(await tx.one(app.documents, { branch: "draft", base: "main" })).toMatchObject({
      branch: "draft",
      title: "Draft",
    });
    // Committing it is validated upstream through the view (see
    // db.serializable-branch-read.test.ts); this Db has no server.
  });

  it("reports unavailable history when restoring onto an empty store", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-capture-origin")),
      driver: { type: "memory" },
    });
    db.insert(app.documents, { branch: "main", title: "Original" }, { branch: "main" });
    const restored = parseSnapshotRef(serializeSnapshotRef(await db.captureSnapshot()));
    const empty = await createDb({
      ...(await localAccountConfig("branch-api-capture-empty")),
      driver: { type: "memory" },
    });
    try {
      await expect(
        empty.all(app.documents, { branch: "draft", base: ["main", restored] }),
      ).rejects.toMatchObject({
        code: "not_observed",
        message: expect.stringContaining("Snapshot history is unavailable"),
      });
    } finally {
      await empty.shutdown();
    }
  });

  it("reports capture before schema attachment and after shutdown", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-capture-lifecycle")),
      driver: { type: "memory" },
    });
    await expect(db.captureSnapshot()).rejects.toThrow("Run a query or mutation first");
    await db.shutdown();
    await expect(db.captureSnapshot()).rejects.toThrow();
  });

  it("uses referenced row IDs as scalar branch selectors", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-reference-shorthand")),
      driver: { type: "memory" },
    });

    const main = db.insert(referenceApp.scenarios, { name: "Main" }).value;
    const draft = db.insert(referenceApp.scenarios, {
      name: "Draft",
      base_scenario_id: main.id,
    }).value;
    const task = db.insert(
      referenceApp.tasks,
      { scenario_id: main.id, title: "Inherited task" },
      { branch: main.id },
    ).value;

    expect(
      await db.one(referenceApp.tasks.where({ id: task.id }), {
        branch: draft.id,
        base: main.id,
      }),
    ).toEqual({ ...task, scenario_id: draft.id });
  });
});
