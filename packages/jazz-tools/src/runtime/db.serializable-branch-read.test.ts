import { expect, it, vi } from "vitest";
import { NapiDb } from "jazz-napi";
import { WasmDb } from "jazz-wasm";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz, testApp } from "../testing/index.js";
import { FEATURE_SERIALIZABLE_BRANCH_READS } from "./native-runtime/websocket.js";

const app = testApp({
  tables: {
    documents: { columns: { branch: "string", title: "string" }, branchBy: ["branch"] },
  },
});
const permissions = {
  format: 1,
  tables: { documents: { read: "true", insert: "true", update: "true" } },
};

// Over a link that negotiated FEATURE_SERIALIZABLE_BRANCH_READS, the server
// validates a serializable transaction's reads through the branch view
// (jazz#5806): a base change under the draft it read is a conflict.
it("validates serializable branch reads upstream", async () => {
  const server = await startTestJazz({ app, permissions, coreOptions: { inMemory: true } });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let writer: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    db = await createDb(await localAccountConfig(server.appId, server.url));
    writer = await createDb(await localAccountConfig(server.appId, server.url));
    const document = await db
      .insert(app.documents, { branch: "main", title: "Intro" }, { branch: "main" })
      .wait({ tier: "global" });
    const draft = { branch: "draft", base: "main" } as const;
    expect(await db.all(app.documents, { ...draft, tier: "remote" })).toEqual([
      { ...document, branch: "draft" },
    ]);

    const stable = db.beginSerializableTransaction();
    expect(await stable.all(app.documents, { ...draft, tier: "local-first" })).toHaveLength(1);
    stable.update(app.documents, document.id, { title: "Draft" }, draft);
    await stable.commit().wait();

    const changed = db.beginSerializableTransaction();
    expect(await changed.all(app.documents, { ...draft, tier: "local-first" })).toHaveLength(1);
    // A new main row shows through the draft: what `changed` read is stale.
    await writer
      .insert(app.documents, { branch: "main", title: "Concurrent" }, { branch: "main" })
      .wait({ tier: "global" });
    await expect(changed.commit().wait()).rejects.toMatchObject({
      code: "serialization_conflict",
    });
  } finally {
    await writer?.shutdown();
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);

// A link that did not negotiate FEATURE_SERIALIZABLE_BRANCH_READS (an older
// server, here a client that doesn't advertise it): the commit applies
// locally, and the client refuses to send it rather than have it validated
// as a root read.
it("refuses to send a serializable branch read over a link without the feature", async () => {
  const masked = (features: number) => features & ~FEATURE_SERIALIZABLE_BRANCH_READS;
  const spies = [WasmDb.prototype, NapiDb.prototype].map((prototype) => {
    const original = prototype.wireFeatures;
    return vi
      .spyOn(prototype, "wireFeatures")
      .mockImplementation(function (this: typeof prototype) {
        return masked(original.call(this));
      });
  });
  const server = await startTestJazz({ app, permissions, coreOptions: { inMemory: true } });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    db = await createDb(await localAccountConfig(server.appId, server.url));
    const document = await db
      .insert(app.documents, { branch: "main", title: "Intro" }, { branch: "main" })
      .wait({ tier: "global" });
    const draft = { branch: "draft", base: "main" } as const;
    expect(await db.all(app.documents, { ...draft, tier: "remote" })).toHaveLength(1);

    const tx = db.beginSerializableTransaction();
    expect(await tx.all(app.documents, { ...draft, tier: "local-first" })).toHaveLength(1);
    tx.update(app.documents, document.id, { title: "Draft" }, draft);
    await expect(tx.commit().wait()).rejects.toMatchObject({ code: "write_rejected" });
  } finally {
    for (const spy of spies) spy.mockRestore();
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);

// What a serializable branch read records is its view's rows: writes outside
// that view don't conflict with it.
it("doesn't conflict on writes outside the branch view it read", async () => {
  const server = await startTestJazz({ app, permissions, coreOptions: { inMemory: true } });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let writer: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    db = await createDb(await localAccountConfig(server.appId, server.url));
    writer = await createDb(await localAccountConfig(server.appId, server.url));
    const document = await db
      .insert(app.documents, { branch: "main", title: "Intro" }, { branch: "main" })
      .wait({ tier: "global" });
    const draft = { branch: "draft", base: "main" } as const;
    expect(await db.all(app.documents, { ...draft, tier: "remote" })).toHaveLength(1);

    // Another branch changed.
    const otherBranch = db.beginSerializableTransaction();
    expect(await otherBranch.all(app.documents, { ...draft, tier: "local-first" })).toHaveLength(1);
    await writer
      .insert(app.documents, { branch: "other", title: "Elsewhere" }, { branch: "other" })
      .wait({ tier: "global" });
    otherBranch.update(app.documents, document.id, { title: "Draft" }, draft);
    await otherBranch.commit().wait();

    // A later write under a frozen base.
    const snapshot = await db.captureSnapshot();
    const frozen = { branch: "draft", base: ["main", snapshot] } as const;
    const underFrozen = db.beginSerializableTransaction();
    expect(await underFrozen.all(app.documents, { ...frozen, tier: "local-first" })).toHaveLength(
      1,
    );
    await writer
      .insert(app.documents, { branch: "main", title: "After the snapshot" }, { branch: "main" })
      .wait({ tier: "global" });
    underFrozen.update(app.documents, document.id, { title: "Frozen draft" }, frozen);
    await underFrozen.commit().wait();
  } finally {
    await writer?.shutdown();
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);
