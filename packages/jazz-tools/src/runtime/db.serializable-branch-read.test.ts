import { expect, it } from "vitest";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz, testApp } from "../testing/index.js";

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
