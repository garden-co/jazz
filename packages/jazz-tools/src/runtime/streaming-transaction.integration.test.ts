import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createPolicyTestApp } from "../testing/index.js";

const app = s.defineApp(
  {
    files: s.table({ name: s.string(), data: s.bytes() }, {}),
  },
  { migrations: "none" },
);

const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.files.allowRead.where({ "$createdBy.account": session.user.account });
  policy.files.allowInsert.always();
  policy.files.allowUpdate.where({ "$createdBy.account": session.user.account });
});

const uploader = {
  issuer: "https://auth.files.example",
  user_id: "uploader",
  account_id: "00000000-0000-4000-8000-0000000f11e5",
  claims: {},
  authMode: "external" as const,
};

const size = 200 * 1024;
const expected = new Uint8Array(size).map((_, index) => index % 251);
async function* chunks(): AsyncGenerator<Uint8Array> {
  for (let offset = 0; offset < size; offset += 50_000) {
    yield expected.subarray(offset, offset + 50_000);
  }
}

describe("streaming inside a transaction", () => {
  it("commits a streamed value with the transaction's other writes", async () => {
    const testApp = await createPolicyTestApp(app, permissions, expect);
    try {
      const db = testApp.as(uploader);
      const tx = db.beginTransaction();
      const streamed = tx.insertStreaming(app.files, { name: "blob", data: chunks() });
      tx.insert(app.files, { name: "small", data: new Uint8Array([1, 2, 3]) });
      const { id } = await streamed;
      expect(await db.all(app.files)).toEqual([]);
      await tx.commit().wait({ tier: "local" });

      const row = await db.one(app.files.where({ id }));
      expect(row?.name).toBe("blob");
      expect(row?.data).toEqual(expected);
      expect((await db.all(app.files)).map((file) => file.name).sort()).toEqual(["blob", "small"]);
    } finally {
      await testApp.shutdown();
    }
  }, 30_000);

  it("drops a streamed value when the transaction rolls back", async () => {
    const testApp = await createPolicyTestApp(app, permissions, expect);
    try {
      const db = testApp.as(uploader);
      const tx = db.beginTransaction();
      await tx.insertStreaming(app.files, { name: "discarded", data: chunks() });
      await tx.rollback();
      expect(await db.all(app.files)).toEqual([]);
    } finally {
      await testApp.shutdown();
    }
  }, 30_000);

  it("streams into a row in a serializable transaction callback", async () => {
    const testApp = await createPolicyTestApp(app, permissions, expect);
    try {
      const db = testApp.as(uploader);
      const { id } = db.insert(app.files, { name: "file", data: new Uint8Array() }).value;
      const committed = await db.serializableTransaction(async (tx) => {
        await tx.updateStreaming(app.files, id, { data: chunks() });
      });
      await committed.wait();
      expect((await db.one(app.files.where({ id })))?.data).toEqual(expected);
    } finally {
      await testApp.shutdown();
    }
  }, 30_000);
});
