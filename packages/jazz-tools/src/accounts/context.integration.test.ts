import { describe, expect, it } from "vitest";
import { createDb, schema } from "../index.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";

describe("account context authority", () => {
  it("opens locally without activating its registry transport and rejects another app or authority", async () => {
    const { serverUrl: _serverUrl, ...config } = await localAccountConfig("local-only-account");
    const app = schema.defineApp({ notes: schema.table({ text: schema.string() }) });
    const db = await createDb(config);
    try {
      const { value: row } = await db.insert(app.notes, { text: "offline" });
      expect(await db.all(app.notes)).toEqual([
        expect.objectContaining({ id: row.id, text: "offline" }),
      ]);
      expect(await db.one(app.notes.select("$createdBy").where({ id: row.id }))).toMatchObject({
        $createdBy: { account: config.account.id, identity: config.account.identity },
      });
      const author = { account: config.account.id, identity: config.account.identity };
      const otherAuthor = {
        ...author,
        account:
          author.account === "00000000-0000-4000-8000-000000000001"
            ? "00000000-0000-4000-8000-000000000002"
            : "00000000-0000-4000-8000-000000000001",
      };
      expect(await db.all(app.notes.where({ $createdBy: author }))).toHaveLength(1);
      expect(await db.all(app.notes.where({ "$createdBy.account": author.account }))).toHaveLength(
        1,
      );
      expect(
        await db.all(app.notes.where({ "$createdBy.identity": author.identity })),
      ).toHaveLength(1);
      expect(await db.all(app.notes.where({ $createdBy: { ne: author } }))).toEqual([]);
      const accountlessAuthor = { ...author, account: null };
      await expect(async () =>
        db.all(
          // @ts-expect-error Durable row-author conditions require an account.
          app.notes.where({ $createdBy: { ne: accountlessAuthor } }),
        ),
      ).rejects.toThrow('Invalid structured author condition for "$createdBy"');
      expect(await db.all(app.notes.where({ $createdBy: { ne: otherAuthor } }))).toHaveLength(1);
      expect(
        await db.all(
          app.notes.where({
            $createdBy: { ...author, identity: { ...author.identity, subject: "someone-else" } },
          }),
        ),
      ).toEqual([]);
      expect(
        await db.all(
          app.union([
            app.notes.where({ $createdBy: author }),
            app.notes.where({ $createdBy: otherAuthor }),
          ]),
        ),
      ).toHaveLength(1);
      await expect(createDb({ ...config, appId: "another-app" })).rejects.toMatchObject({
        code: "account_application_mismatch",
      });
      await expect(
        createDb({ ...config, serverUrl: "https://other-core.example" }),
      ).rejects.toMatchObject({ code: "account_application_mismatch" });
    } finally {
      await db.shutdown();
    }
  });
});
