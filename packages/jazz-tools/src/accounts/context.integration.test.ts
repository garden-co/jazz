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
