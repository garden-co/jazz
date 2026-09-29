import { describe, expect, it } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { schema as s } from "../index.js";
import { createPolicyTestApp } from "./index.js";

const schema = {
  notes: s.table({ title: s.string(), ownerId: s.uuid() }, {}),
};
type Schema = s.Schema<typeof schema>;
const app: s.App<Schema> = s.defineApp(schema);

const sessionPolicy = definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ ownerId: session.user.account });
  policy.notes.allowInsert.where({ ownerId: session.user.account });
});
const openPolicy = definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
});

// The docs list `$createdAt` and friends as selectable edit-metadata columns.
// They must come back whatever shape the table's read policy has.
describe.each([
  ["an always() read policy", openPolicy],
  ["a session-dependent read policy", sessionPolicy],
])("selecting magic columns under %s", (_label, permissions) => {
  it("returns $createdAt and $updatedAt", async () => {
    const testApp = await createPolicyTestApp(app, permissions, expect);
    try {
      const account = crypto.randomUUID();
      const db = testApp.as({
        issuer: "https://magic-columns.test",
        user_id: "ada",
        account_id: account,
        claims: {},
        authMode: "external",
      });
      await db.insert(app.notes, { title: "one", ownerId: account }).wait({ tier: "global" });

      const rows = await db.all(app.notes.select("title", "$createdAt", "$updatedAt"), {
        tier: "global",
      });

      expect(rows).toHaveLength(1);
      expect(rows[0]!.$createdAt).toBeInstanceOf(Date);
      expect(rows[0]!.$updatedAt).toBeInstanceOf(Date);
    } finally {
      await testApp.shutdown();
    }
  }, 60_000);
});
