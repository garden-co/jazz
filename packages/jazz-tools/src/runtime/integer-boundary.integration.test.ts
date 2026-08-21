import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createPolicyTestApp } from "../testing/index.js";

const app = s.defineApp({
  ints: s.table({ value: s.int() }),
});

const permissions = s.definePermissions(app, ({ policy }) => {
  policy.ints.allowInsert.always();
  policy.ints.allowRead.always();
  policy.ints.allowUpdate.always();
  policy.ints.allowDelete.always();
});

describe("public integer values", () => {
  it("round-trips i32 boundaries and rejects invalid values before the runtime", async () => {
    const testApp = await createPolicyTestApp(app, permissions, expect);

    try {
      const db = testApp.as({ user_id: "integer-boundary", claims: {}, authMode: "local-first" });
      const boundaries = [-2_147_483_648, 2_147_483_647];

      for (const value of boundaries) {
        const row = await db.insert(app.ints, { value }).wait({ tier: "global" });
        expect(row.value).toBe(value);
      }

      for (const value of [-2_147_483_649, 2_147_483_648, 1.5, NaN, Infinity]) {
        expect(() => db.insert(app.ints, { value })).toThrow(
          "Integer values must be signed 32-bit integers between -2147483648 and 2147483647",
        );
      }
    } finally {
      await testApp.shutdown();
    }
  }, 10_000);
});
