import { describe, expect, it } from "vitest";
import { createPolicyTestApp, testApp } from "../testing/index.js";

const app = testApp({ tables: { ints: { columns: { value: "int" } } } });

const permissions = {
  tables: { ints: { insert: "true", read: "true", update: "true", delete: "true" } },
};

describe("public integer values", () => {
  it("round-trips i32 boundaries and rejects invalid values before the runtime", async () => {
    const policyApp = await createPolicyTestApp(app, permissions, expect);

    try {
      const db = policyApp.as({
        issuer: "https://issuer.example",
        user_id: "integer-boundary",
        claims: {},
        authMode: "external",
      });
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
      await policyApp.shutdown();
    }
  }, 10_000);
});
