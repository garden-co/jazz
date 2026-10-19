import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { applyColumnTransforms } from "./row-transformer.js";

describe("column transform input access", () => {
  it("does not read any values when no transforms are configured", () => {
    const row = {
      get untouched() {
        throw new Error("untransformed values must not be copied");
      },
    };
    expect(applyColumnTransforms(row)).toBe(row);
  });

  it("reads only declared dependencies when preserving stored context", () => {
    const app = s.defineApp({
      prices: s.table(
        {
          amount: s.float().transform({
            dependsOn: ["unit"],
            from: (value, context: Readonly<{ unit: number }>) => value / context.unit,
            to: (value: number, context: Readonly<{ unit: number }>) => value * context.unit,
          }),
          unit: s.float(),
        },
        {},
      ),
    });
    const row = {
      amount: 250,
      unit: 100,
      get untouched() {
        throw new Error("unrelated values must not be copied");
      },
    };
    expect(applyColumnTransforms(row, app.prices._columnTransforms)).toBe(row);
    expect(row.amount).toBe(2.5);
  });
});
