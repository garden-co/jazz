import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { applyColumnTransforms } from "./row-transformer.js";

describe("column transform input access", () => {
  it("does not inspect unrelated tables when guarding hopped stored-transform queries", () => {
    const app = s.defineApp(
      {
        sources: s.table({ targetId: s.uuid() }, { target: s.rel("targets", "targetId") }),
        targets: s.table(
          { value: s.int().transform({ from: (value) => value, to: (value: number) => value }) },
          {},
        ),
        unrelated: s.table({ text: s.string() }, {}),
      },
      { migrations: "none" },
    );
    const unrelated = app.wasmSchema.unrelated!;
    const original = Object.getOwnPropertyDescriptor(unrelated, "columns")!;
    let unrelatedReads = 0;
    Object.defineProperty(unrelated, "columns", {
      configurable: true,
      get() {
        unrelatedReads++;
        return original.value;
      },
    });
    try {
      expect(() => app.sources.hopTo("target").where({ value: 1 }).orderBy("value")).not.toThrow();
      expect(unrelatedReads).toBe(0);
    } finally {
      Object.defineProperty(unrelated, "columns", original);
    }
  });

  it("does not read any values when no transforms are configured", () => {
    const row = {
      get untouched() {
        throw new Error("untransformed values must not be copied");
      },
    };
    expect(applyColumnTransforms(row)).toBe(row);
  });

  it("reads only declared dependencies when preserving stored context", () => {
    const app = s.defineApp(
      {
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
      },
      { migrations: "none" },
    );
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
