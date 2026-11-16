import { describe, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { applyColumnTransforms } from "./row-transformer.js";

describe("column transform input access", () => {
  it("does not inspect unrelated tables when guarding hopped stored-transform queries", () => {
    const app = testApp(
      {
        tables: {
          sources: {
            columns: { targetId: "uuid" },
            relations: { target: { rel: "targets", column: "targetId" } },
          },
          targets: { columns: { value: { type: "int", transform: "identity" } } },
          unrelated: { columns: { text: "string" } },
        },
        transforms: { identity: { stored: "int", view: "int" } },
      },
      {
        transforms: { identity: { from: (value: number) => value, to: (value: number) => value } },
      },
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
});
