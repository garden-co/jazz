import { describe, expect, it } from "vitest";
import { col, getCollectedSchema, resetCollectedState, table } from "./dsl.js";

describe("enum DSL invariants", () => {
  it("rejects empty variant list", () => {
    expect(() => (col.enum as (...args: unknown[]) => unknown)()).toThrow(
      "Enum columns require at least one variant.",
    );
  });

  it("rejects empty variant strings", () => {
    expect(() => col.enum("todo", "")).toThrow("Enum variants cannot be empty strings.");
  });

  it("rejects duplicate variants", () => {
    expect(() => col.enum("todo", "todo")).toThrow("Enum variants must be unique.");
  });

  it("rejects duplicate variants in add enum migration", () => {
    expect(() => col.add().enum("todo", "todo", { default: "todo" })).toThrow(
      "Enum variants must be unique.",
    );
  });

  it("rejects empty variants in drop enum migration", () => {
    expect(() => col.drop().enum("todo", "", { backwardsDefault: "todo" })).toThrow(
      "Enum variants cannot be empty strings.",
    );
  });
});

describe("inheritPolicy DSL", () => {
  it("stores inheritPolicy on ref columns", () => {
    resetCollectedState();
    table("todos", {
      image: col.ref("files").inheritPolicy(),
    });
    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns[0]).toMatchObject({
      name: "image",
      references: "files",
      inheritPolicy: true,
    });
  });

  it("stores inheritPolicy on array(ref(...)) columns", () => {
    resetCollectedState();
    table("files", {
      parts: col.array(col.ref("file_parts").inheritPolicy()),
    });
    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns[0]).toMatchObject({
      name: "parts",
      references: "file_parts",
      inheritPolicy: true,
    });
  });

  it("rejects inheritPolicy() for array(non-ref)", () => {
    expect(() => col.array(col.string()).inheritPolicy()).toThrow(
      "inheritPolicy() requires array(ref(...))",
    );
  });
});
