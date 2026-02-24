import { describe, expect, it } from "vitest";
import { col } from "./dsl.js";

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

describe("bytes DSL API", () => {
  it("supports bytes as the primary BYTEA builder name", () => {
    expect(col.bytes()._sqlType).toBe("BYTEA");
    expect(col.add().bytes({ default: new Uint8Array([0]) }).sqlType).toBe("BYTEA");
    expect(col.drop().bytes({ backwardsDefault: new Uint8Array([0]) }).sqlType).toBe("BYTEA");
  });
});
