import { describe, expect, it } from "vitest";
import { serializeRuntimeSchema } from "./schema-wire.js";

describe("runtime schema identity", () => {
  it("is independent of property insertion order at any depth", () => {
    const a = { family_members: { columns: [{ name: "id", type: "uuid" }], policies: { read: "public" } } };
    const b = { family_members: { policies: { read: "public" }, columns: [{ type: "uuid", name: "id" }] } };
    expect(serializeRuntimeSchema(a as never)).toBe(serializeRuntimeSchema(b as never));
  });

  it("still distinguishes genuinely different column order", () => {
    const a = { t: { columns: [{ name: "a" }, { name: "b" }] } };
    const b = { t: { columns: [{ name: "b" }, { name: "a" }] } };
    expect(serializeRuntimeSchema(a as never)).not.toBe(serializeRuntimeSchema(b as never));
  });

  it("still distinguishes different table names", () => {
    const a = { family_members: { columns: [] } };
    const b = { familyMembers: { columns: [] } };
    expect(serializeRuntimeSchema(a as never)).not.toBe(serializeRuntimeSchema(b as never));
  });
});
