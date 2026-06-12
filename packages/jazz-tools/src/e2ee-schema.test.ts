import { describe, expect, it } from "vitest";
import { schema as s } from "./index.js";

const validDefinition = () => ({
  projects: s.table({ name: s.string() }).encryptionSpace(),
  todos: s.table({
    title: s.string().encrypted("projectId"),
    done: s.boolean(),
    projectId: s.ref("projects"),
  }),
});

const wasmSchemaOf = (app: unknown): Record<string, any> =>
  (app as { wasmSchema: Record<string, any> }).wasmSchema;

describe("E2EE schema DSL", () => {
  it("emits encryption markers and the $keys companion table", () => {
    const wasm = wasmSchemaOf(s.defineApp(validDefinition()));

    expect(wasm.projects.encryption_space).toBe(true);
    const title = wasm.todos.columns.find((c: any) => c.name === "title");
    expect(title.encrypted_with).toBe("projectId");

    const keys = wasm["projects$keys"];
    expect(keys).toBeDefined();
    expect(keys.columns.map((c: any) => c.name)).toEqual([
      "space_id",
      "key_id",
      "recipient_user_id",
      "recipient_public_key",
      "sealed_key",
    ]);
    expect(keys.columns[0].references).toBe("projects");
    expect(keys.policies.select.using.type).toBe("True");
    expect(keys.policies.insert.with_check).toEqual({
      type: "SessionIsNotNull",
      path: ["user_id"],
    });
    expect(keys.policies.update).toBeUndefined();
    expect(keys.policies.delete.using.type).toBe("SessionIsNotNull");
  });

  it("excludes encrypted columns from indexing", () => {
    const wasm = wasmSchemaOf(s.defineApp(validDefinition()));
    expect(wasm.todos.indexed_columns).toEqual(["done", "projectId"]);
  });

  it("rejects encrypted() pointing at a missing column", () => {
    expect(() =>
      s.defineApp({
        projects: s.table({ name: s.string() }).encryptionSpace(),
        todos: s.table({ title: s.string().encrypted("missing") }),
      }),
    ).toThrow(/missing/);
  });

  it("rejects encrypted() pointing at a nullable ref", () => {
    expect(() =>
      s.defineApp({
        projects: s.table({ name: s.string() }).encryptionSpace(),
        todos: s.table({
          title: s.string().encrypted("projectId"),
          projectId: s.ref("projects").optional(),
        }),
      }),
    ).toThrow(/non-nullable/);
  });

  it("rejects encrypted() pointing at a non-space table", () => {
    expect(() =>
      s.defineApp({
        projects: s.table({ name: s.string() }),
        todos: s.table({
          title: s.string().encrypted("projectId"),
          projectId: s.ref("projects"),
        }),
      }),
    ).toThrow(/encryption space/);
  });

  it("rejects user tables containing $", () => {
    expect(() => s.defineApp({ nope$keys: s.table({ name: s.string() }) })).toThrow(/reserved/);
  });

  it("rejects explicitly indexing an encrypted column", () => {
    expect(() =>
      s.defineApp({
        projects: s.table({ name: s.string() }).encryptionSpace(),
        todos: s
          .table({
            title: s.string().encrypted("projectId"),
            projectId: s.ref("projects"),
          })
          .indexOnly(["title"]),
      }),
    ).toThrow(/index/);
  });
});
