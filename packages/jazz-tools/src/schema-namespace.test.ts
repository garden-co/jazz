import { describe, expect, it } from "vitest";
import { schema } from "./index.js";
import { schema as nativeSchema } from "./react-native/index.js";
import { allowExternalProvenanceName, schemaColumns } from "./dsl.js";
import { definePermissions } from "./permissions/index.js";
import { rel, reverse } from "./relationships.js";
import {
  defineApp,
  defineComputed,
  defineSchema,
  defineSliceableApp,
  defineTable,
} from "./typed-app.js";

describe("schema namespace", () => {
  // `schema` used to be an object literal of these same builders. It's an ES
  // namespace now so bundlers can drop the members an app doesn't use; every
  // member it had must still be there, as the same function.
  it("has every builder it had as an object", () => {
    const expected: Record<string, unknown> = {
      ...schemaColumns,
      rel,
      reverse,
      table: defineTable,
      computed: defineComputed,
      defineSchema,
      defineApp,
      defineSliceableApp,
      definePermissions,
      allowExternalProvenanceName,
    };
    expect(Object.keys(schema).sort()).toEqual(Object.keys(expected).sort());
    for (const [name, builder] of Object.entries(expected)) {
      expect(schema[name as keyof typeof schema], name).toBe(builder);
    }
  });

  it("is the same namespace in the React Native entry", () => {
    expect(nativeSchema).toBe(schema);
  });

  it("still builds a schema and its permissions", () => {
    const app = schema.defineApp(
      {
        todos: schema.table(
          {
            title: schema.string(),
            status: schema.enum("open", "done"),
            tags: schema.array(schema.string()),
          },
          {},
        ),
      },
      { migrations: "none" },
    );
    const permissions = schema.definePermissions(app, ({ policy }) => {
      policy.todos.allowRead.always();
    });
    expect(permissions).toBeDefined();
  });
});
