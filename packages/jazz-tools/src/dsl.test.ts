import { describe, expect, it } from "vitest";
import type { StandardJSONSchemaV1 } from "@standard-schema/spec";
import { col, getCollectedSchema, resetCollectedState, table } from "./dsl.js";
import { schemaToWasm } from "./codegen/schema-reader.js";
import { structuralSchemaHash } from "./dev/schema-utils.js";
import type { AddOp } from "./schema.js";

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

  it("rejects more scalar variants than the native tag space supports", () => {
    const variants = Array.from({ length: 257 }, (_, index) => `variant-${index}`);
    expect(() => (col.enum as (...values: string[]) => unknown)(...variants)).toThrow(
      "at most 256 variants",
    );
  });

  it("preserves scalar enum declaration order in both schema data and identity", () => {
    const wasmSchemaFor = (variants: [string, ...string[]]) => {
      resetCollectedState();
      table("tasks", { status: col.enum(...variants) });
      return schemaToWasm(getCollectedSchema());
    };

    const declared = wasmSchemaFor(["complete", "incomplete", "blocked"]);
    expect(declared.tasks!.columns[0]!.column_type).toEqual({
      type: "Enum",
      variants: ["complete", "incomplete", "blocked"],
    });

    expect(structuralSchemaHash(wasmSchemaFor(["complete", "incomplete"]))).not.toBe(
      structuralSchemaHash(wasmSchemaFor(["incomplete", "complete"])),
    );
  });

  it("builds scalar payload enum cases and rejects unsupported payload shapes", () => {
    const event = col.enum({
      message: { text: col.string(), level: col.int().optional() },
      closed: { code: col.int() },
    });
    expect(event._sqlType).toEqual({
      kind: "ENUM",
      cases: [
        {
          name: "message",
          fields: [
            { name: "text", sqlType: "TEXT", nullable: false },
            { name: "level", sqlType: "INTEGER", nullable: true },
          ],
        },
        { name: "closed", fields: [{ name: "code", sqlType: "INTEGER", nullable: false }] },
      ],
    });
    expect(() => col.enum({ bad: { type: col.string() } })).toThrow("reserved");
    expect(() => col.enum({ bad: { tags: col.array(col.string()) } })).toThrow(
      "must be scalar columns",
    );
    expect(() => col.enum({ bad: { authorId: col.ref("users") } })).toThrow(
      "cannot use references",
    );
  });

  describe("add enum", () => {
    it("rejects duplicate variants in add enum migration", () => {
      expect(() => col.add.enum("todo", "todo", { default: "todo" })).toThrow(
        "Enum variants must be unique.",
      );
    });

    it("rejects empty variants in drop enum migration", () => {
      expect(() => col.drop.enum("todo", "", { backwardsDefault: "todo" })).toThrow(
        "Enum variants cannot be empty strings.",
      );
    });

    it("preserves enum add default's nullability in the returned op type", () => {
      const requiredStatus = col.add.enum("todo", "done", { default: "todo" });
      const optionalStatus = col.add.enum("todo", "done", { default: null });

      const requiredOp: AddOp<{ kind: "ENUM"; variants: ["todo", "done"] }, "todo" | "done"> =
        requiredStatus;
      const optionalOp: AddOp<
        { kind: "ENUM"; variants: ["todo", "done"] },
        "todo" | "done" | null
      > = optionalStatus;

      expect(requiredOp.default).toBe("todo");
      expect(optionalOp.default).toBeNull();

      // @ts-expect-error non-nullable added enum defaults cannot be null
      const _invalidRequiredOp: AddOp<
        { kind: "ENUM"; variants: ["todo", "done"] },
        "todo" | "done"
      > = col.add.enum("todo", "done", { default: null });
    });
  });
});

describe("bytes DSL API", () => {
  it("supports bytes as the primary BYTEA builder name", () => {
    expect(col.bytes()._sqlType).toBe("BYTEA");
    expect(col.add.bytes({ default: new Uint8Array([0]) }).sqlType).toBe("BYTEA");
    expect(col.drop.bytes({ backwardsDefault: new Uint8Array([0]) }).sqlType).toBe("BYTEA");
  });
});

describe("json DSL API", () => {
  it("stores the Standard Schema output JSON schema", () => {
    resetCollectedState();
    const taskEstimateSchema = {
      "~standard": {
        version: 1,
        vendor: "jazz-test",
        jsonSchema: {
          input: () => ({ type: "string" }),
          output: () => ({ type: "number" }),
        },
      },
    } satisfies StandardJSONSchemaV1<string, number>;

    table("tasks", {
      estimate: col.json(taskEstimateSchema),
    });

    expect(getCollectedSchema().tables[0]?.columns[0]).toEqual({
      name: "estimate",
      sqlType: { kind: "JSON", schema: { type: "number" } },
      nullable: false,
    });
  });
});

describe("schema default DSL", () => {
  it("stores schema defaults on built columns", () => {
    resetCollectedState();
    table("todos", {
      done: col.boolean().default(false),
      status: col.enum("todo", "done").default("todo"),
      metadata: col.json().default({ archived: false }),
      ownerId: col.ref("users").default("00000000-0000-0000-0000-000000000001"),
      tags: col.array(col.string()).default(["work", "personal"]),
      archivedAt: col.timestamp().optional().default(null),
    });

    const columns = getCollectedSchema().tables[0]?.columns;
    expect(columns).toEqual([
      { name: "done", sqlType: "BOOLEAN", nullable: false, default: false },
      {
        name: "status",
        sqlType: { kind: "ENUM", variants: ["todo", "done"] },
        nullable: false,
        default: "todo",
      },
      {
        name: "metadata",
        sqlType: { kind: "JSON" },
        nullable: false,
        default: { archived: false },
      },
      {
        name: "ownerId",
        sqlType: "UUID",
        nullable: false,
        default: "00000000-0000-0000-0000-000000000001",
        references: "users",
      },
      {
        name: "tags",
        sqlType: { kind: "ARRAY", element: "TEXT" },
        nullable: false,
        default: ["work", "personal"],
      },
      { name: "archivedAt", sqlType: "TIMESTAMP", nullable: true, default: null },
    ]);
  });

  it("preserves optional() chaining when default is already set", () => {
    resetCollectedState();
    table("todos", {
      archivedAt: col.timestamp().default(0).optional(),
    });

    expect(getCollectedSchema().tables[0]?.columns[0]).toEqual({
      name: "archivedAt",
      sqlType: "TIMESTAMP",
      nullable: true,
      default: 0,
    });
  });

  it("types schema defaults by column and nullability", () => {
    col.boolean().default(false);
    col.timestamp().optional().default(null);
    col.enum("todo", "done").default("todo");
    col.ref("users").default("00000000-0000-0000-0000-000000000001");
    col.array(col.int()).default([1, 2, 3]);

    // @ts-expect-error non-nullable defaults cannot be null
    col.boolean().default(null);
    // @ts-expect-error integer defaults must be numbers
    col.int().default("1");
    // @ts-expect-error enum defaults must be one of the declared variants
    col.enum("todo", "done").default("archived");
    // @ts-expect-error ref defaults must be strings
    col.ref("users").default(123);
    // @ts-expect-error array defaults must match the element type
    col.array(col.int()).default(["1"]);
  });
});

describe("column merge strategy DSL", () => {
  it("stores counter merge strategy on integer columns and exports it to wasm schema", () => {
    resetCollectedState();
    table("counters", {
      value: col.int().merge("counter"),
      label: col.string(),
    });

    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns).toEqual([
      {
        name: "value",
        sqlType: "INTEGER",
        nullable: false,
        mergeStrategy: "counter",
      },
      {
        name: "label",
        sqlType: "TEXT",
        nullable: false,
      },
    ]);

    expect(schemaToWasm(schema)).toEqual({
      counters: {
        columns: [
          {
            name: "value",
            column_type: { type: "Integer" },
            nullable: false,
            merge_strategy: "Counter",
          },
          {
            name: "label",
            column_type: { type: "Text" },
            nullable: false,
          },
        ],
      },
    });
  });

  it("normalizes explicit lww away", () => {
    resetCollectedState();
    table("todos", {
      title: col.string().merge("lww"),
    });

    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns).toEqual([
      {
        name: "title",
        sqlType: "TEXT",
        nullable: false,
      },
    ]);
    expect(schemaToWasm(schema)).toEqual({
      todos: {
        columns: [
          {
            name: "title",
            column_type: { type: "Text" },
            nullable: false,
          },
        ],
      },
    });
  });

  it("rejects counter merge strategy on non-integer columns", () => {
    expect(() => col.string().merge("counter" as never)).toThrow(
      "Counter merge strategy is only supported on non-nullable INTEGER columns.",
    );
  });

  it("rejects counter merge strategy on nullable integer columns in either chaining order", () => {
    expect(() =>
      col
        .int()
        .optional()
        .merge("counter" as never),
    ).toThrow("Counter merge strategy is only supported on non-nullable INTEGER columns.");
    expect(() => col.int().merge("counter").optional()).toThrow(
      "Counter merge strategy is only supported on non-nullable INTEGER columns.",
    );
  });

  it("stores g-set merge strategy on array columns and exports it to wasm schema", () => {
    resetCollectedState();
    table("docs", {
      tags: col.array(col.string()).merge("g-set"),
    });

    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns).toEqual([
      {
        name: "tags",
        sqlType: { kind: "ARRAY", element: "TEXT" },
        nullable: false,
        mergeStrategy: "g-set",
      },
    ]);

    expect(schemaToWasm(schema)).toEqual({
      docs: {
        columns: [
          {
            name: "tags",
            column_type: { type: "Array", element: { type: "Text" } },
            nullable: false,
            merge_strategy: "GSet",
          },
        ],
      },
    });
  });

  it("rejects g-set merge strategy on non-array columns", () => {
    expect(() => col.string().merge("g-set" as never)).toThrow(
      "g-set merge strategy is only supported on non-nullable ARRAY columns.",
    );
  });

  it("rejects g-set merge strategy on nullable array columns in either chaining order", () => {
    expect(() =>
      col
        .array(col.string())
        .optional()
        .merge("g-set" as never),
    ).toThrow("g-set merge strategy is only supported on non-nullable ARRAY columns.");
    expect(() => col.array(col.string()).merge("g-set").optional()).toThrow(
      "g-set merge strategy is only supported on non-nullable ARRAY columns.",
    );
  });
});

describe("ref DSL", () => {
  it("stores references on ref columns", () => {
    resetCollectedState();
    table("todos", {
      imageId: col.ref("images"),
    });
    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns[0]).toMatchObject({
      name: "imageId",
      references: "images",
    });
  });

  it("stores references on array(ref(...)) columns", () => {
    resetCollectedState();
    table("bundles", {
      itemIds: col.array(col.ref("bundle_items")),
    });
    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns[0]).toMatchObject({
      name: "itemIds",
      references: "bundle_items",
    });
  });

  it("rejects scalar reference columns not ending in Id or _id", () => {
    resetCollectedState();
    expect(() => table("todos", { image: col.ref("images") })).toThrow(
      "Invalid reference key 'image'. Rename it to 'image_id' or 'imageId'.",
    );
  });

  it("rejects array(ref(...)) columns not ending in Ids or _ids", () => {
    resetCollectedState();
    expect(() => table("todos", { images: col.array(col.ref("images")) })).toThrow(
      "Invalid array reference key 'images'. Rename it to 'images_ids' or 'imagesIds'.",
    );
  });
});

describe("reserved magic-column namespace", () => {
  it("rejects schema columns starting with $", () => {
    resetCollectedState();
    expect(() =>
      table("todos", {
        $canRead: col.boolean(),
      }),
    ).toThrow(/reserved for magic columns/i);
  });
});
