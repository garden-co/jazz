import { describe, expect, it } from "vitest";
import {
  col,
  getCollectedMigration,
  getCollectedSchema,
  migrate,
  resetCollectedState,
  table,
} from "./dsl.js";

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
        sqlType: { kind: "ENUM", variants: ["done", "todo"] },
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

describe("ref DSL", () => {
  it("stores references on ref columns", () => {
    resetCollectedState();
    table("todos", {
      imageId: col.ref("files"),
    });
    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns[0]).toMatchObject({
      name: "imageId",
      references: "files",
    });
  });

  it("stores references on array(ref(...)) columns", () => {
    resetCollectedState();
    table("files", {
      partIds: col.array(col.ref("file_parts")),
    });
    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns[0]).toMatchObject({
      name: "partIds",
      references: "file_parts",
    });
  });

  it("rejects scalar reference columns not ending in Id or _id", () => {
    resetCollectedState();
    // @ts-expect-error ref columns must end in Id or _id
    expect(() => table("todos", { image: col.ref("files") })).toThrow(
      "Invalid reference key 'image'. Rename it to 'image_id' or 'imageId'.",
    );
  });

  it("rejects array(ref(...)) columns not ending in Ids or _ids", () => {
    resetCollectedState();
    // @ts-expect-error array(ref(...)) columns must end in Ids or _ids
    expect(() => table("todos", { images: col.array(col.ref("files")) })).toThrow(
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

  it("rejects introduced migration columns starting with $", () => {
    resetCollectedState();
    expect(() =>
      migrate("todos", {
        $canRead: col.add().boolean({ default: false }),
      }),
    ).toThrow(/reserved for magic columns/i);
  });

  it("allows dropping legacy $-prefixed columns", () => {
    resetCollectedState();
    expect(() =>
      migrate("todos", {
        $legacy: col.drop().boolean({ backwardsDefault: false }),
      }),
    ).not.toThrow();

    expect(getCollectedMigration()).toEqual({
      table: "todos",
      operations: [
        {
          type: "drop",
          column: "$legacy",
          sqlType: "BOOLEAN",
          value: false,
        },
      ],
    });
  });
});
