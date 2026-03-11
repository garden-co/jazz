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

describe("ref DSL", () => {
  it("stores references on ref columns", () => {
    resetCollectedState();
    table("todos", {
      image: col.ref("files"),
    });
    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns[0]).toMatchObject({
      name: "image",
      references: "files",
    });
  });

  it("stores references on array(ref(...)) columns", () => {
    resetCollectedState();
    table("files", {
      parts: col.array(col.ref("file_parts")),
    });
    const schema = getCollectedSchema();
    expect(schema.tables[0]?.columns[0]).toMatchObject({
      name: "parts",
      references: "file_parts",
    });
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
