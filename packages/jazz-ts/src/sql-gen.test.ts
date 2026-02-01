import { describe, it, expect } from "vitest";
import { table, col, migrate, getCollectedSchema, getCollectedMigration, resetCollectedState } from "./dsl.js";
import { schemaToSql, lensToSql } from "./sql-gen.js";

describe("schemaToSql", () => {
  it("generates SQL matching schema_455a1f10a158 (v1: title + completed)", () => {
    resetCollectedState();
    table("todos", {
      title: col.string(),
      completed: col.boolean(),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    expect(sql).toBe(`CREATE TABLE todos (
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL
);
`);
  });

  it("generates SQL matching schema_357c464c4c43 (v2: + description)", () => {
    resetCollectedState();
    table("todos", {
      title: col.string(),
      completed: col.boolean(),
      description: col.string().optional(),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    expect(sql).toBe(`CREATE TABLE todos (
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL,
    description TEXT
);
`);
  });

  it("handles all column types", () => {
    resetCollectedState();
    table("test", {
      text: col.string(),
      text_null: col.string().optional(),
      bool: col.boolean(),
      bool_null: col.boolean().optional(),
      integer: col.int(),
      integer_null: col.int().optional(),
      real: col.float(),
      real_null: col.float().optional(),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    expect(sql).toContain("text TEXT NOT NULL");
    expect(sql).toContain("text_null TEXT");
    expect(sql).toContain("bool BOOLEAN NOT NULL");
    expect(sql).toContain("bool_null BOOLEAN");
    expect(sql).toContain("integer INTEGER NOT NULL");
    expect(sql).toContain("integer_null INTEGER");
    expect(sql).toContain("real REAL NOT NULL");
    expect(sql).toContain("real_null REAL");
  });
});

describe("lensToSql", () => {
  it("generates forward lens SQL matching lens_455a1f10a158_357c464c4c43_fwd", () => {
    resetCollectedState();
    migrate("todos", {
      description: col.add().string({ default: "" }),
    });
    const lens = getCollectedMigration()!;

    const sql = lensToSql(lens, "fwd");

    expect(sql).toBe(`ALTER TABLE todos ADD COLUMN description TEXT DEFAULT '';
`);
  });

  it("generates backward lens SQL matching lens_455a1f10a158_357c464c4c43_bwd", () => {
    resetCollectedState();
    migrate("todos", {
      description: col.add().string({ default: "" }),
    });
    const lens = getCollectedMigration()!;

    const sql = lensToSql(lens, "bwd");

    expect(sql).toBe(`ALTER TABLE todos DROP COLUMN description;
`);
  });

  it("handles rename operations", () => {
    resetCollectedState();
    migrate("todos", {
      new_name: col.rename("old_name"),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(
      `ALTER TABLE todos RENAME COLUMN new_name TO old_name;
`
    );
    expect(lensToSql(lens, "bwd")).toBe(
      `ALTER TABLE todos RENAME COLUMN old_name TO new_name;
`
    );
  });

  it("handles drop operations", () => {
    resetCollectedState();
    migrate("todos", {
      removed: col.drop().string({ backwardsDefault: "default_value" }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(
      `ALTER TABLE todos DROP COLUMN removed;
`
    );
    expect(lensToSql(lens, "bwd")).toBe(
      `ALTER TABLE todos ADD COLUMN removed TEXT DEFAULT 'default_value';
`
    );
  });
});
