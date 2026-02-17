import { describe, it, expect } from "vitest";
import {
  table,
  col,
  migrate,
  getCollectedSchema,
  getCollectedMigration,
  resetCollectedState,
} from "./dsl.js";
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

  it("handles array column types", () => {
    resetCollectedState();
    table("arrays", {
      numbers: col.array(col.int()),
      tags: col.array(col.string()),
      flags: col.array(col.boolean()),
      matrix: col.array(col.array(col.int())),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    expect(sql).toContain("numbers INTEGER[] NOT NULL");
    expect(sql).toContain("flags BOOLEAN[] NOT NULL");
    expect(sql).toContain("tags TEXT[]");
    expect(sql).toContain("matrix INTEGER[][] NOT NULL");
  });

  it("generates UUID REFERENCES for required ref", () => {
    resetCollectedState();
    table("todos", {
      title: col.string(),
      owner_id: col.ref("users"),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    expect(sql).toContain("owner_id UUID REFERENCES users NOT NULL");
  });

  it("generates nullable UUID REFERENCES for optional ref", () => {
    resetCollectedState();
    table("todos", {
      title: col.string(),
      parent_id: col.ref("todos").optional(),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    expect(sql).toContain("parent_id UUID REFERENCES todos");
    expect(sql).not.toContain("parent_id UUID REFERENCES todos NOT NULL");
  });

  it("stores references in Column metadata", () => {
    resetCollectedState();
    table("todos", {
      owner_id: col.ref("users"),
      parent_id: col.ref("todos").optional(),
    });
    const schema = getCollectedSchema();

    const owner = schema.tables[0].columns.find((c) => c.name === "owner_id")!;
    expect(owner.sqlType).toBe("UUID");
    expect(owner.references).toBe("users");
    expect(owner.nullable).toBe(false);

    const parent = schema.tables[0].columns.find((c) => c.name === "parent_id")!;
    expect(parent.sqlType).toBe("UUID");
    expect(parent.references).toBe("todos");
    expect(parent.nullable).toBe(true);
  });

  it("stores references in array(ref(...)) metadata", () => {
    resetCollectedState();
    table("files", {
      parts: col.array(col.ref("file_parts")),
    });
    const schema = getCollectedSchema();

    const parts = schema.tables[0].columns.find((c) => c.name === "parts")!;
    expect(parts.sqlType).toEqual({ kind: "ARRAY", element: "UUID" });
    expect(parts.references).toBe("file_parts");
    expect(parts.nullable).toBe(false);
  });

  it("generates complete table with mixed columns and refs", () => {
    resetCollectedState();
    table("todos", {
      title: col.string(),
      parent_id: col.ref("todos").optional(),
      owner_id: col.ref("users"),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    expect(sql).toBe(`CREATE TABLE todos (
    title TEXT NOT NULL,
    parent_id UUID REFERENCES todos,
    owner_id UUID REFERENCES users NOT NULL
);
`);
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
`,
    );
    expect(lensToSql(lens, "bwd")).toBe(
      `ALTER TABLE todos RENAME COLUMN old_name TO new_name;
`,
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
`,
    );
    expect(lensToSql(lens, "bwd")).toBe(
      `ALTER TABLE todos ADD COLUMN removed TEXT DEFAULT 'default_value';
`,
    );
  });

  it("handles add nullable column operations", () => {
    resetCollectedState();
    migrate("todos", {
      description: col.add().optional().string({ default: null }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(
      `ALTER TABLE todos ADD COLUMN description TEXT DEFAULT NULL;
`,
    );
    expect(lensToSql(lens, "bwd")).toBe(
      `ALTER TABLE todos DROP COLUMN description;
`,
    );
  });

  it("preserves SQL type for add lens operations", () => {
    resetCollectedState();
    migrate("todos", {
      priority: col.add().int({ default: 0 }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(`ALTER TABLE todos ADD COLUMN priority INTEGER DEFAULT 0;
`);
  });

  it("preserves SQL type for drop lens operations", () => {
    resetCollectedState();
    migrate("todos", {
      priority: col.drop().int({ backwardsDefault: 0 }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "bwd")).toBe(`ALTER TABLE todos ADD COLUMN priority INTEGER DEFAULT 0;
`);
  });
});
