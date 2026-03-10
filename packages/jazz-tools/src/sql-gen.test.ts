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

  it("quotes reserved keyword identifiers", () => {
    resetCollectedState();
    table("data_entry_entries", {
      table: col.string(),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    expect(sql).toBe(`CREATE TABLE data_entry_entries (
    "table" TEXT NOT NULL
);
`);
  });

  it("rejects reserved magic-column namespace in schema columns", () => {
    expect(() =>
      schemaToSql({
        tables: [
          {
            name: "todos",
            columns: [{ name: "$canRead", sqlType: "BOOLEAN", nullable: false }],
          },
        ],
      }),
    ).toThrow(/reserved for magic columns/i);
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
      ts: col.timestamp(),
      ts_null: col.timestamp().optional(),
      real: col.float(),
      real_null: col.float().optional(),
      blob: col.bytes(),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    expect(sql).toContain("text TEXT NOT NULL");
    expect(sql).toContain("text_null TEXT");
    expect(sql).toContain("bool BOOLEAN NOT NULL");
    expect(sql).toContain("bool_null BOOLEAN");
    expect(sql).toContain("integer INTEGER NOT NULL");
    expect(sql).toContain("integer_null INTEGER");
    expect(sql).toContain("ts TIMESTAMP NOT NULL");
    expect(sql).toContain("ts_null TIMESTAMP");
    expect(sql).toContain("real REAL NOT NULL");
    expect(sql).toContain("real_null REAL");
    expect(sql).toContain("blob BYTEA NOT NULL");
  });

  it("handles enum column types", () => {
    resetCollectedState();
    table("tasks", {
      status: col.enum("in_progress", "todo", "done"),
    });
    const schema = getCollectedSchema();

    const sql = schemaToSql(schema);

    // Variants are normalized in DSL.
    expect(sql).toContain("status ENUM('done','in_progress','todo') NOT NULL");
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

  it("generates CREATE POLICY statements from table permissions", () => {
    resetCollectedState();
    table("todos", {
      title: col.string(),
      owner_id: col.string(),
    });
    const schema = getCollectedSchema();
    const ownerMatchesSession: import("./schema.js").PolicyExpr = {
      type: "Cmp",
      column: "owner_id",
      op: "Eq",
      value: { type: "SessionRef", path: ["user_id"] },
    };

    schema.tables[0]!.policies = {
      select: { using: ownerMatchesSession },
      insert: { with_check: ownerMatchesSession },
      update: { using: ownerMatchesSession, with_check: ownerMatchesSession },
      delete: { using: ownerMatchesSession },
    };

    const sql = schemaToSql(schema);

    expect(sql).toBe(`CREATE TABLE todos (
    title TEXT NOT NULL,
    owner_id TEXT NOT NULL
);
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (owner_id = @session.user_id);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING (owner_id = @session.user_id) WITH CHECK (owner_id = @session.user_id);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (owner_id = @session.user_id);
`);
  });

  it("generates INHERITS REFERENCING policy expressions", () => {
    resetCollectedState();
    table("files", {
      owner_id: col.string(),
    });
    const schema = getCollectedSchema();

    schema.tables[0]!.policies = {
      select: {
        using: {
          type: "InheritsReferencing",
          operation: "Select",
          source_table: "todos",
          via_column: "image",
        },
      },
    };

    const sql = schemaToSql(schema);
    expect(sql).toContain(
      "CREATE POLICY files_select_policy ON files FOR SELECT USING (INHERITS SELECT REFERENCING todos VIA image);",
    );
  });

  it("generates CONTAINS and IN-list policy expressions", () => {
    resetCollectedState();
    table("todos", {
      owner_id: col.string(),
      status: col.string(),
    });
    const schema = getCollectedSchema();
    schema.tables[0]!.policies = {
      select: {
        using: {
          type: "And",
          exprs: [
            {
              type: "Contains",
              column: "owner_id",
              value: { type: "Literal", value: "ali" },
            },
            {
              type: "InList",
              column: "status",
              values: [
                { type: "Literal", value: "active" },
                { type: "Literal", value: "trial" },
              ],
            },
          ],
        },
      },
    };

    const sql = schemaToSql(schema);
    expect(sql).toContain(
      "CREATE POLICY todos_select_policy ON todos FOR SELECT USING ((owner_id CONTAINS 'ali') AND (status IN ('active', 'trial')));",
    );
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

  it("rejects reserved magic-column namespace in introduced lens columns", () => {
    expect(() =>
      lensToSql(
        {
          table: "todos",
          operations: [{ type: "introduce", column: "$canRead", sqlType: "BOOLEAN", value: false }],
        },
        "fwd",
      ),
    ).toThrow(/reserved for magic columns/i);
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

  it("renders bytea defaults as hex literals", () => {
    resetCollectedState();
    migrate("files", {
      payload: col.add().bytes({ default: new Uint8Array([0, 1, 255]) }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd"))
      .toBe(`ALTER TABLE files ADD COLUMN payload BYTEA DEFAULT '\\\\x0001ff';
`);
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

  it("preserves TIMESTAMP type for add lens operations", () => {
    resetCollectedState();
    migrate("todos", {
      created_at: col.add().timestamp({ default: 1735689600000000 }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(
      `ALTER TABLE todos ADD COLUMN created_at TIMESTAMP DEFAULT 1735689600000000;
`,
    );
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

  it("preserves TIMESTAMP type for drop lens operations", () => {
    resetCollectedState();
    migrate("todos", {
      created_at: col.drop().timestamp({ backwardsDefault: 1735689600000000 }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "bwd")).toBe(
      `ALTER TABLE todos ADD COLUMN created_at TIMESTAMP DEFAULT 1735689600000000;
`,
    );
  });

  it("serializes Date defaults for add timestamp lens operations", () => {
    resetCollectedState();
    migrate("todos", {
      created_at: col.add().timestamp({ default: new Date("2025-01-01T00:00:00.000Z") }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(
      `ALTER TABLE todos ADD COLUMN created_at TIMESTAMP DEFAULT 1735689600000;
`,
    );
  });

  it("serializes Date backwards defaults for drop timestamp lens operations", () => {
    resetCollectedState();
    migrate("todos", {
      created_at: col.drop().timestamp({ backwardsDefault: new Date("2025-01-01T00:00:00.000Z") }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "bwd")).toBe(
      `ALTER TABLE todos ADD COLUMN created_at TIMESTAMP DEFAULT 1735689600000;
`,
    );
  });

  it("supports array defaults in migration SQL generation", () => {
    resetCollectedState();
    migrate("projects", {
      todos: col.add().string({ default: [] as unknown as string }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(`ALTER TABLE projects ADD COLUMN todos TEXT DEFAULT ARRAY[];
`);
  });

  it("supports adding array columns in lenses", () => {
    resetCollectedState();
    migrate("projects", {
      todos: col.add().array({ of: "UUID", default: [] }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(
      `ALTER TABLE projects ADD COLUMN todos UUID[] DEFAULT ARRAY[];
`,
    );
  });

  it("supports adding array columns with default values in lenses", () => {
    resetCollectedState();
    migrate("projects", {
      todos: col.add().array({ of: "UUID", default: ["123e4567-e89b-12d3-a456-426614174000"] }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(
      `ALTER TABLE projects ADD COLUMN todos UUID[] DEFAULT ARRAY['123e4567-e89b-12d3-a456-426614174000'];
`,
    );
  });

  it("supports dropping array columns with backward re-add", () => {
    resetCollectedState();
    migrate("projects", {
      todos: col.drop().array({ of: "UUID", backwardsDefault: [] }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(`ALTER TABLE projects DROP COLUMN todos;
`);
    expect(lensToSql(lens, "bwd")).toBe(
      `ALTER TABLE projects ADD COLUMN todos UUID[] DEFAULT ARRAY[];
`,
    );
  });

  it("supports adding enum columns in lenses", () => {
    resetCollectedState();
    migrate("tasks", {
      status: col.add().enum("todo", "done", { default: "todo" }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(
      `ALTER TABLE tasks ADD COLUMN status ENUM('done','todo') DEFAULT 'todo';
`,
    );
  });

  it("supports dropping enum columns with backward re-add", () => {
    resetCollectedState();
    migrate("tasks", {
      status: col.drop().enum("todo", "done", { backwardsDefault: "done" }),
    });
    const lens = getCollectedMigration()!;

    expect(lensToSql(lens, "fwd")).toBe(`ALTER TABLE tasks DROP COLUMN status;
`);
    expect(lensToSql(lens, "bwd")).toBe(
      `ALTER TABLE tasks ADD COLUMN status ENUM('done','todo') DEFAULT 'done';
`,
    );
  });
});
