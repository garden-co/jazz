import { spawnSync } from "node:child_process";
import { chmod, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { build, createMigration, pushMigration } from "./cli.js";

const dslPath = fileURLToPath(new URL("./dsl.ts", import.meta.url));
const typedAppPath = fileURLToPath(new URL("./typed-app.ts", import.meta.url));
const migrationsPath = fileURLToPath(new URL("./migrations.ts", import.meta.url));
const permissionsDslPath = fileURLToPath(new URL("./permissions/index.ts", import.meta.url));
// Bin integration tests run in a subprocess that loads dist/cli.js, so current.ts must import
// from the compiled dist to share the same dsl module instance (and thus _collectedSchema state).
const distDslPath = fileURLToPath(new URL("../dist/dsl.js", import.meta.url));
const distTypedAppPath = fileURLToPath(new URL("../dist/typed-app.js", import.meta.url));
const distPermissionsDslPath = fileURLToPath(
  new URL("../dist/permissions/index.js", import.meta.url),
);

const tempRoots: string[] = [];

afterEach(async () => {
  vi.unstubAllGlobals();
  await Promise.all(tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function createWorkspace(): Promise<{ root: string; schemaDir: string; jazzBin: string }> {
  const root = await mkdtemp(join(tmpdir(), "jazz-tools-cli-test-"));
  tempRoots.push(root);
  const schemaDir = join(root, "schema");
  await mkdir(schemaDir, { recursive: true });

  const jazzBin = join(root, "fake-jazz");
  await writeFile(jazzBin, "#!/bin/sh\nexit 0\n");
  await chmod(jazzBin, 0o755);

  return { root, schemaDir, jazzBin };
}

async function createFakeRustBin(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "jazz-tools-cli-rust-bin-"));
  tempRoots.push(root);

  const rustBin = join(root, "fake-jazz-tools");
  await writeFile(rustBin, "#!/bin/sh\nexit 0\n");
  await chmod(rustBin, 0o755);

  return rustBin;
}

function currentSchemaWithoutInlinePermissions(): string {
  return `
import { table, col } from ${JSON.stringify(dslPath)};

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  ownerId: col.string(),
});
`;
}

function currentSchemaWithInlinePermissions(): string {
  return `
import { table, col } from ${JSON.stringify(dslPath)};

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  ownerId: col.string(),
}, {
  permissions: {
    select: { type: "True" },
  },
});
`;
}

function permissionsSchema(appImportPath: string = "./app.js"): string {
  return `
import { definePermissions } from ${JSON.stringify(permissionsDslPath)};
import { app } from ${JSON.stringify(appImportPath)};

export default definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ owner_id: session.user_id }),
]);
`;
}

function rootSchemaWithoutInlinePermissions(
  dslImportPath: string = dslPath,
  typedAppImportPath: string = typedAppPath,
): string {
  return `
import { col } from ${JSON.stringify(dslImportPath)};
import { defineApp, type DefinedSchema, type TypedApp } from ${JSON.stringify(typedAppImportPath)};

const schemaDef = {
  projects: {
    name: col.string(),
  },
  todos: {
    title: col.string(),
    ownerId: col.string(),
  },
};

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);
`;
}

function rootSchemaWithComments(
  dslImportPath: string = dslPath,
  typedAppImportPath: string = typedAppPath,
): string {
  return `
import { col } from ${JSON.stringify(dslImportPath)};
import { defineApp, type DefinedSchema, type TypedApp } from ${JSON.stringify(typedAppImportPath)};

const schemaDef = {
  projects: {
    name: col.string(),
  },
  todos: {
    title: col.string(),
    ownerId: col.string(),
  },
  comments: {
    body: col.string(),
  },
};

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);
`;
}

function rootPermissionsSchema(
  appImportPath: string = "./schema.ts",
  importPath: string = permissionsDslPath,
): string {
  return `
import { definePermissions } from ${JSON.stringify(importPath)};
import { app } from ${JSON.stringify(appImportPath)};

export default definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ owner_id: session.user_id }),
]);
`;
}

function permissionsSchemaUnknownTable(): string {
  return `
export default {
  ghosts: {
    select: {
      using: { type: "True" },
    },
  },
};
`;
}

function permissionsSchemaMissingExport(): string {
  return `
export const nope = 42;
`;
}

function permissionsSchemaNamedExport(): string {
  return `
import { definePermissions } from ${JSON.stringify(permissionsDslPath)};
import { app } from "./app.js";

export const permissions = definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ owner_id: session.user_id }),
]);
`;
}

function permissionsSchemaInvalidShape(): string {
  return `
export default {
  todos: 123,
};
`;
}

// Bin integration variants — import from dist/dsl.js to share the module instance with dist/cli.js.
function binCurrentSchema(): string {
  return `
import { table, col } from ${JSON.stringify(distDslPath)};

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  ownerId: col.string(),
});
`;
}

function binSchemaWithMessagesAndCanvases(): string {
  return `
import { table, col } from ${JSON.stringify(distDslPath)};

table("messages", {
  content: col.string(),
  isPublic: col.boolean(),
});

table("canvases", {
  name: col.string(),
  isPublic: col.boolean(),
});
`;
}

function binMigrationDropIsPublicFromBothTables(): string {
  return `
import { migrate, col } from ${JSON.stringify(distDslPath)};

migrate("messages", {
  isPublic: col.drop().boolean({ backwardsDefault: false }),
});

migrate("canvases", {
  isPublic: col.drop().boolean({ backwardsDefault: false }),
});
`;
}

function binPermissionsSchema(appImportPath: string = "./app.js"): string {
  return `
import { definePermissions } from ${JSON.stringify(distPermissionsDslPath)};
import { app } from ${JSON.stringify(appImportPath)};

export default definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ owner_id: session.user_id }),
]);
`;
}

function currentSchemaWithComments(): string {
  return `
import { table, col } from ${JSON.stringify(distDslPath)};

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  ownerId: col.string(),
});

table("comments", {
  body: col.string(),
});
`;
}

function schemaWithMessagesAndCanvases(): string {
  return `
import { table, col } from ${JSON.stringify(dslPath)};

table("messages", {
  content: col.string(),
  isPublic: col.boolean(),
});

table("canvases", {
  name: col.string(),
  isPublic: col.boolean(),
});
`;
}

function migrationDropIsPublicFromBothTables(): string {
  return `
import { migrate, col } from ${JSON.stringify(dslPath)};

migrate("messages", {
  isPublic: col.drop().boolean({ backwardsDefault: false }),
});

migrate("canvases", {
  isPublic: col.drop().boolean({ backwardsDefault: false }),
});
`;
}

describe("cli build basic output", () => {
  it("generates app.ts even when current.sql already exists", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithoutInlinePermissions());
    await writeFile(join(schemaDir, "current.sql"), "-- stale");

    await build({ schemaDir, jazzBin });

    await readFile(join(schemaDir, "app.ts"), "utf8");
  });
});

describe("cli build root schema.ts mode", () => {
  it("generates schema/current.sql from root schema.ts without app.ts codegen", async () => {
    const { root, jazzBin } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await build({ schemaDir: root, jazzBin });

    const sql = await readFile(join(root, "schema", "current.sql"), "utf8");
    expect(sql).toContain("CREATE TABLE projects");
    expect(sql).toContain("CREATE TABLE todos");
    await expect(readFile(join(root, "schema", "app.ts"), "utf8")).rejects.toThrow();
  });

  it("finds root schema.ts when build is pointed at the default ./schema directory", async () => {
    const { root, schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await build({ schemaDir, jazzBin });

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain("CREATE TABLE todos");
  });

  it("updates schema/current.sql when root schema.ts changes after initial build", async () => {
    const { root, schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await build({ schemaDir, jazzBin });

    await writeFile(join(root, "schema.ts"), rootSchemaWithComments());
    await build({ schemaDir, jazzBin });

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain("CREATE TABLE comments");
  });
});

describe("cli migrations", () => {
  it("generates a typed migration stub from stored schema hashes", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const toHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith(`/schema/${fromHash}`)) {
        return new Response(
          JSON.stringify({
            todos: {
              columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
            },
          }),
          { status: 200 },
        );
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return new Response(
          JSON.stringify({
            todos: {
              columns: [
                { name: "title", column_type: { type: "Text" }, nullable: false },
                { name: "notes", column_type: { type: "Text" }, nullable: true },
              ],
            },
          }),
          { status: 200 },
        );
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const filePath = await createMigration({
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash,
      toHash,
    });

    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain("defineMigration");
    expect(generated).toContain(`fromHash: "${fromHash}"`);
    expect(generated).toContain(`toHash: "${toHash}"`);
    expect(generated).toContain('t.add("notes", { default: null });');
  });

  it("skips table add/drop steps when inferring a migration stub", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "abababababababababababababababababababababababababababababababab";
    const toHash = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith(`/schema/${fromHash}`)) {
        return new Response(
          JSON.stringify({
            todos: {
              columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
            },
            legacy_users: {
              columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
            },
          }),
          { status: 200 },
        );
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return new Response(
          JSON.stringify({
            todos: {
              columns: [
                { name: "title", column_type: { type: "Text" }, nullable: false },
                { name: "notes", column_type: { type: "Text" }, nullable: true },
              ],
            },
            users: {
              columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
            },
          }),
          { status: 200 },
        );
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const filePath = await createMigration({
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash,
      toHash,
    });

    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain('m.table("todos", (t) => {');
    expect(generated).toContain('t.add("notes", { default: null });');
    expect(generated).not.toContain("createTable");
    expect(generated).not.toContain("dropTable");
    expect(generated).not.toContain('"legacy_users"');
    expect(generated).not.toContain('"users"');
  });

  it("pushes a reviewed migration via the admin migrations endpoint", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await mkdir(migrationsDir, { recursive: true });

    const fromHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const toHash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const migrationPath = join(migrationsDir, `20260318-rename-${fromHash}-${toHash}.ts`);

    await writeFile(
      migrationPath,
      `
import { col } from ${JSON.stringify(dslPath)};
import { defineMigration } from ${JSON.stringify(migrationsPath)};

export default defineMigration({
  fromHash: ${JSON.stringify(fromHash)},
  toHash: ${JSON.stringify(toHash)},
  from: {
    users: {
      email: col.string(),
    },
  },
  to: {
    users: {
      email_address: col.string(),
    },
  },
  migrate: (m) => {
    m.table("users", (t) => {
      t.rename("email", "email_address");
    });
  },
});
`,
    );

    const fetchMock = vi.fn(async (_input: string, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      expect(body.fromHash).toBe(fromHash);
      expect(body.toHash).toBe(toHash);
      expect(body.forwardSql).toContain("ALTER TABLE users RENAME COLUMN email TO email_address;");
      return new Response(JSON.stringify({ ok: true }), { status: 201 });
    });
    vi.stubGlobal("fetch", fetchMock);

    await pushMigration({
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash,
      toHash,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

describe("cli build migration SQL generation", () => {
  it("generates DROP COLUMN for every table when migrate() is called on multiple tables", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), schemaWithMessagesAndCanvases());
    await writeFile(
      join(schemaDir, "migration_v1_v2_aaaaaaaaaaaa_bbbbbbbbbbbb.ts"),
      migrationDropIsPublicFromBothTables(),
    );

    await build({ schemaDir, jazzBin });

    const fwdSql = await readFile(
      join(schemaDir, "migration_v1_v2_fwd_aaaaaaaaaaaa_bbbbbbbbbbbb.sql"),
      "utf8",
    );
    const bwdSql = await readFile(
      join(schemaDir, "migration_v1_v2_bwd_aaaaaaaaaaaa_bbbbbbbbbbbb.sql"),
      "utf8",
    );

    expect(fwdSql).toContain("ALTER TABLE messages DROP COLUMN isPublic;");
    expect(fwdSql).toContain("ALTER TABLE canvases DROP COLUMN isPublic;");
    expect(bwdSql).toContain("ALTER TABLE messages ADD COLUMN isPublic BOOLEAN DEFAULT FALSE;");
    expect(bwdSql).toContain("ALTER TABLE canvases ADD COLUMN isPublic BOOLEAN DEFAULT FALSE;");
  });
});

describe("cli build permissions generation", () => {
  it("loads root permissions.ts from app root and creates the stub alongside it", async () => {
    const { root, jazzBin } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema("./schema.ts"));

    await build({ schemaDir: root, jazzBin });

    const sql = await readFile(join(root, "schema", "current.sql"), "utf8");
    const permissionsTest = await readFile(join(root, "permissions.test.ts"), "utf8");

    expect(sql).toContain(
      "CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);",
    );
    expect(permissionsTest).toContain("Permissions test starter.");
    await expect(readFile(join(root, "schema", "app.ts"), "utf8")).rejects.toThrow();
  });

  it("loads permissions.ts, merges policies, and creates permissions.test.ts stub", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithoutInlinePermissions());
    await writeFile(join(schemaDir, "permissions.ts"), permissionsSchema());

    await build({ schemaDir, jazzBin });

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    const appTs = await readFile(join(schemaDir, "app.ts"), "utf8");
    const permissionsTest = await readFile(join(schemaDir, "permissions.test.ts"), "utf8");

    expect(sql).toContain(
      "CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);",
    );
    expect(appTs).toContain('"policies"');
    expect(appTs).toContain('"type": "SessionRef"');
    expect(appTs).toContain('"column": "owner_id"');
    expect(permissionsTest).toContain("Permissions test starter.");
  });

  it("loads permissions.ts when it imports app from ./app.ts", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithoutInlinePermissions());
    await writeFile(join(schemaDir, "permissions.ts"), permissionsSchema("./app.ts"));

    await build({ schemaDir, jazzBin });

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain(
      "CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);",
    );
  });

  it("loads permissions.ts when it imports app from ./app", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithoutInlinePermissions());
    await writeFile(join(schemaDir, "permissions.ts"), permissionsSchema("./app"));

    await build({ schemaDir, jazzBin });

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain(
      "CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);",
    );
  });

  it("fails when current.ts uses inline table permissions", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithInlinePermissions());

    await expect(build({ schemaDir, jazzBin })).rejects.toThrow(
      /inline table permissions are no longer supported/i,
    );
  });

  it("does not overwrite an existing permissions.test.ts file", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithoutInlinePermissions());
    await writeFile(join(schemaDir, "permissions.ts"), permissionsSchema());
    await writeFile(join(schemaDir, "permissions.test.ts"), "// keep-existing-test\n");

    await build({ schemaDir, jazzBin });

    const permissionsTest = await readFile(join(schemaDir, "permissions.test.ts"), "utf8");
    expect(permissionsTest).toBe("// keep-existing-test\n");
  });

  it("fails when permissions.ts has no default/permissions export", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithoutInlinePermissions());
    await writeFile(join(schemaDir, "permissions.ts"), permissionsSchemaMissingExport());

    await expect(build({ schemaDir, jazzBin })).rejects.toThrow(/missing permissions export/i);
  });

  it("fails when permissions.ts references unknown tables", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithoutInlinePermissions());
    await writeFile(join(schemaDir, "permissions.ts"), permissionsSchemaUnknownTable());

    await expect(build({ schemaDir, jazzBin })).rejects.toThrow(
      /permissions\.ts defines permissions for unknown table\(s\): ghosts/i,
    );
  });

  it("accepts named permissions export for transitional ergonomics", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithoutInlinePermissions());
    await writeFile(join(schemaDir, "permissions.ts"), permissionsSchemaNamedExport());

    await build({ schemaDir, jazzBin });

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain(
      "CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);",
    );
  });

  it("fails when permissions.ts export shape is invalid", async () => {
    const { schemaDir, jazzBin } = await createWorkspace();
    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithoutInlinePermissions());
    await writeFile(join(schemaDir, "permissions.ts"), permissionsSchemaInvalidShape());

    await expect(build({ schemaDir, jazzBin })).rejects.toThrow(/invalid permissions export/i);
  });
});

// Integration test: exercises the bin/jazz-tools.js entry point, which applies extra
// logic on top of build() to decide whether to invoke the TS CLI at all.
const binPath = fileURLToPath(new URL("../bin/jazz-tools.js", import.meta.url));

function runBinBuild(schemaDir: string, rustBin: string): void {
  const result = spawnSync(
    process.execPath,
    [binPath, "build", "--schema-dir", schemaDir, "--rust-bin", rustBin],
    {
      stdio: "inherit",
    },
  );

  expect(result.status).toBe(0);
}

describe("bin integration", () => {
  it("generates current.sql on first build (no current.sql)", async () => {
    const { schemaDir } = await createWorkspace();
    const rustBin = await createFakeRustBin();
    await writeFile(join(schemaDir, "current.ts"), binCurrentSchema());

    runBinBuild(schemaDir, rustBin);

    await readFile(join(schemaDir, "current.sql"), "utf8");
  });

  it("generates app.ts on first build (no current.sql)", async () => {
    const { schemaDir } = await createWorkspace();
    const rustBin = await createFakeRustBin();
    await writeFile(join(schemaDir, "current.ts"), binCurrentSchema());

    runBinBuild(schemaDir, rustBin);

    await readFile(join(schemaDir, "app.ts"), "utf8");
  });

  it("regenerates current.sql and app.ts when current.sql already exists", async () => {
    // Regression: bin skips the TS CLI step when current.sql is present,
    // so neither file is updated on subsequent builds.
    const { schemaDir } = await createWorkspace();
    const rustBin = await createFakeRustBin();
    await writeFile(join(schemaDir, "current.ts"), binCurrentSchema());
    await writeFile(join(schemaDir, "current.sql"), "-- stale");

    runBinBuild(schemaDir, rustBin);

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain("CREATE TABLE");
    await readFile(join(schemaDir, "app.ts"), "utf8");
  });

  // bootstrap → change current.ts → rebuild

  it("updates current.sql when current.ts changes after initial build", async () => {
    const { schemaDir } = await createWorkspace();
    const rustBin = await createFakeRustBin();
    await writeFile(join(schemaDir, "current.ts"), binCurrentSchema());
    runBinBuild(schemaDir, rustBin);

    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithComments());
    runBinBuild(schemaDir, rustBin);

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain("CREATE TABLE comments");
  });

  it("updates app.ts when current.ts changes after initial build", async () => {
    const { schemaDir } = await createWorkspace();
    const rustBin = await createFakeRustBin();
    await writeFile(join(schemaDir, "current.ts"), binCurrentSchema());
    runBinBuild(schemaDir, rustBin);

    await writeFile(join(schemaDir, "current.ts"), currentSchemaWithComments());
    runBinBuild(schemaDir, rustBin);

    const appTs = await readFile(join(schemaDir, "app.ts"), "utf8");
    expect(appTs).toContain("comments");
  });

  it("generates migration SQL from stub on rebuild when current.sql already exists", async () => {
    const { schemaDir } = await createWorkspace();
    const rustBin = await createFakeRustBin();
    await writeFile(join(schemaDir, "current.ts"), binSchemaWithMessagesAndCanvases());
    runBinBuild(schemaDir, rustBin);

    await writeFile(
      join(schemaDir, "migration_v1_v2_aaaaaaaaaaaa_bbbbbbbbbbbb.ts"),
      binMigrationDropIsPublicFromBothTables(),
    );
    runBinBuild(schemaDir, rustBin);

    await readFile(join(schemaDir, "migration_v1_v2_fwd_aaaaaaaaaaaa_bbbbbbbbbbbb.sql"), "utf8");
    await readFile(join(schemaDir, "migration_v1_v2_bwd_aaaaaaaaaaaa_bbbbbbbbbbbb.sql"), "utf8");
  });

  it("loads permissions.ts that imports ./app via bin entry point", async () => {
    const { schemaDir } = await createWorkspace();
    const rustBin = await createFakeRustBin();
    await writeFile(join(schemaDir, "current.ts"), binCurrentSchema());
    await writeFile(join(schemaDir, "permissions.ts"), binPermissionsSchema("./app"));

    runBinBuild(schemaDir, rustBin);

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain(
      "CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);",
    );
  });

  it("runs the TypeScript build for a root schema.ts when the bin is pointed at ./schema", async () => {
    const { root, schemaDir } = await createWorkspace();
    const rustBin = await createFakeRustBin();
    await writeFile(
      join(root, "schema.ts"),
      rootSchemaWithoutInlinePermissions(distDslPath, distTypedAppPath),
    );

    runBinBuild(schemaDir, rustBin);

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain("CREATE TABLE todos");
    await expect(readFile(join(schemaDir, "app.ts"), "utf8")).rejects.toThrow();
  });

  it("loads root permissions.ts that imports ./schema via the bin entry point", async () => {
    const { root, schemaDir } = await createWorkspace();
    const rustBin = await createFakeRustBin();
    await writeFile(
      join(root, "schema.ts"),
      rootSchemaWithoutInlinePermissions(distDslPath, distTypedAppPath),
    );
    await writeFile(
      join(root, "permissions.ts"),
      rootPermissionsSchema("./schema", distPermissionsDslPath),
    );

    runBinBuild(schemaDir, rustBin);

    const sql = await readFile(join(schemaDir, "current.sql"), "utf8");
    expect(sql).toContain(
      "CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);",
    );
  });
});
