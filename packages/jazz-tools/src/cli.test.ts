import { chmod, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { build } from "./cli.js";

const dslPath = fileURLToPath(new URL("./dsl.ts", import.meta.url));
const permissionsDslPath = fileURLToPath(new URL("./permissions/index.ts", import.meta.url));

const tempRoots: string[] = [];

afterEach(async () => {
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

function currentSchemaWithoutInlinePermissions(): string {
  return `
import { table, col } from ${JSON.stringify(dslPath)};

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  owner_id: col.string(),
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
  owner_id: col.string(),
}, {
  permissions: {
    select: { type: "True" },
  },
});
`;
}

function permissionsSchema(): string {
  return `
import { definePermissions } from ${JSON.stringify(permissionsDslPath)};
import { app } from "./app";

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
import { app } from "./app";

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

describe("cli build permissions generation", () => {
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
