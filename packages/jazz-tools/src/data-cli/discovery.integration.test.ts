import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { publishStoredSchema } from "../runtime/schema-fetch.js";
import { parseDataArgs, runDataQuery } from "./command.js";
import { compileSql, parseSql } from "./sql.js";

const app = s.defineApp({
  notes: s.table({
    text: s.string(),
    done: s.boolean().default(false),
    ownerId: s.ref("owners").optional(),
    labels: s.array(s.string()).optional(),
  }),
  owners: s.table({ name: s.string() }),
});
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
  policy.owners.allowRead.never();
  policy.owners.allowInsert.never();
  policy.owners.allowUpdate.never();
  policy.owners.allowDelete.never();
});
const wrapper = fileURLToPath(new URL("../../bin/jazz-tools.js", import.meta.url));
async function cli(args: string[], env: NodeJS.ProcessEnv = {}) {
  return new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    const child = spawn(process.execPath, [wrapper, ...args], {
      env: {
        ...process.env,
        JAZZ_APP_ID: "",
        JAZZ_SERVER_URL: "",
        JAZZ_JWT_TOKEN: "",
        JAZZ_BACKEND_SECRET: "",
        JAZZ_ADMIN_SECRET: "",
        ...env,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.setEncoding("utf8").on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (code) => resolve({ code, stdout, stderr }));
  });
}

describe("schema discovery and sql commands", () => {
  it("discovers deployed tables and types with admin-only auth, then reads and writes via sql", async () => {
    const appId = randomUUID();
    const server = await startLocalJazzServer({ appId, inMemory: true });
    try {
      const remote = ["--app-id", appId, "--server-url", server.url];
      const env = { JAZZ_ADMIN_SECRET: server.adminSecret };
      const missing = await cli(["schema", "tables", ...remote], env);
      expect(missing.code).toBe(1);
      expect(missing.stderr).toContain("no permissions head");
      await deploy({
        appId,
        serverUrl: server.url,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      // Another stored version must not become "current" merely by being newer.
      const unpublishedApp = s.defineApp({ unrelated: s.table({ text: s.string() }) });
      await publishStoredSchema(server.url, {
        appId,
        adminSecret: server.adminSecret,
        schema: unpublishedApp.wasmSchema,
      });
      for (const command of [
        ["schema", "tables"],
        ["schema", "list"],
        ["sql", "SHOW TABLES"],
        ["data", "query", "--sql", "show tables"],
      ]) {
        const result = await cli([...command, ...remote, "--format=json"], env);
        expect(result.stderr).toBe("");
        expect(result.code).toBe(0);
        expect(JSON.parse(result.stdout)).toEqual([{ table: "notes" }, { table: "owners" }]);
      }
      const description = await cli(
        ["schema", "describe", "notes", ...remote, "--format=json"],
        env,
      );
      expect(description.stderr).toBe("");
      expect(description.code).toBe(0);
      const columns = JSON.parse(description.stdout);
      expect(columns).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ column: "id", type: "UUID", nullable: false, generated: true }),
          expect.objectContaining({
            column: "text",
            type: "TEXT",
            hasDefault: false,
            default: null,
          }),
          expect.objectContaining({
            column: "done",
            type: "BOOLEAN",
            hasDefault: true,
            default: false,
          }),
          expect.objectContaining({
            column: "ownerId",
            type: "UUID",
            nullable: true,
            references: "owners",
          }),
          expect.objectContaining({
            column: "labels",
            type: "TEXT[]",
            typeDefinition: { type: "Array", element: { type: "Text" } },
          }),
        ]),
      );
      const sqlDescription = await cli(["sql", "DESCRIBE notes", ...remote, "--format=json"], env);
      expect(sqlDescription.code).toBe(0);
      expect(JSON.parse(sqlDescription.stdout)).toEqual(columns);
      const unknown = await cli(["schema", "describe", "absent", ...remote], env);
      expect(unknown.code).toBe(1);
      expect(unknown.stderr).toContain("Unknown table");
      const denied = await cli(["schema", "tables", ...remote], {
        JAZZ_ADMIN_SECRET: server.backendSecret,
      });
      expect(denied.code).toBe(1);
      expect(denied.stderr).toContain("401");
      const adminRead = await cli(["sql", "SELECT * FROM notes", ...remote], env);
      expect(adminRead.code).toBe(1);
      expect(adminRead.stderr).toContain("catalogue access only");
      const dataEnv = { ...env, JAZZ_BACKEND_SECRET: server.backendSecret };
      const insertSql = "INSERT INTO owners (name) VALUES ('CLI discovery test')";
      const readOnly = await cli(["sql", insertSql, ...remote], dataEnv);
      expect(readOnly.code).toBe(1);
      expect(readOnly.stderr).toContain("--write");
      const written = await cli(["sql", insertSql, ...remote, "--write", "--format=json"], dataEnv);
      expect(written.stderr).toBe("");
      expect(written.code).toBe(0);
      expect(JSON.parse(written.stdout)[0]).toMatchObject({ affectedRows: 1 });
      const selected = await cli(
        ["sql", "SELECT name FROM owners", ...remote, "--format=json"],
        dataEnv,
      );
      expect(selected.stderr).toBe("");
      expect(selected.code).toBe(0);
      expect(JSON.parse(selected.stdout)).toEqual([{ name: "CLI discovery test" }]);
    } finally {
      await server.stop();
    }
  }, 60000);

  it("inspects local schemas without app/server/data credentials and quotes describe names safely", async () => {
    const schemaDir = await mkdtemp(join(tmpdir(), "jazz-discovery-"));
    try {
      await writeFile(
        join(schemaDir, "schema.ts"),
        `import { schema as s } from "jazz-tools";
export const app = s.defineApp({ notes: s.table({ text: s.string() }) });\n`,
      );
      const args = ["--schema-dir", schemaDir, "--format=json"];
      const tables = await cli(["schema", "tables", ...args]);
      expect(tables.stderr).toBe("");
      expect(tables.code).toBe(0);
      expect(JSON.parse(tables.stdout)).toEqual([{ table: "notes" }]);
      const columns = await cli(["sql", "DESCRIBE notes", ...args]);
      expect(columns.code).toBe(0);
      expect(JSON.parse(columns.stdout).map((column: { column: string }) => column.column)).toEqual(
        ["id", "text"],
      );
      const escaped = parseDataArgs(['notes"; DELETE FROM notes', ...args], {}, {}, "describe");
      await expect(runDataQuery(escaped)).rejects.toThrow("Unknown table");
      const noAuth = parseDataArgs(["--file", join(schemaDir, "read.sql"), ...args], {}, {}, "sql");
      await writeFile(join(schemaDir, "read.sql"), "SELECT * FROM notes");
      await expect(runDataQuery(noAuth)).rejects.toThrow("catalogue access only");
    } finally {
      await rm(schemaDir, { recursive: true, force: true });
    }
  });

  it("does not allow schema inspection to hide another statement or a mutation", () => {
    expect(() => parseSql("SHOW TABLES; DELETE FROM notes", true)).toThrow("multiple statements");
    expect(() => parseSql("DESCRIBE notes WHERE id = 'x'")).toThrow("unsupported clause");
    expect(() =>
      parseDataArgs(["--sql", "DELETE FROM notes", "--write"], {}, {}, "tables"),
    ).toThrow("Schema discovery accepts no");
    expect(() =>
      parseDataArgs(["SELECT * FROM notes", "--sql", "SHOW TABLES"], {}, {}, "sql"),
    ).toThrow("exactly one");
    expect(() => parseDataArgs(["app-one", "--app-id", "app-two"], {}, {}, "data")).toThrow(
      "not both",
    );
    expect(compileSql(parseSql("SHOW TABLES"), s.defineApp({}).wasmSchema)).toMatchObject({
      result: { columns: ["table"], rows: [] },
    });
  });

  it.each([["sql"], ["schema", "tables"], ["schema", "describe"]])(
    "shows discovery help for %j",
    async (...command) => {
      const result = await cli([...command, "--help"]);
      expect(result.code).toBe(0);
      expect(result.stdout).toContain("--app-id");
      expect(result.stdout).toContain("schema describe");
    },
  );
});
