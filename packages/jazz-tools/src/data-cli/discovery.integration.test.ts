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
import { parseSql } from "./sql.js";

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

async function cli(args: string[], env: Record<string, string | undefined> = {}) {
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
  it("inspects a stored schema with admin-only auth, then reads and writes via sql", async () => {
    const appId = randomUUID();
    const server = await startLocalJazzServer({ appId, inMemory: true });
    try {
      const remote = ["--app-id", appId, "--server-url", server.url];
      const env = { JAZZ_ADMIN_SECRET: server.adminSecret };
      // `schema tables` is local by default, so a remote read must be explicit.
      const missing = await cli(["schema", "tables", "--schema-hash", "current", ...remote], env);
      expect(missing.code).toBe(2);
      expect(missing.stderr).toContain("SCHEMA_NOT_FOUND");
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
      // One task, one canonical path, through both the discovery and sql nouns.
      for (const command of [
        ["schema", "tables", "--schema-hash", "current"],
        ["sql", "SHOW TABLES"],
      ]) {
        const result = await cli([...command, ...remote, "--format=json"], env);
        expect(result.stderr).toBe("");
        expect(result.code).toBe(0);
        expect(JSON.parse(result.stdout)).toEqual([{ table: "notes" }, { table: "owners" }]);
      }

      const description = await cli(
        ["schema", "describe", "notes", "--schema-hash", "current", ...remote, "--format=json"],
        env,
      );
      expect(description.stderr).toBe("");
      expect(description.code).toBe(0);
      const columns = JSON.parse(description.stdout);
      expect(columns).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            column: "id",
            type: "UUID",
            nullable: false,
            meta: expect.objectContaining({ generated: true }),
          }),
          expect.objectContaining({
            column: "text",
            type: "TEXT",
            default: null,
            meta: expect.objectContaining({ hasDefault: false }),
          }),
          expect.objectContaining({
            column: "done",
            type: "BOOLEAN",
            default: false,
            meta: expect.objectContaining({ hasDefault: true }),
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
            meta: expect.objectContaining({
              typeDefinition: { type: "Array", element: { type: "Text" } },
            }),
          }),
        ]),
      );
      // The same information through the SQL surface, and every emitted key is
      // declared by the result's own column list.
      const sqlDescription = await cli(["sql", "DESCRIBE notes", ...remote, "--format=json"], env);
      expect(sqlDescription.code).toBe(0);
      expect(JSON.parse(sqlDescription.stdout)).toEqual(columns);
      const tableDescription = await cli(
        ["sql", "DESCRIBE notes", ...remote, "--format=table"],
        env,
      );
      const header = tableDescription.stdout.split("\n")[0]!;
      for (const key of Object.keys(columns[0])) expect(header).toContain(key);

      const unknown = await cli(["sql", "DESCRIBE absent", ...remote, "--format=json"], env);
      expect(unknown.code).toBe(2);
      expect(JSON.parse(unknown.stderr)).toMatchObject({
        error: { code: "UNKNOWN_TABLE", exitCode: 2 },
      });

      // A backend secret is not an admin secret: the catalogue refuses it (401),
      // which is a credential problem (3), not a policy refusal (4).
      const denied = await cli(["schema", "tables", "--schema-hash", "current", ...remote], {
        JAZZ_ADMIN_SECRET: server.backendSecret,
      });
      expect(denied.code).toBe(3);
      expect(denied.stderr).toContain("401");
      const adminRead = await cli(["sql", "SELECT * FROM notes", ...remote, "--format=json"], env);
      expect(adminRead.code).toBe(3);
      expect(JSON.parse(adminRead.stderr)).toMatchObject({ error: { code: "AUTH_REQUIRED" } });

      const dataEnv = { ...env, JAZZ_BACKEND_SECRET: server.backendSecret };
      const inserted = await cli(
        [
          "sql",
          "INSERT INTO owners (name) VALUES ('CLI discovery test')",
          ...remote,
          "--write",
          "--format=json",
        ],
        dataEnv,
      );
      expect(inserted.stderr).toBe("");
      expect(inserted.code).toBe(0);
      expect(JSON.parse(inserted.stdout)[0]).toMatchObject({ affectedRows: 1, atomic: false });
      const selected = await cli(
        ["sql", "SELECT name FROM owners", ...remote, "--format=json"],
        dataEnv,
      );
      expect(selected.stderr).toBe("");
      expect(selected.code).toBe(0);
      expect(JSON.parse(selected.stdout)).toEqual([{ name: "CLI discovery test" }]);
      // A stray positional is SQL, never an app id, and a self-proved local
      // schema never needs the catalogue.
      const localInsert = await cli(
        [
          "sql",
          "INSERT INTO owners (name) VALUES ('local')",
          "--write",
          "--schema-dir",
          ".",
          "--app-id",
          appId,
          "--server-url",
          server.url,
          "--backend-secret",
          server.backendSecret,
          "--format=json",
        ],
        { JAZZ_ADMIN_SECRET: "" },
      );
      expect(localInsert.code).toBe(1);
      expect(localInsert.stderr).toContain("schema.ts");
    } finally {
      await server.stop();
    }
  }, 60000);

  it("inspects local schemas without app, server, or credentials and quotes describe names safely", async () => {
    const schemaDir = await mkdtemp(join(tmpdir(), "jazz-discovery-"));
    try {
      await writeFile(
        join(schemaDir, "schema.ts"),
        `import { schema as s } from "jazz-tools";\nexport const app = s.defineApp({ notes: s.table({ text: s.string() }) });\n`,
      );
      const args = ["--schema-dir", schemaDir, "--format=json"];
      const tables = await cli(["schema", "tables", ...args]);
      expect(tables.stderr).toBe("");
      expect(tables.code).toBe(0);
      expect(JSON.parse(tables.stdout)).toEqual([{ table: "notes" }]);
      const identity = ["--app-id", "unused", "--server-url", "http://127.0.0.1:1"];
      const columns = await cli(["sql", "DESCRIBE notes", ...args, ...identity]);
      expect(columns.code).toBe(0);
      expect(JSON.parse(columns.stdout).map((column: { column: string }) => column.column)).toEqual(
        ["id", "text"],
      );
      // Local discovery is offline in both modes, with no credentials at all.
      for (const command of [
        ["schema", "tables", "--schema-dir", schemaDir],
        ["sql", "SHOW TABLES", "--schema-dir", schemaDir],
      ]) {
        const offline = await cli([...command, "--format=json"]);
        expect(offline.stderr).toBe("");
        expect(offline.code).toBe(0);
      }
      const escaped = parseDataArgs(['notes"; DELETE FROM notes', ...args], {}, {}, "describe");
      await expect(runDataQuery(escaped)).rejects.toThrow("Unknown table");
      const noAuth = parseDataArgs(
        [
          "--file",
          join(schemaDir, "read.sql"),
          ...args,
          "--app-id",
          "a",
          "--server-url",
          "http://127.0.0.1:1",
        ],
        {},
        {},
        "sql",
      );
      await writeFile(join(schemaDir, "read.sql"), "SELECT * FROM notes");
      await expect(runDataQuery(noAuth)).rejects.toThrow("catalogue access only");
      // `--explain` resolves the local schema and never needs an app or a server.
      const explained = await cli([
        "sql",
        "SELECT * FROM notes",
        "--schema-dir",
        schemaDir,
        "--explain",
        "--format=json",
      ]);
      expect(explained.stderr).toBe("");
      expect(explained.code).toBe(0);
      expect(JSON.parse(explained.stdout)).toMatchObject({
        statement: "select",
        table: "notes",
        projection: ["id", "text"],
        executes: false,
        connects: false,
        schemaSource: expect.stringContaining("local:"),
      });
    } finally {
      await rm(schemaDir, { recursive: true, force: true });
    }
  });

  it("does not let schema inspection hide another statement or a mutation", () => {
    expect(() => parseSql("SHOW TABLES; DELETE FROM notes", { write: true })).toThrow(
      "multiple statements",
    );
    expect(() => parseSql("DESCRIBE notes WHERE id = 'x'")).toThrow("unsupported clause");
    expect(() =>
      parseDataArgs(["--sql", "DELETE FROM notes", "--write"], {}, {}, "tables"),
    ).toThrow("Schema discovery accepts no");
    expect(() => parseDataArgs(["--write"], {}, {}, "describe")).toThrow("read-only");
  });

  it.each([["sql"], ["schema", "tables"], ["schema", "describe"]])(
    "shows discovery help for %j",
    async (...command) => {
      const result = await cli([...command, "--help"]);
      expect(result.code).toBe(0);
      expect(result.stdout).toContain("--app-id");
      expect(result.stdout).toContain("Exit codes");
    },
  );

  it("answers --capabilities without credentials or a schema", async () => {
    const result = await cli(["sql", "--capabilities", "--format=json"]);
    expect(result.stderr).toBe("");
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ version: 1, statements: expect.any(Array) });
  });
});
