import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer, startTestJwtIssuer } from "../testing/index.js";
import { fetchSchemaHashes } from "../runtime/schema-fetch.js";
import { compileSql, executeSql, parseSql } from "./sql.js";
import { parseDataArgs, resolveDataOptions, runDataQuery, type DataFlags } from "./command.js";

const app = s.defineApp({
  notes: s.table({
    text: s.string(),
    rank: s.int(),
    count: s.bigint().optional(),
    done: s.boolean().default(false),
  }),
  private_notes: s.table({ text: s.string() }),
});
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
  policy.private_notes.allowRead.never();
  policy.private_notes.allowInsert.never();
  policy.private_notes.allowUpdate.never();
  policy.private_notes.allowDelete.never();
});
const wrapper = fileURLToPath(new URL("../../bin/jazz-tools.js", import.meta.url));
const compileContext = { schemaHash: "test", schemaSource: "local:test" };

async function cli(args: string[], env: Record<string, string | undefined>, input?: string | null) {
  return await new Promise<{ code: number | null; stdout: string; stderr: string }>(
    (resolve, reject) => {
      const child = spawn(process.execPath, [wrapper, ...args], {
        env: {
          ...process.env,
          JAZZ_JWT_TOKEN: "",
          JAZZ_BACKEND_SECRET: "",
          JAZZ_ADMIN_SECRET: "",
          ...env,
        },
        stdio: ["pipe", "pipe", "pipe"],
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
      if (input !== null) child.stdin.end(input);
    },
  );
}

function sqlFlags(
  args: string[],
  defaults: { appId?: string; serverUrl?: string },
  env: Record<string, string | undefined>,
): DataFlags {
  return parseDataArgs(args, defaults, env, "sql");
}

describe("sql command against a Jazz server", () => {
  it("round-trips SQL through real query and mutation APIs, catalogue, and npm wrapper", async () => {
    const appId = randomUUID();
    const server = await startLocalJazzServer({ appId, inMemory: true });
    const session = await createJazzSession({
      app,
      appId,
      serverUrl: server.url,
      driver: { type: "memory" },
      initial: { backendSecret: server.backendSecret },
    });
    const schemaDir = await mkdtemp(join(tmpdir(), "jazz-data-cli-"));
    try {
      await deploy({
        appId,
        serverUrl: server.url,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const db = session.getSnapshot().client!.db;
      const sql = (text: string, write = false) =>
        executeSql(db, compileSql(parseSql(text, { write }), app.wasmSchema, compileContext));
      const inserted = await sql(
        "INSERT INTO notes (text, rank, count) VALUES ('O''Brien; -- literal', 1, 9223372036854775807)",
        true,
      );
      const id = inserted.rows[0]!.id as string;
      expect(inserted.rows[0]).toMatchObject({
        operation: "insert",
        affectedRows: 1,
        id: expect.any(String),
        idSource: "server",
        txId: expect.any(String),
        atomic: false,
        safeToRetry: false,
      });
      await db.insert(app.notes, { text: "second", rank: 2, count: null }).wait({ tier: "global" });
      await db.insert(app.notes, { text: "third", rank: 3, count: null }).wait({ tier: "global" });
      expect(
        (
          await sql(
            "SELECT text, rank FROM notes WHERE rank >= 1 AND rank <= 3 ORDER BY rank DESC LIMIT 1 OFFSET 1",
          )
        ).rows,
      ).toEqual([{ text: "second", rank: 2 }]);
      expect((await sql("SELECT text FROM notes WHERE count IS NOT NULL")).rows).toEqual([
        { text: "O'Brien; -- literal" },
      ]);
      expect((await sql("SELECT count FROM notes WHERE count = 9223372036854775807")).rows).toEqual(
        [{ count: 9223372036854775807n }],
      );
      expect((await sql("SELECT * FROM notes LIMIT 0")).rows).toEqual([]);
      expect(
        (await sql("SELECT rank FROM notes WHERE count IS NULL ORDER BY rank ASC OFFSET 1")).rows,
      ).toEqual([{ rank: 3 }]);
      expect(
        (await sql(`UPDATE notes SET text = 'changed', done = TRUE WHERE id = '${id}'`, true))
          .rows[0],
      ).toMatchObject({ affectedRows: 1, atomic: false });
      expect(await db.one(app.notes.where({ id }), { tier: "remote" })).toMatchObject({
        text: "changed",
        done: true,
      });

      // A seeded id means a retry cannot duplicate the row: the engine refuses
      // the second write, which is how a caller learns the first one committed.
      const seededFirst = await sql(
        "INSERT INTO notes (text, rank, count) VALUES ('seeded', 9, NULL) WITH ID SEED 'retry-key'",
        true,
      );
      expect(seededFirst.rows[0]).toMatchObject({
        idSource: "client",
        safeToRetry: true,
        affectedRows: 1,
      });
      const seededId = seededFirst.rows[0]!.id as string;
      await expect(
        sql(
          "INSERT INTO notes (text, rank, count) VALUES ('seeded', 9, NULL) WITH ID SEED 'retry-key'",
          true,
        ),
      ).rejects.toThrow(/already exists/i);
      expect((await sql("SELECT text, rank FROM notes WHERE rank = 9")).rows).toEqual([
        { text: "seeded", rank: 9 },
      ]);
      expect(await db.one(app.notes.where({ id: seededId }), { tier: "remote" })).toMatchObject({
        text: "seeded",
      });

      const { hashes } = await fetchSchemaHashes(server.url, {
        appId,
        adminSecret: server.adminSecret,
      });
      const hash = hashes[0]!;
      const env = {
        JAZZ_APP_ID: appId,
        JAZZ_SERVER_URL: server.url,
        JAZZ_BACKEND_SECRET: server.backendSecret,
        JAZZ_ADMIN_SECRET: server.adminSecret,
      };
      const remote = ["--schema-hash", hash];
      expect(
        (
          await runDataQuery(
            sqlFlags(
              ["SELECT text FROM notes WHERE rank = 1", ...remote],
              { appId, serverUrl: server.url },
              env,
            ),
          )
        ).result,
      ).toMatchObject({ rows: [{ text: "changed" }] });
      const selected = await cli(
        ["sql", "--file", "-", ...remote, "--format", "jsonl"],
        env,
        "SELECT text FROM notes ORDER BY rank DESC LIMIT 1;",
      );
      expect(selected.stderr).toBe("");
      expect(selected.code).toBe(0);
      expect(JSON.parse(selected.stdout)).toEqual({ text: "seeded" });

      const readOnly = await cli(
        ["sql", "INSERT INTO notes (text, rank) VALUES ('forbidden', 4)", ...remote],
        env,
      );
      expect(readOnly.code).toBe(2);
      expect(readOnly.stdout).toBe("");
      expect(readOnly.stderr).toContain("READ_ONLY");

      const written = await cli(
        [
          "sql",
          "INSERT INTO notes (text, rank) VALUES ('from CLI', 4)",
          ...remote,
          "--write",
          "--format=json",
        ],
        env,
      );
      expect(written.stderr).toBe("");
      expect(written.code).toBe(0);
      expect(JSON.parse(written.stdout)[0]).toMatchObject({ affectedRows: 1, operation: "insert" });

      // `--explain` resolves the deployed schema and reports the plan offline.
      const explained = await cli(
        ["sql", `DELETE FROM notes WHERE id = '${id}'`, ...remote, "--explain", "--format=json"],
        env,
      );
      expect(explained.stderr).toBe("");
      expect(explained.code).toBe(0);
      expect(JSON.parse(explained.stdout)).toMatchObject({
        statement: "delete",
        table: "notes",
        id,
        schemaHash: hash,
        executes: false,
        connects: false,
        atomic: false,
      });

      // `--verbose` writes the resolved context to stderr and keeps stdout clean.
      const verbose = await cli(
        ["sql", "SELECT text FROM notes WHERE rank = 4", ...remote, "--verbose", "--format=json"],
        env,
      );
      expect(JSON.parse(verbose.stdout)).toEqual([{ text: "from CLI" }]);
      expect(JSON.parse(verbose.stderr.trim())).toMatchObject({
        schemaHash: hash,
        statement: "select",
        auth: "backend-secret",
        appId,
      });

      // Local schema loading never requires catalogue credentials.
      await writeFile(
        join(schemaDir, "schema.ts"),
        `import { schema as s } from "jazz-tools";\nexport const app = s.defineApp({ notes: s.table({ text: s.string(), rank: s.int(), count: s.bigint().optional(), done: s.boolean().default(false) }), private_notes: s.table({ text: s.string() }) });\n`,
      );
      await writeFile(join(schemaDir, "read.sql"), "SELECT text FROM notes WHERE rank = 4");
      const local = await cli(
        ["sql", "--schema-dir", schemaDir, "--file", join(schemaDir, "read.sql"), "--format=json"],
        { ...env, JAZZ_ADMIN_SECRET: "" },
      );
      expect(local.stderr).toBe("");
      expect(local.code).toBe(0);
      expect(JSON.parse(local.stdout)).toEqual([{ text: "from CLI" }]);

      expect((await sql(`DELETE FROM notes WHERE id = '${id}'`, true)).rows[0]).toMatchObject({
        affectedRows: 1,
      });
      expect(await db.one(app.notes.where({ id }), { tier: "remote" })).toBeNull();
      expect((await sql(`DELETE FROM notes WHERE id = '${id}'`, true)).rows[0]).toMatchObject({
        affectedRows: 0,
        txId: null,
        atomic: false,
      });
    } finally {
      await session.close();
      await server.stop();
      await rm(schemaDir, { recursive: true, force: true });
    }
  }, 60000);

  it("preserves JWT read/write permissions and never falls back to inherited backend authority", async () => {
    const issuer = await startTestJwtIssuer();
    const appId = randomUUID();
    const server = await startLocalJazzServer({
      appId,
      inMemory: true,
      jwksUrl: issuer.jwksUrl,
      jwtIssuer: issuer.issuer,
      jwtAudience: issuer.audience,
    });
    const backend = await createJazzSession({
      app,
      appId,
      serverUrl: server.url,
      driver: { type: "memory" },
      initial: { backendSecret: server.backendSecret },
    });
    const user = await createJazzSession({
      app,
      appId,
      serverUrl: server.url,
      driver: { type: "memory" },
    });
    try {
      await deploy({
        appId,
        serverUrl: server.url,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const jwt = issuer.jwtForUser("sql-reader");
      await user.registerJWT(jwt);
      const db = backend.getSnapshot().client!.db;
      const row = await db
        .insert(app.notes, { text: "readable", rank: 1, count: null })
        .wait({ tier: "global" });
      await db.insert(app.private_notes, { text: "private" }).wait({ tier: "global" });
      const { hashes } = await fetchSchemaHashes(server.url, {
        appId,
        adminSecret: server.adminSecret,
      });
      const env = {
        JAZZ_APP_ID: appId,
        JAZZ_SERVER_URL: server.url,
        JAZZ_JWT_TOKEN: jwt,
        JAZZ_ADMIN_SECRET: server.adminSecret,
        JAZZ_BACKEND_SECRET: "",
      };
      const remote = ["--schema-hash", hashes[0]!];
      const flags = (sql: string) =>
        sqlFlags([sql, ...remote], { appId, serverUrl: server.url }, env);
      expect((await runDataQuery(flags("SELECT text FROM notes"))).result).toMatchObject({
        rows: [{ text: "readable" }],
      });
      expect((await runDataQuery(flags("SELECT * FROM private_notes"))).result).toMatchObject({
        rows: [],
      });
      expect(
        (
          await runDataQuery(
            sqlFlags(
              ["INSERT INTO notes (text, rank) VALUES ('user write', 2)", "--write", ...remote],
              { appId, serverUrl: server.url },
              env,
            ),
          )
        ).result,
      ).toMatchObject({ rows: [expect.objectContaining({ affectedRows: 1 })] });
      await expect(
        runDataQuery(
          sqlFlags(
            ["INSERT INTO private_notes (text) VALUES ('denied')", "--write", ...remote],
            { appId, serverUrl: server.url },
            env,
          ),
        ),
      ).rejects.toThrow(/denied|permission|policy|rejected/i);
      await expect(
        runDataQuery(
          sqlFlags(
            [`UPDATE notes SET text = 'denied' WHERE id = '${row.id}'`, "--write", ...remote],
            { appId, serverUrl: server.url },
            env,
          ),
        ),
      ).rejects.toThrow(/denied|permission|policy|rejected/i);
      await expect(
        runDataQuery(
          sqlFlags(
            [`DELETE FROM notes WHERE id = '${row.id}'`, "--write", ...remote],
            { appId, serverUrl: server.url },
            env,
          ),
        ),
      ).rejects.toThrow(/denied|permission|policy|rejected/i);

      const forbidden = await cli(
        [
          "sql",
          "INSERT INTO private_notes (text) VALUES ('denied by CLI')",
          ...remote,
          "--jwt",
          jwt,
          "--write",
          "--format=json",
        ],
        { ...env, JAZZ_BACKEND_SECRET: server.backendSecret },
      );
      expect(forbidden.code).toBe(4);
      expect(forbidden.stdout).toBe("");
      expect(JSON.parse(forbidden.stderr)).toMatchObject({
        error: { code: "DENIED", exitCode: 4 },
      });
      expect(await db.one(app.notes.where({ id: row.id }), { tier: "remote" })).toMatchObject({
        text: "readable",
      });

      // An admin secret is catalogue-only: it can never read rows.
      const adminOnly = await cli(
        [
          "sql",
          "SELECT text FROM notes",
          ...remote,
          "--admin-secret",
          server.adminSecret,
          "--format=json",
        ],
        {
          ...env,
          JAZZ_JWT_TOKEN: "",
          JAZZ_BACKEND_SECRET: "",
          JAZZ_ADMIN_SECRET: server.adminSecret,
        },
      );
      expect(adminOnly.code).toBe(3);
      expect(JSON.parse(adminOnly.stderr)).toMatchObject({ error: { code: "AUTH_REQUIRED" } });

      const unenrolled = await cli(
        [
          "sql",
          "SELECT text FROM notes",
          ...remote,
          "--jwt",
          issuer.jwtForUser("unenrolled"),
          "--format=json",
        ],
        { ...env, JAZZ_BACKEND_SECRET: "" },
      );
      expect(unenrolled.code).toBe(3);
      expect(unenrolled.stderr).toMatch(/identity_not_assigned|401/i);
    } finally {
      await user.close();
      await backend.close();
      await server.stop();
      await issuer.stop();
    }
  }, 60000);

  it("treats blank credentials as absent and refuses two real ones", async () => {
    const blank = parseDataArgs(
      ["SELECT 1"],
      { appId: "a", serverUrl: "http://127.0.0.1:1" },
      { JAZZ_JWT_TOKEN: "", JAZZ_BACKEND_SECRET: "backend" },
      "sql",
    );
    expect(blank).toMatchObject({ jwt: undefined, backendSecret: "backend" });
    const both = parseDataArgs(
      ["SELECT 1"],
      { appId: "a", serverUrl: "http://127.0.0.1:1" },
      { JAZZ_JWT_TOKEN: "user", JAZZ_BACKEND_SECRET: "backend" },
      "sql",
    );
    expect(() => resolveDataOptions(both, parseSql("SELECT text FROM notes"))).toThrow(
      /explicitly choose/,
    );
  });

  it("answers help and capabilities without credentials, and bounds unavailable-server waits", async () => {
    const help = await cli(["sql", "--help"], {});
    expect(help.code).toBe(0);
    expect(help.stdout).toContain("--backend-secret");
    expect(help.stdout).toContain("Exit codes");
    expect(help.stdout).not.toContain("data query");

    const capabilities = await cli(["sql", "--capabilities"], {});
    expect(capabilities.code).toBe(0);
    expect(JSON.parse(capabilities.stdout)).toMatchObject({ version: 1 });

    const timedOut = await cli(
      [
        "sql",
        "--file",
        "-",
        "--timeout=100",
        "--schema-dir",
        ".",
        "--app-id",
        "a",
        "--server-url",
        "http://127.0.0.1:1",
        "--backend-secret",
        "b",
      ],
      {},
      null,
    );
    // Keep stdin open: the whole-command deadline must cover input as well.
    expect(timedOut.code).toBe(5);
    expect(timedOut.stderr).toContain("TIMEOUT");
  });
});
