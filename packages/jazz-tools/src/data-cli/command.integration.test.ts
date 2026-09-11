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
import { runDataQuery, type DataOptions } from "./command.js";

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

async function cli(args: string[], env: NodeJS.ProcessEnv, input?: string | null) {
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

describe("data query against a Jazz server", () => {
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
        executeSql(db, compileSql(parseSql(text, write), app.wasmSchema));
      const inserted = await sql(
        "INSERT INTO notes (text, rank, count) VALUES ('O''Brien; -- literal', 1, 9223372036854775807)",
        true,
      );
      const id = inserted.rows[0]!.id as string;
      expect(inserted.rows[0]).toMatchObject({
        operation: "insert",
        affectedRows: 1,
        id: expect.any(String),
        txId: expect.any(String),
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
      ).toMatchObject({ affectedRows: 1 });
      expect(await db.one(app.notes.where({ id }), { tier: "remote" })).toMatchObject({
        text: "changed",
        done: true,
      });

      const { hashes } = await fetchSchemaHashes(server.url, {
        appId,
        adminSecret: server.adminSecret,
      });
      const options: DataOptions = {
        appId,
        serverUrl: server.url,
        auth: { backendSecret: server.backendSecret },
        schemaHash: hashes[0]!,
        adminSecret: server.adminSecret,
        sql: "SELECT text FROM notes WHERE rank = 1",
        write: false,
        format: "json",
        timeout: 30000,
      };
      expect((await runDataQuery(options)).rows).toEqual([{ text: "changed" }]);
      const env = {
        JAZZ_APP_ID: appId,
        JAZZ_SERVER_URL: server.url,
        JAZZ_BACKEND_SECRET: server.backendSecret,
        JAZZ_ADMIN_SECRET: server.adminSecret,
      };
      const remote = ["data", "query", "--schema-hash", hashes[0]!];
      const selected = await cli(
        [...remote, "--file", "-", "--format", "jsonl"],
        env,
        "SELECT text FROM notes ORDER BY rank DESC LIMIT 1;",
      );
      expect(selected.stderr).toBe("");
      expect(selected.code).toBe(0);
      expect(JSON.parse(selected.stdout)).toEqual({ text: "third" });
      const denied = await cli(
        [...remote, "--sql", "INSERT INTO notes (text, rank) VALUES ('forbidden', 4)"],
        env,
      );
      expect(denied.code).toBe(1);
      expect(denied.stderr).toContain("--write");
      expect(denied.stdout).toBe("");
      const written = await cli(
        [
          ...remote,
          "--sql",
          "INSERT INTO notes (text, rank) VALUES ('from CLI', 4)",
          "--write",
          "--format=json",
        ],
        env,
      );
      expect(written.stderr).toBe("");
      expect(written.code).toBe(0);
      expect(JSON.parse(written.stdout)[0]).toMatchObject({ affectedRows: 1, operation: "insert" });

      // Local schema loading never requires catalogue credentials.
      await writeFile(
        join(schemaDir, "schema.ts"),
        `import { schema as s } from "jazz-tools";
export const app = s.defineApp({ notes: s.table({ text: s.string(), rank: s.int(), count: s.bigint().optional(), done: s.boolean().default(false) }), private_notes: s.table({ text: s.string() }) });\n`,
      );
      await writeFile(join(schemaDir, "read.sql"), "SELECT text FROM notes WHERE rank = 4");
      const local = await cli(
        [
          "data",
          "query",
          "--schema-dir",
          schemaDir,
          "--file",
          join(schemaDir, "read.sql"),
          "--format=json",
        ],
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
      const options: DataOptions = {
        appId,
        serverUrl: server.url,
        auth: { jwt },
        schemaHash: hashes[0]!,
        adminSecret: server.adminSecret,
        sql: "SELECT text FROM notes",
        write: false,
        format: "json",
        timeout: 30000,
      };
      expect((await runDataQuery(options)).rows).toEqual([{ text: "readable" }]);
      expect((await runDataQuery({ ...options, sql: "SELECT * FROM private_notes" })).rows).toEqual(
        [],
      );
      expect(
        (
          await runDataQuery({
            ...options,
            sql: "INSERT INTO notes (text, rank) VALUES ('user write', 2)",
            write: true,
          })
        ).rows[0],
      ).toMatchObject({ affectedRows: 1 });
      await expect(
        runDataQuery({
          ...options,
          sql: "INSERT INTO private_notes (text) VALUES ('denied')",
          write: true,
        }),
      ).rejects.toThrow(/denied|permission|policy|rejected/i);
      await expect(
        runDataQuery({
          ...options,
          sql: `UPDATE notes SET text = 'denied' WHERE id = '${row.id}'`,
          write: true,
        }),
      ).rejects.toThrow(/denied|permission|policy|rejected/i);
      await expect(
        runDataQuery({ ...options, sql: `DELETE FROM notes WHERE id = '${row.id}'`, write: true }),
      ).rejects.toThrow(/denied|permission|policy|rejected/i);
      const forbidden = await cli(
        [
          "data",
          "query",
          "--schema-hash",
          hashes[0]!,
          "--jwt",
          jwt,
          "--write",
          "--sql",
          "INSERT INTO private_notes (text) VALUES ('denied by CLI')",
        ],
        {
          JAZZ_APP_ID: appId,
          JAZZ_SERVER_URL: server.url,
          JAZZ_BACKEND_SECRET: server.backendSecret,
          JAZZ_ADMIN_SECRET: server.adminSecret,
        },
      );
      expect(forbidden.code).toBe(1);
      expect(forbidden.stdout).toBe("");
      expect(forbidden.stderr).toMatch(/denied|permission|policy|rejected/i);
      expect(await db.one(app.notes.where({ id: row.id }), { tier: "remote" })).toMatchObject({
        text: "readable",
      });
      await expect(
        runDataQuery({ ...options, auth: { backendSecret: server.adminSecret } }),
      ).rejects.toThrow(/401/);
      await expect(
        runDataQuery({ ...options, auth: { jwt: issuer.jwtForUser("unenrolled") } }),
      ).rejects.toThrow(/identity_not_assigned/);
    } finally {
      await user.close();
      await backend.close();
      await server.stop();
      await issuer.stop();
    }
  }, 60000);

  it("bounds unavailable-server waits and exposes data-specific help through the wrapper", async () => {
    const help = await cli(["data", "query", "--help"], {});
    expect(help.code).toBe(0);
    expect(help.stdout).toContain("--backend-secret");
    expect(help.stdout).toContain("Read-only");
    const timedOut = await cli(
      ["data", "query", "--file", "-", "--timeout=100", "--schema-dir", "."],
      {
        JAZZ_APP_ID: "test-app",
        JAZZ_SERVER_URL: "http://127.0.0.1:1",
        JAZZ_BACKEND_SECRET: "backend",
      },
      null,
    );
    // Keep stdin open: the whole-command deadline must cover input as well.
    expect(timedOut.code).toBe(1);
    expect(timedOut.stderr).toContain("timed out");
  });
});
