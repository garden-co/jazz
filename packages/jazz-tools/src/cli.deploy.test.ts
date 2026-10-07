import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { vi } from "vitest";
import type { DeploymentRequest, DeploymentResponse } from "./dev/catalogue-api.js";
import { loadCompiledSchema } from "./schema-loader.js";
import { schema as s } from "./schema-namespace.js";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const {
  indexPath,
  APP_ID,
  deploy,
  createWorkspace,
  captureConsoleLogs,
  computeTestSchemaHash,
  rootSchemaWithoutInlinePermissions,
  rootSchemaWithIndexedTodo,
  rootPermissionsSchema,
  rootAllExplicitPermissionsSchema,
  storedSchemaResponse,
} = createCliFixtures(import.meta.url);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isDeploymentRequest(value: unknown): value is DeploymentRequest {
  return (
    isRecord(value) &&
    typeof value.targetSchemaHash === "string" &&
    Array.isArray(value.schemas) &&
    value.schemas.every(
      (entry: unknown) =>
        isRecord(entry) &&
        typeof entry.hash === "string" &&
        isRecord(entry.schema) &&
        isRecord(entry.schema.tables),
    ) &&
    Array.isArray(value.migrations) &&
    value.migrations.every(
      (migration: unknown) =>
        isRecord(migration) &&
        typeof migration.fromHash === "string" &&
        typeof migration.toHash === "string" &&
        Array.isArray(migration.forward),
    ) &&
    isRecord(value.permissions)
  );
}

function parseDeploymentRequest(body: RequestInit["body"]): DeploymentRequest {
  const value: unknown = JSON.parse(String(body));
  if (!isDeploymentRequest(value)) {
    throw new Error("Expected a valid deployment request payload.");
  }
  return value;
}

describe("cli deploy", () => {
  it("rejects missing permissions before publication", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(
      deploy({
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        schemaDir: root,
        migrationsDir: join(root, "migrations"),
      }),
    ).rejects.toThrow("Create a permissions.ts file");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it.each(["initial", "permissions", "unchanged", "indexed"])(
    "reports a %s deployment",
    async (kind) => {
      const { root } = await createWorkspace();
      await writeFile(
        join(root, "schema.ts"),
        kind === "indexed" ? rootSchemaWithIndexedTodo() : rootSchemaWithoutInlinePermissions(),
      );
      await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());
      const hash = await computeTestSchemaHash((await loadCompiledSchema(root)).wasmSchema);
      const missing = kind === "initial" || kind === "indexed";
      let body: DeploymentRequest | undefined;
      const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
        if (input.endsWith("/migrations/graph"))
          return new Response(
            JSON.stringify({
              schemas: missing ? [] : [hash],
              migrations: [],
              activeSchemaHash: missing ? null : hash,
            }),
          );
        expect(input.endsWith("/admin/deploy")).toBe(true);
        body = parseDeploymentRequest(init?.body);
        return new Response(
          JSON.stringify({
            changed: kind !== "unchanged",
            published: { schemas: missing ? [hash] : [], migrations: [] },
          }),
        );
      });
      vi.stubGlobal("fetch", fetchMock);
      const { logs } = await captureConsoleLogs(() =>
        deploy({
          serverUrl: "http://localhost:1625",
          adminSecret: "admin-secret",
          schemaDir: root,
        }),
      );
      expect(fetchMock).toHaveBeenCalledTimes(2);
      if (!body) throw new Error("Expected a deployment request payload.");
      expect(body.schemas).toHaveLength(missing ? 1 : 0);
      expect(body.targetSchemaHash).toBe(hash);
      expect(Object.keys(body.permissions)).toContain("todos");
      if (kind === "indexed")
        expect(body.schemas[0].schema.tables.todos.indexed_columns).toEqual(["ownerId"]);
      expect(
        logs.some((line) =>
          line.includes(kind === "unchanged" ? "already up to date" : "Deployed schema"),
        ),
      ).toBe(true);
      expect(logs.some((line) => line.includes("Warning: table"))).toBe(true);
      expect(logs.some((line) => line.includes("Published permissions as v"))).toBe(false);
    },
  );

  it.each([false, true])(
    "explains revert and merge options (target already published: %s)",
    async (published) => {
      const { root } = await createWorkspace();
      await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
      await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());
      const branches = ["a".repeat(64), "b".repeat(64)];
      const target = await computeTestSchemaHash((await loadCompiledSchema(root)).wasmSchema);
      vi.stubGlobal(
        "fetch",
        vi.fn(async (input: string, init?: RequestInit) => {
          if (input.endsWith("/migrations/graph"))
            return new Response(
              JSON.stringify({
                schemas: published ? [...branches, target] : branches,
                activeSchemaHash: branches[0],
                migrations: [],
              }),
            );
          expect(input.endsWith("/admin/deploy")).toBe(true);
          const body = parseDeploymentRequest(init?.body);
          return new Response(
            JSON.stringify({
              code: "unreachable_deployment_target",
              error: "no forward migration path exists",
              details: { target: body.targetSchemaHash, active: branches[0] },
            }),
            { status: 422 },
          );
        }),
      );
      const result = deploy({
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        schemaDir: root,
      });
      await expect(result).rejects.toThrow(
        [
          `Cannot deploy local schema ${target.slice(0, 12)} from server schema aaaaaaaaaaaa: no forward migration path connects them.`,
          "",
          "Choose one:",
          "",
          "1. Revert to the common schema.",
          "   Restore the shared ancestor in schema.ts (and its permissions), then deploy it.",
          "   After that, restore your branch and deploy again. No reverse migration is needed.",
          "",
          "2. Merge both changes.",
          "   Update schema.ts to include both branches' changes and update permissions.ts.",
          "   Create a migration from each branch to that merged schema:",
          `     jazz-tools migrations create ${APP_ID} --fromHash aaaaaaaaaaaa`,
          `     jazz-tools migrations create ${APP_ID} --fromHash ${target.slice(0, 12)}`,
          "   Review both migrations, then deploy.",
        ].join("\n"),
      );
    },
  );
  it("publishes a complete local migration chain in one request and skips it on the next deploy", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const snapshotsDir = join(migrationsDir, "snapshots");
    await mkdir(snapshotsDir, { recursive: true });
    await writeFile(
      join(root, "schema.ts"),
      `
import { schema as s } from ${JSON.stringify(indexPath)};

const schema = {
  todos: s.table({
    title: s.string(),
    owner: s.string(),
  }, {  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`,
    );
    await writeFile(join(root, "permissions.ts"), rootAllExplicitPermissionsSchema());

    const version = (owner: string) =>
      s.defineApp({ todos: s.table({ title: s.string(), [owner]: s.string() }, {}) }).wasmSchema;
    const headSchema = version("owner_id");
    const middleSchema = version("ownerId");
    const releaseSchema = version("owner");
    const headHash = await computeTestSchemaHash(headSchema);
    const middleHash = await computeTestSchemaHash(middleSchema);
    const releaseHash = await computeTestSchemaHash(releaseSchema);
    const short = (hash: string) => hash.slice(0, 12);
    // Only the middle schema needs a snapshot: the head is stored on the
    // server and the release schema is the project's own.
    await writeFile(
      join(snapshotsDir, `20260318T000000-${short(middleHash)}.json`),
      JSON.stringify(middleSchema),
    );
    const renameMigration = (
      fromHash: string,
      toHash: string,
      fromColumn: string,
      toColumn: string,
      reverseWitnessColumns = false,
    ) => {
      const fromFields = reverseWitnessColumns
        ? `${fromColumn}: s.string(),\n      title: s.string(),`
        : `title: s.string(),\n      ${fromColumn}: s.string(),`;
      const toFields = reverseWitnessColumns
        ? `${toColumn}: s.string(),\n      title: s.string(),`
        : `title: s.string(),\n      ${toColumn}: s.string(),`;
      return `
import { schema as s, migration as m } from ${JSON.stringify(indexPath)};

export default m.defineMigration({
  migrate: {
    todos: {
      ${toColumn}: m.renameFrom(${JSON.stringify(fromColumn)}),
    },
  },
  fromHash: ${JSON.stringify(fromHash)},
  toHash: ${JSON.stringify(toHash)},
  from: {
    todos: s.table({
      ${fromFields}
    }, {}),
  },
  to: {
    todos: s.table({
      ${toFields}
    }, {}),
  },
});
`;
    };
    await writeFile(
      join(migrationsDir, `20260318-first-${short(headHash)}-${short(middleHash)}.ts`),
      renameMigration(short(headHash), short(middleHash), "owner_id", "ownerId"),
    );
    await writeFile(
      join(migrationsDir, `20260319-second-${short(middleHash)}-${short(releaseHash)}.ts`),
      // Witness ordering is intentionally different from the stored schemas;
      // canonical witness comparison is insensitive to declaration order.
      renameMigration(short(middleHash), short(releaseHash), "ownerId", "owner", true),
    );

    const stored = new Map<string, object>([[headHash, headSchema]]);
    const pushedMigrations: Array<{ fromHash: string; toHash: string; forward: unknown }> = [];
    const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith("/migrations/graph"))
        return new Response(
          JSON.stringify({
            schemas: [...stored.keys()],
            activeSchemaHash: pushedMigrations.length ? releaseHash : headHash,
            migrations: pushedMigrations.map(({ fromHash, toHash }) => ({ fromHash, toHash })),
          }),
        );
      if (input.endsWith(`/schema/${headHash}`)) return storedSchemaResponse(headSchema);
      expect(input.endsWith("/admin/deploy")).toBe(true);
      const body = parseDeploymentRequest(init?.body);
      expect(body.targetSchemaHash).toBe(releaseHash);
      for (const entry of body.schemas) stored.set(entry.hash, entry.schema.tables);
      pushedMigrations.push(...body.migrations);
      const response: DeploymentResponse = {
        changed: body.schemas.length > 0,
        published: {
          schemas: body.schemas.map(({ hash }) => hash),
          migrations: body.migrations.map(({ fromHash, toHash }) => ({ fromHash, toHash })),
        },
      };
      return new Response(JSON.stringify(response));
    });
    vi.stubGlobal("fetch", fetchMock);

    const { logs } = await captureConsoleLogs(() =>
      deploy({
        appId: APP_ID,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        schemaDir: root,
        migrationsDir,
      }),
    );

    expect(pushedMigrations.map(({ fromHash, toHash }) => [fromHash, toHash])).toEqual([
      [headHash, middleHash],
      [middleHash, releaseHash],
    ]);
    expect(pushedMigrations.map(({ forward }) => forward)).toEqual([
      [{ table: "todos", operations: [{ type: "rename", column: "owner_id", value: "ownerId" }] }],
      [{ table: "todos", operations: [{ type: "rename", column: "ownerId", value: "owner" }] }],
    ]);
    expect([...stored.keys()].sort()).toEqual([headHash, middleHash, releaseHash].sort());
    expect(logs.filter((line) => line.includes("Pushed migration"))).toHaveLength(2);
    expect(logs.some((line) => line.toLowerCase().includes("not connected"))).toBe(false);
    expect(logs.some((line) => line.includes("Deployed schema"))).toBe(true);

    // A replay is idempotent: already-connected edges are reported as skipped.
    const { logs: replayLogs } = await captureConsoleLogs(() =>
      deploy({
        appId: APP_ID,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        schemaDir: root,
        migrationsDir,
      }),
    );
    for (const [from, to] of [
      [headHash, middleHash],
      [middleHash, releaseHash],
    ]) {
      expect(replayLogs).toContain(
        `Migration ${from.slice(0, 12)} -> ${to.slice(0, 12)} is already connected; skipping migration publish.`,
      );
    }
    expect(replayLogs.some((line) => /^(?:Published|Pushed) migration\b/i.test(line))).toBe(false);
    expect(fetchMock.mock.calls.filter(([url]) => url.endsWith("/admin/deploy"))).toHaveLength(2);
    expect(pushedMigrations).toHaveLength(2);
  });

  it("rejects the removed noVerify bypass before publication", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(
      deploy({
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        schemaDir: "/unused",
        migrationsDir: "/unused",
        // @ts-expect-error Old JavaScript callers must not silently bypass migration checks.
        noVerify: true,
      }),
    ).rejects.toThrow("noVerify is no longer supported");
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
