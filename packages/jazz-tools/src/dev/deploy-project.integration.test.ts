import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { runtimeSchemaJsonReplacer } from "../drivers/schema-wire.js";
import { computeSchemaHash } from "../schema-hash.js";
import { deploy } from "./catalogue-project.js";
import { fetchMigrationGraph } from "./migration-graph.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./dev-server.js";
import { createTempRootTracker } from "./test-helpers.js";
import type { DeploymentRequest } from "./catalogue-api.js";

const roots = createTempRootTracker();
let server: LocalJazzServerHandle | undefined;
afterEach(async () => {
  vi.restoreAllMocks();
  await server?.stop();
  server = undefined;
  await roots.cleanup();
});

it("validates all branches before writing, deploys the converged history, and sends only missing artifacts", async () => {
  server = await startLocalJazzServer({ inMemory: true });
  const root = await roots.create("jazz-deploy-graph-");
  const migrationsDir = join(root, "migrations");
  const snapshotsDir = join(migrationsDir, "snapshots");
  await mkdir(snapshotsDir, { recursive: true });
  const options = {
    schemaDir: root,
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
  };
  const columns = [[], ["a"], ["b"], ["a", "b"]];
  const versions = await Promise.all(
    columns.map(async (added) => {
      const app = s.defineApp({
        notes: s.table(
          { title: s.string(), ...Object.fromEntries(added.map((column) => [column, s.string()])) },
          {},
        ),
      });
      return { schema: app.wasmSchema, hash: await computeSchemaHash(app.wasmSchema) };
    }),
  );
  const fields = (index: number) =>
    ["title", ...columns[index]!].map((column) => `${column}: s.string()`).join(", ");
  const writeCurrent = (index: number) =>
    writeFile(
      join(root, "schema.ts"),
      `import { schema as s } from "jazz-tools"; export const app = s.defineApp({ notes: s.table({ ${fields(index)} }, {}) });`,
    );
  await writeCurrent(0);
  await writeFile(join(root, "permissions.ts"), "export default {};");
  expect((await deploy(options)).changed).toBe(true);
  const initial = await fetchMigrationGraph(options);
  for (const version of versions.slice(1, 3)) {
    await writeFile(
      join(snapshotsDir, `20260922T000000-${version.hash.slice(0, 12)}.json`),
      JSON.stringify(version.schema, runtimeSchemaJsonReplacer),
    );
  }
  await writeCurrent(3);
  const writeMigration = (from: number, to: number, column: string) =>
    writeFile(
      join(
        migrationsDir,
        `20260922-add-${column}-${versions[from]!.hash.slice(0, 12)}-${versions[to]!.hash.slice(0, 12)}.ts`,
      ),
      `import { schema as s } from "jazz-tools";
    export default s.defineMigration({ fromHash: "${versions[from]!.hash}", toHash: "${versions[to]!.hash}",
      from: { notes: s.table({ ${fields(from)} }, {}) }, to: { notes: s.table({ ${fields(to)} }, {}) },
      migrate: { notes: { ${column}: s.add.string({ default: "" }) } } });`,
    );
  await writeMigration(0, 1, "a");
  await writeMigration(0, 2, "b");
  await writeMigration(1, 3, "b");
  await expect(deploy(options)).rejects.toThrow("migrations create");
  expect(await fetchMigrationGraph(options)).toEqual(initial);
  await writeMigration(2, 3, "a");

  const requests: DeploymentRequest[] = [];
  const fetch = globalThis.fetch;
  vi.spyOn(globalThis, "fetch").mockImplementation((input, init) => {
    if (init?.method === "POST") {
      expect(String(input).endsWith("/admin/deploy")).toBe(true);
      requests.push(JSON.parse(String(init.body)));
    }
    return fetch(input, init);
  });
  const result = await deploy(options);
  expect(result.published.schemas.sort()).toEqual(
    versions
      .slice(1)
      .map((version) => version.hash)
      .sort(),
  );
  expect(result.published.migrations).toHaveLength(4);
  expect(requests).toHaveLength(1);
  expect(requests[0]!.schemas).toHaveLength(3);
  expect(requests[0]!.migrations).toHaveLength(4);
  const graph = await fetchMigrationGraph(options);
  expect(graph.activeSchemaHash).toBe(versions[3]!.hash);
  expect(graph.migrations).toHaveLength(4);

  await writeFile(
    join(root, "permissions.ts"),
    `import { schema as s } from "jazz-tools"; import { app } from "./schema.js";
    export default s.definePermissions(app, ({ policy }) => { policy.notes.allowRead.always(); });`,
  );
  expect((await deploy(options)).changed).toBe(true);
  expect(requests[1]).toMatchObject({ schemas: [], migrations: [] });
  expect((await deploy(options)).changed).toBe(false);
  expect(requests[2]).toMatchObject({ schemas: [], migrations: [] });
  expect(await fetchMigrationGraph(options)).toEqual(graph);
}, 30_000);
