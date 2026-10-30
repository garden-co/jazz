import {
  mkdtemp,
  mkdir,
  writeFile,
  readFile,
  readdir,
  rm,
  rename,
  symlink,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { computeSchemaHash } from "./catalogue.js";
import { deploy, validateProject } from "./catalogue-project.js";

const roots: string[] = [];
const indexPath = fileURLToPath(new URL("../index.ts", import.meta.url));
const options = { appId: "test", serverUrl: "http://localhost:1625", adminSecret: "test" };
afterEach(async () => {
  vi.unstubAllGlobals();
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});
async function fixture(commonjs = false) {
  const root = await mkdtemp(join(tmpdir(), "jazz-deploy-project-"));
  roots.push(root);
  const before = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
  const after = s.defineApp({ notes: s.table({ body: s.string() }, {}) }, { migrations: "none" });
  const fromHash = await computeSchemaHash(before.wasmSchema);
  const toHash = await computeSchemaHash(after.wasmSchema);
  const migrationsDir = join(root, "migrations");
  await mkdir(join(migrationsDir, "snapshots"), { recursive: true });
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "schema.ts"),
    `import { schema as s } from ${JSON.stringify(indexPath)}; import { migrations } from "./migrations"; export const app = s.defineApp({notes:s.table({body:s.string()}, {})}, { migrations });`,
  );
  await writeFile(join(root, "permissions.ts"), `export default {};`);
  await writeFile(
    join(migrationsDir, "snapshots", `20260925T000000-${fromHash.slice(0, 12)}.json`),
    JSON.stringify(before.wasmSchema),
  );
  const file = join(
    migrationsDir,
    `20260925T000001-${fromHash.slice(0, 12)}-${toHash.slice(0, 12)}.ts`,
  );
  const source = `import { schema as s, migration as m } from ${JSON.stringify(indexPath)};
const migration = m.defineMigration({fromHash:${JSON.stringify(fromHash)},toHash:${JSON.stringify(toHash)},
from:{notes:s.table({title:s.string()}, {})},to:{notes:s.table({body:s.string()}, {})},migrate:{notes:{body:m.renameFrom("title")}}});
${commonjs ? "module.exports = migration;" : "export default migration;"}`;
  await writeFile(file, source);
  const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.endsWith("/migrations/graph"))
      return Response.json({ activeSchemaHash: fromHash, schemas: [fromHash], migrations: [] });
    expect(url.endsWith("/admin/deploy")).toBe(true);
    const body = JSON.parse(String(init?.body));
    expect(body.migrations).toHaveLength(1);
    expect(body.migrations[0]).toMatchObject({ fromHash, toHash });
    return Response.json({
      changed: true,
      published: { schemas: [toHash], migrations: [{ fromHash, toHash }] },
    });
  });
  vi.stubGlobal("fetch", fetchMock);
  return { root, migrationsDir, file, source, fetchMock };
}
it.each([false, true])(
  "deploys a reviewed migration and removes its private bundle (CommonJS: %s)",
  async (commonjs) => {
    const { root, migrationsDir } = await fixture(commonjs);
    const onEvent = vi.fn();
    expect((await deploy({ ...options, schemaDir: root, onEvent })).changed).toBe(true);
    expect(onEvent.mock.calls.some(([event]) => event.type === "migration-published")).toBe(true);
    expect(
      (await readdir(migrationsDir)).filter((name) => name.startsWith(".jazz-bundle-")),
    ).toEqual([]);
  },
);
it.each(["file", "directory"])(
  "rejects a symlinked migration %s before publication",
  async (kind) => {
    const { root, migrationsDir, file, fetchMock } = await fixture();
    const path = kind === "file" ? file : migrationsDir;
    await rename(path, `${path}-real`);
    await symlink(`${path}-real`, path, kind === "file" ? "file" : "dir");
    await expect(deploy({ ...options, schemaDir: root })).rejects.toThrow("symlink or junction");
    expect(fetchMock.mock.calls.every(([url]) => !url.endsWith("/deploy"))).toBe(true);
  },
);
it("rejects a migration witness that differs from its historical snapshot", async () => {
  const { root, file, source, fetchMock } = await fixture();
  await writeFile(file, source.replace("title:s.string()", "title:s.string(),extra:s.string()"));
  await expect(deploy({ ...options, schemaDir: root })).rejects.toThrow();
  expect(fetchMock.mock.calls.every(([url]) => !url.endsWith("/deploy"))).toBe(true);
});

it("keeps concurrent migration bundles isolated across module realms", async () => {
  const { file, migrationsDir } = await fixture();
  const { build } = await import("esbuild");
  const outputs = new Set<string>();
  let release!: () => void;
  const ready = new Promise<void>((resolve) => {
    release = resolve;
  });
  vi.doMock("esbuild", () => ({
    build: async (options: import("esbuild").BuildOptions) => {
      const output = options.outfile!;
      expect(outputs.has(output)).toBe(false);
      outputs.add(output);
      if (outputs.size === 2) release();
      await ready;
      return build(options);
    },
  }));
  try {
    vi.resetModules();
    const first = await import("./catalogue-project.js");
    vi.resetModules();
    const second = await import("./catalogue-project.js");
    await Promise.all([first.loadDefinedMigration(file), second.loadDefinedMigration(file)]);
    expect(outputs.size).toBe(2);
    expect(
      (await readdir(migrationsDir)).filter((name) => name.startsWith(".jazz-bundle-")),
    ).toEqual([]);
  } finally {
    vi.doUnmock("esbuild");
    vi.resetModules();
  }
});
/** The `migrations` an app imports from its generated module. */
async function importMigrations(migrationsDir: string) {
  const module = await import(pathToFileURL(join(migrationsDir, "index.ts")).href);
  return module.migrations;
}
it("writes the deployed schemas and migrations for the app's clients", async () => {
  const { root, migrationsDir } = await fixture();
  await deploy({ ...options, schemaDir: root });
  const bundle = await importMigrations(migrationsDir);
  const before = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
  const after = s.defineApp({ notes: s.table({ body: s.string() }, {}) }, { migrations: "none" });
  const [fromHash, toHash] = await Promise.all([
    computeSchemaHash(before.wasmSchema),
    computeSchemaHash(after.wasmSchema),
  ]);
  expect(bundle.schemas.map(({ hash }: { hash: string }) => hash).sort()).toEqual(
    [fromHash, toHash].sort(),
  );
  expect(bundle.migrations).toEqual([
    {
      fromHash,
      toHash,
      forward: [
        expect.objectContaining({
          table: "notes",
          operations: [{ type: "rename", column: "title", value: "body" }],
        }),
      ],
    },
  ]);
});

it("writes the migrations module for an app with a single schema version", async () => {
  const root = await mkdtemp(join(tmpdir(), "jazz-validate-project-"));
  roots.push(root);
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "schema.ts"),
    `import { schema as s } from ${JSON.stringify(indexPath)}; import { migrations } from "./migrations"; export const app = s.defineApp({notes:s.table({title:s.string()}, {})}, { migrations });`,
  );
  await writeFile(join(root, "permissions.ts"), `export default {};`);
  await validateProject({ schemaDir: root });
  const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
  expect(await importMigrations(join(root, "migrations"))).toEqual({
    schemas: [
      { hash: await computeSchemaHash(app.wasmSchema), schema: { tables: app.wasmSchema } },
    ],
    migrations: [],
  });
  // Unchanged, validating again leaves the module as it was.
  const path = join(root, "migrations", "index.ts");
  const written = await readFile(path, "utf8");
  await validateProject({ schemaDir: root });
  expect(await readFile(path, "utf8")).toBe(written);
});

it("leaves an app that does not import its migrations module without one", async () => {
  const root = await mkdtemp(join(tmpdir(), "jazz-validate-project-"));
  roots.push(root);
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "schema.ts"),
    `import { schema as s } from ${JSON.stringify(indexPath)}; export const app = s.defineApp({notes:s.table({title:s.string()}, {})}, { migrations: "none" });`,
  );
  await writeFile(join(root, "permissions.ts"), `export default {};`);
  await validateProject({ schemaDir: root });
  expect(existsSync(join(root, "migrations", "index.ts"))).toBe(false);
});
