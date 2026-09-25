import { mkdtemp, mkdir, writeFile, readdir, rm, rename, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { computeSchemaHash } from "./catalogue.js";
import { deploy } from "./catalogue-project.js";

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
  const before = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
  const after = s.defineApp({ notes: s.table({ body: s.string() }, {}) });
  const fromHash = await computeSchemaHash(before.wasmSchema);
  const toHash = await computeSchemaHash(after.wasmSchema);
  const migrationsDir = join(root, "migrations");
  await mkdir(join(migrationsDir, "snapshots"), { recursive: true });
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "schema.ts"),
    `import { schema as s } from ${JSON.stringify(indexPath)}; export const app = s.defineApp({notes:s.table({body:s.string()}, {})});`,
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
  const source = `import { schema as s } from ${JSON.stringify(indexPath)};
const migration = s.defineMigration({fromHash:${JSON.stringify(fromHash)},toHash:${JSON.stringify(toHash)},
from:{notes:s.table({title:s.string()}, {})},to:{notes:s.table({body:s.string()}, {})},migrate:{notes:{body:s.renameFrom("title")}}});
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
