import {
  mkdtemp,
  copyFile,
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
import { fileURLToPath } from "node:url";
import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { computeSchemaHash } from "./catalogue.js";
import { deploy, validateProject } from "./catalogue-project.js";
import { legacySchemaHash } from "./legacy-schema-hash.js";

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
    `import { schema as s } from ${JSON.stringify(indexPath)}; import artifact from "./jazz.app.json" with { type: "json" }; export const app = s.defineApp({notes:s.table({body:s.string()}, {})}, { artifact });`,
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
      return Response.json({
        latestBundleSchemaHash: fromHash,
        schemas: [fromHash],
        migrations: [],
      });
    expect(url.endsWith("/admin/bundles")).toBe(true);
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
    expect(fetchMock.mock.calls.every(([url]) => !url.endsWith("/bundles"))).toBe(true);
  },
);
it("rejects a migration witness that differs from its historical snapshot", async () => {
  const { root, file, source, fetchMock } = await fixture();
  await writeFile(file, source.replace("title:s.string()", "title:s.string(),extra:s.string()"));
  await expect(deploy({ ...options, schemaDir: root })).rejects.toThrow();
  expect(fetchMock.mock.calls.every(([url]) => !url.endsWith("/bundles"))).toBe(true);
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
/** The jazz.app.json validate writes beside schema.ts (the migrations directory's parent). */
async function readArtifact(migrationsDir: string): Promise<{
  format: number;
  current: string;
  bundle: string;
  schemas: Array<{ hash: string; tables: Record<string, unknown> }>;
  migrations: Array<{ from: string; to: string; forward: unknown[] }>;
  codegen: { tables: Record<string, unknown>; slices: Record<string, string[]> };
}> {
  return JSON.parse(await readFile(join(migrationsDir, "..", "jazz.app.json"), "utf8"));
}
it("writes the deployed schemas and migrations for the app's clients", async () => {
  const { root, migrationsDir } = await fixture();
  await deploy({ ...options, schemaDir: root });
  const bundle = await readArtifact(migrationsDir);
  expect(bundle.format).toBe(1);
  const before = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
  const after = s.defineApp({ notes: s.table({ body: s.string() }, {}) }, { migrations: "none" });
  const [fromHash, toHash] = await Promise.all([
    computeSchemaHash(before.wasmSchema),
    computeSchemaHash(after.wasmSchema),
  ]);
  // Oldest first.
  expect(bundle.schemas.map(({ hash }) => hash)).toEqual([fromHash, toHash]);
  expect(bundle.current).toBe(toHash);
  expect(bundle.migrations).toEqual([
    {
      from: fromHash,
      to: toHash,
      forward: [
        expect.objectContaining({
          table: "notes",
          operations: [{ type: "Rename", from: "title", to: "body" }],
        }),
      ],
    },
  ]);
});

it("writes jazz.app.json and jazz.permissions.json for an app with a single schema version", async () => {
  const root = await singleVersionProject();
  await validateProject({ schemaDir: root });
  const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
  const written = await readArtifact(join(root, "migrations"));
  const hash = await computeSchemaHash(app.wasmSchema);
  expect(written).toMatchObject({ format: 1, current: hash, migrations: [] });
  expect(written.schemas.map((schema) => schema.hash)).toEqual([hash]);
  // Clients ship jazz.app.json: the permissions are in the backend's file.
  expect(Object.keys(written).sort()).toEqual([
    "codegen",
    "current",
    "format",
    "migrations",
    "schemas",
  ]);
  const permissions = JSON.parse(await readFile(join(root, "jazz.permissions.json"), "utf8"));
  expect(permissions).toMatchObject({ format: 1, schema: hash, permissions: {} });
  expect(permissions.bundle).toMatch(/^[0-9a-f]{64}$/);
  // Unchanged, validating again leaves both files as they were.
  const paths = [join(root, "jazz.app.json"), join(root, "jazz.permissions.json")];
  const texts = await Promise.all(paths.map((path) => readFile(path, "utf8")));
  await validateProject({ schemaDir: root });
  expect(await Promise.all(paths.map((path) => readFile(path, "utf8")))).toEqual(texts);
  await validateProject({ schemaDir: root, check: true });
});

it("writes jazz.app.json for an app that does not import it", async () => {
  const root = await mkdtemp(join(tmpdir(), "jazz-validate-project-"));
  roots.push(root);
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "schema.ts"),
    `import { schema as s } from ${JSON.stringify(indexPath)}; export const app = s.defineApp({notes:s.table({title:s.string()}, {})}, { migrations: "none" });`,
  );
  await writeFile(join(root, "permissions.ts"), `export default {};`);
  await validateProject({ schemaDir: root });
  expect(existsSync(join(root, "jazz.app.json"))).toBe(true);
  expect(existsSync(join(root, "jazz.permissions.json"))).toBe(true);
  expect(existsSync(join(root, "migrations", "index.ts"))).toBe(false);
});

/** A project with one schema version, whose schema.ts ships its jazz.app.json. */
async function singleVersionProject(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "jazz-validate-project-"));
  roots.push(root);
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "schema.ts"),
    `import { schema as s } from ${JSON.stringify(indexPath)}; import artifact from "./jazz.app.json" with { type: "json" }; export const app = s.defineApp({notes:s.table({title:s.string()}, {})}, { artifact });`,
  );
  await writeFile(join(root, "permissions.ts"), `export default {};`);
  return root;
}

it("validate --check refuses a missing or stale jazz.app.json or jazz.permissions.json, and writes nothing", async () => {
  const root = await singleVersionProject();
  const path = join(root, "jazz.app.json");
  await expect(validateProject({ schemaDir: root, check: true })).rejects.toThrow(
    "jazz.app.json is missing. Run `jazz-tools validate` to write it, and commit it.",
  );
  expect(existsSync(path)).toBe(false);
  await validateProject({ schemaDir: root });
  const text = await readFile(path, "utf8");
  // Byte for byte: even reformatting the same JSON is stale.
  await writeFile(path, JSON.stringify(JSON.parse(text)));
  await expect(validateProject({ schemaDir: root, check: true })).rejects.toThrow(
    "jazz.app.json is not what schema.ts, permissions.ts and the migrations make",
  );
  await writeFile(path, text);
  await validateProject({ schemaDir: root, check: true });
  // The backend's file is checked as the app's is.
  const permissionsPath = join(root, "jazz.permissions.json");
  const permissionsText = await readFile(permissionsPath, "utf8");
  await rm(permissionsPath);
  await expect(validateProject({ schemaDir: root, check: true })).rejects.toThrow(
    "jazz.permissions.json is missing. Run `jazz-tools validate` to write it, and commit it.",
  );
  await writeFile(permissionsPath, permissionsText);
  // A permissions change makes jazz.permissions.json stale, and only it:
  // clients' jazz.app.json holds no permissions.
  await writeFile(
    join(root, "permissions.ts"),
    `export default { notes: { select: { using: { type: "True" } } } };`,
  );
  await expect(validateProject({ schemaDir: root, check: true })).rejects.toThrow(
    "jazz.permissions.json is not what schema.ts, permissions.ts and the migrations make",
  );
  expect(await readFile(path, "utf8")).toBe(text);
  expect(await readFile(permissionsPath, "utf8")).toBe(permissionsText);
  await validateProject({ schemaDir: root });
  expect(await readFile(path, "utf8")).toBe(text);
  expect(await readFile(permissionsPath, "utf8")).not.toBe(permissionsText);
});

it("rewrites a jazz.app.json from before the permissions moved out of it", async () => {
  const root = await singleVersionProject();
  await validateProject({ schemaDir: root });
  const path = join(root, "jazz.app.json");
  const text = await readFile(path, "utf8");
  const permissions = JSON.parse(await readFile(join(root, "jazz.permissions.json"), "utf8"));
  // The single file an earlier build of this release wrote: the app's file
  // with the permissions and their bundle in it. schema.ts imports it, and
  // defineApp refuses it, yet validate still loads the schema and rewrites it.
  const single = { ...JSON.parse(text), bundle: permissions.bundle, permissions: {} };
  await writeFile(path, JSON.stringify(single, null, 2));
  await expect(validateProject({ schemaDir: root, check: true })).rejects.toThrow(
    "jazz.app.json is not what schema.ts, permissions.ts and the migrations make",
  );
  await validateProject({ schemaDir: root });
  expect(await readFile(path, "utf8")).toBe(text);
});

it("removes the migrations/index.ts an older jazz-tools generated; --check refuses it", async () => {
  const root = await singleVersionProject();
  const legacy = join(root, "migrations", "index.ts");
  await mkdir(join(root, "migrations"), { recursive: true });
  const generated = [
    "// Generated by jazz-tools from schema.ts and its migrations. Do not edit:",
    "// `jazz-tools validate` and `jazz-tools migrations create` rewrite it.",
    'import type { ClientMigrations } from "jazz-tools";',
    'export const migrations: ClientMigrations = {"schemas":[],"migrations":[]};',
    "",
  ].join("\n");
  await writeFile(legacy, generated);
  // Written first, so --check sees the generated module and not a missing file.
  await validateProject({ schemaDir: root });
  expect(existsSync(legacy)).toBe(false);
  await writeFile(legacy, generated);
  await expect(validateProject({ schemaDir: root, check: true })).rejects.toThrow(
    "was generated by jazz-tools before alpha.63",
  );
  const { warnings } = await validateProject({ schemaDir: root });
  expect(existsSync(legacy)).toBe(false);
  expect(warnings).toContain(
    "Removed migrations/index.ts, which jazz-tools generated before alpha.63: jazz.app.json replaces it.",
  );
  await validateProject({ schemaDir: root, check: true });
});

it("keeps a hand-written migrations/index.ts", async () => {
  const root = await singleVersionProject();
  const own = join(root, "migrations", "index.ts");
  await mkdir(join(root, "migrations"), { recursive: true });
  await writeFile(own, "export const mine = 1;\n");
  await validateProject({ schemaDir: root });
  expect(await readFile(own, "utf8")).toBe("export const mine = 1;\n");
});

it("records relations, TypeScript-only columns and exported slices in codegen", async () => {
  const root = await mkdtemp(join(tmpdir(), "jazz-validate-codegen-"));
  roots.push(root);
  await writeFile(join(root, "package.json"), JSON.stringify({ type: "module" }));
  await writeFile(
    join(root, "schema.ts"),
    `import { schema as s } from ${JSON.stringify(indexPath)};
import artifact from "./jazz.app.json" with { type: "json" };
const schema = {
  projects: s.table({ name: s.string() }, {
    todos: s.reverse("todos", "project"),
    shout: s.computed(({ name }) => name.toUpperCase()),
  }),
  todos: s.table(
    {
      title: s.string().transform({ from: (title) => title.trim(), to: (title) => title }),
      projectId: s.uuid(),
    },
    { project: s.rel("projects", "projectId") },
  ),
  notes: s.table({ body: s.string() }, {}),
};
const app = s.defineSliceableApp(schema, { artifact });
export const planning = app.slice("projects", "todos");
export const notesOnly = app.slice("notes");
export { app };
`,
  );
  await writeFile(join(root, "permissions.ts"), `export default {};`);
  await validateProject({ schemaDir: root });
  const { codegen } = await readArtifact(join(root, "migrations"));
  expect(codegen).toEqual({
    tables: {
      projects: {
        relations: { todos: { type: "Reverse", table: "todos", relation: "project" } },
        ts_only: [{ type: "Computed", column: "shout" }],
      },
      todos: {
        relations: { project: { type: "Forward", table: "projects", column: "projectId" } },
        ts_only: [{ type: "Transform", column: "title" }],
      },
    },
    slices: { notesOnly: ["notes"], planning: ["projects", "todos"] },
  });
});

/** The fixture as an alpha.59 project left it: its snapshot and migration
 * name schema versions by their BLAKE3 hashes. */
async function legacyFixture({ keepCurrent = false } = {}) {
  const project = await fixture();
  const before = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
  const after = s.defineApp({ notes: s.table({ body: s.string() }, {}) }, { migrations: "none" });
  const [oldFrom, oldTo] = [
    legacySchemaHash(before.wasmSchema),
    legacySchemaHash(after.wasmSchema),
  ];
  const [fromHash, toHash] = await Promise.all([
    computeSchemaHash(before.wasmSchema),
    computeSchemaHash(after.wasmSchema),
  ]);
  const snapshots = join(project.migrationsDir, "snapshots");
  const [snapshot] = await readdir(snapshots);
  const legacySnapshot = `20260925T000000-${oldFrom.slice(0, 12)}.json`;
  await rename(join(snapshots, snapshot!), join(snapshots, legacySnapshot));
  const legacyName = `20260925T000001-${oldFrom.slice(0, 12)}-${oldTo.slice(0, 12)}.ts`;
  if (!keepCurrent) await rm(project.file);
  const migration = (from: string, to: string) =>
    project.source
      .replace(/fromHash:"[0-9a-f]+"/, `fromHash:"${from}"`)
      .replace(/toHash:"[0-9a-f]+"/, `toHash:"${to}"`);
  await writeFile(join(project.migrationsDir, legacyName), migration(oldFrom, oldTo));
  return {
    ...project,
    legacyName,
    legacySnapshot,
    snapshot: snapshot!,
    snapshots,
    migration,
    hashes: { oldFrom, oldTo, fromHash, toHash },
  };
}

const short = (hash: string) => hash.slice(0, 12);

it("moves migrations written with pre-SHA-256 schema hashes to legacy/", async () => {
  const { root, migrationsDir, legacyName, legacySnapshot, snapshot } = await legacyFixture();
  const result = await validateProject({ schemaDir: root });
  expect(existsSync(join(migrationsDir, legacyName))).toBe(false);
  expect(existsSync(join(migrationsDir, "legacy", legacyName))).toBe(true);
  // The snapshot's contents are exact: it is renamed to its current hash.
  expect(await readdir(join(migrationsDir, "snapshots"))).toEqual([snapshot]);
  expect(result.warnings.some((warning) => warning.includes(`Moved ${legacyName}`))).toBe(true);
  expect(
    result.warnings.some((warning) =>
      warning.includes(`Renamed snapshot ${legacySnapshot} to ${snapshot}`),
    ),
  ).toBe(true);
  expect((await readArtifact(migrationsDir)).migrations).toEqual([]);
  // Validating again finds nothing more to move (the retirement's
  // `migrations create` reminder comes with the moves).
  expect((await validateProject({ schemaDir: root })).warnings).toEqual(
    result.warnings.filter(
      (warning) =>
        !warning.includes("Moved") &&
        !warning.includes("Renamed snapshot") &&
        !warning.includes("migrations create"),
    ),
  );
});

it("says a migration from the newest snapshot is needed when none is for schema.ts", async () => {
  const { root, legacyName, snapshot } = await legacyFixture();
  await expect(validateProject({ schemaDir: root, check: true })).rejects.toThrow(
    `Then run \`jazz-tools migrations create\`: no snapshot is for schema.ts, so the app needs a migration from ${snapshot}.`,
  );
  const { warnings } = await validateProject({ schemaDir: root });
  const moved = warnings.find((warning) => warning.startsWith(`Moved ${legacyName}`));
  expect(moved).not.toContain("The app runs on schema.ts as it is");
  expect(warnings.filter((warning) => warning.includes("migrations create"))).toEqual([
    `Run \`jazz-tools migrations create\` before the app starts: no snapshot is for schema.ts, so the app needs a migration from ${snapshot}, the newest snapshot.`,
  ]);
});

it("says a migration is needed when the legacy migration is already in legacy/", async () => {
  const { root, migrationsDir, legacyName, snapshot } = await legacyFixture();
  await mkdir(join(migrationsDir, "legacy"), { recursive: true });
  await copyFile(join(migrationsDir, legacyName), join(migrationsDir, "legacy", legacyName));
  const { warnings } = await validateProject({ schemaDir: root });
  expect(warnings.some((warning) => warning.startsWith(`Removed ${legacyName}`))).toBe(true);
  expect(warnings.some((warning) => warning.includes(`needs a migration from ${snapshot}`))).toBe(
    true,
  );
});

it("keeps the newest committed snapshot, not one without a timestamp", async () => {
  const { root, migrationsDir, snapshots, snapshot } = await legacyFixture();
  const other = s.defineApp({ notes: s.table({ other: s.string() }, {}) }, { migrations: "none" });
  // Listed as a snapshot, but not a committed name: it sorts after the
  // timestamped ones, so a plain sort would keep it.
  const handNamed = `${short(await computeSchemaHash(other.wasmSchema))}.json`;
  expect(handNamed > snapshot).toBe(true);
  await writeFile(join(snapshots, handNamed), JSON.stringify(other.wasmSchema));
  const { warnings } = await validateProject({ schemaDir: root });
  expect(await readdir(snapshots)).toEqual([snapshot]);
  expect(await readdir(join(migrationsDir, "legacy", "snapshots"))).toEqual([handNamed]);
  expect(warnings.some((warning) => warning.includes(`migration from ${snapshot}`))).toBe(true);
});

it("keeps a current migration beside a legacy one", async () => {
  const { root, migrationsDir, file, legacyName } = await legacyFixture({ keepCurrent: true });
  await validateProject({ schemaDir: root });
  expect(existsSync(file)).toBe(true);
  expect(existsSync(join(migrationsDir, "legacy", legacyName))).toBe(true);
  expect((await readArtifact(migrationsDir)).migrations).toHaveLength(1);
});

it("moves a migration whose only stale end is its target", async () => {
  const { root, migrationsDir, legacyName, migration, hashes } = await legacyFixture();
  await rm(join(migrationsDir, legacyName));
  const name = `20260925T000002-${short(hashes.fromHash)}-${short(hashes.oldTo)}.ts`;
  await writeFile(join(migrationsDir, name), migration(hashes.fromHash, hashes.oldTo));
  await validateProject({ schemaDir: root });
  expect(existsSync(join(migrationsDir, "legacy", name))).toBe(true);
});

it("finishes a run that renamed the snapshots but moved no migration", async () => {
  const { root, migrationsDir, legacyName, legacySnapshot, snapshot, snapshots } =
    await legacyFixture();
  await rename(join(snapshots, legacySnapshot), join(snapshots, snapshot));
  await validateProject({ schemaDir: root });
  expect(existsSync(join(migrationsDir, "legacy", legacyName))).toBe(true);
});

it("leaves a migration whose hashes name no schema at all for the usual checks", async () => {
  const { root, migrationsDir, legacyName, migration } = await legacyFixture();
  const unknown = `20260925T000003-a0a0a0a0a0a0-b1b1b1b1b1b1.ts`;
  await writeFile(join(migrationsDir, unknown), migration("a0a0a0a0a0a0", "b1b1b1b1b1b1"));
  await validateProject({ schemaDir: root });
  expect(existsSync(join(migrationsDir, unknown))).toBe(true);
  expect(existsSync(join(migrationsDir, "legacy", legacyName))).toBe(true);
});

it("refuses a snapshot named by neither hash of its contents", async () => {
  const { root, migrationsDir, legacyName, legacySnapshot, snapshots } = await legacyFixture();
  const edited = "20260925T000000-c2c2c2c2c2c2.json";
  await rename(join(snapshots, legacySnapshot), join(snapshots, edited));
  await expect(validateProject({ schemaDir: root })).rejects.toThrow(
    new RegExp(`Snapshot ${edited} is named for schema hash c2c2c2c2c2c2`),
  );
  expect(existsSync(join(migrationsDir, legacyName))).toBe(true);
});

it("refuses to overwrite a different file in legacy/, and drops an identical copy", async () => {
  const { root, migrationsDir, legacyName, legacySnapshot, snapshots } = await legacyFixture();
  const target = join(migrationsDir, "legacy", legacyName);
  await mkdir(join(migrationsDir, "legacy"));
  await writeFile(target, "// someone else's\n");
  await expect(validateProject({ schemaDir: root })).rejects.toThrow(
    `a different legacy/${legacyName} is already there`,
  );
  // Nothing moved or was renamed.
  expect(existsSync(join(migrationsDir, legacyName))).toBe(true);
  expect(await readdir(snapshots)).toEqual([legacySnapshot]);

  await writeFile(target, await readFile(join(migrationsDir, legacyName)));
  const { warnings } = await validateProject({ schemaDir: root });
  expect(existsSync(join(migrationsDir, legacyName))).toBe(false);
  expect(warnings.some((warning) => warning.startsWith(`Removed ${legacyName}`))).toBe(true);
});

it("checks without moving or renaming anything, failing while legacy files remain", async () => {
  const { root, migrationsDir, legacyName, legacySnapshot, snapshot, snapshots } =
    await legacyFixture();
  const listing = async () => (await readdir(migrationsDir, { recursive: true })).sort();
  const before = await listing();
  await expect(validateProject({ schemaDir: root, check: true })).rejects.toThrow(
    `move ${legacyName} to legacy/; rename snapshot ${legacySnapshot} to ${snapshot}`,
  );
  expect(await listing()).toEqual(before);
  expect(await readdir(snapshots)).toEqual([legacySnapshot]);
  await validateProject({ schemaDir: root });
  await validateProject({ schemaDir: root, check: true });
});

it("moves a migration in alpha.59's DSL to legacy/ without loading it", async () => {
  const { root, migrationsDir, legacyName, hashes } = await legacyFixture();
  // As the alpha.59 CLI wrote it: \`s.defineMigration\` and \`s.add\` are gone.
  await writeFile(
    join(migrationsDir, legacyName),
    `import { schema as s } from ${JSON.stringify(indexPath)};
export default s.defineMigration({
  migrate: { notes: { body: s.add.string({ default: "" }) } },
  fromHash: "${short(hashes.oldFrom)}",
  toHash: "${short(hashes.oldTo)}",
  from: { notes: s.table({ title: s.string() }, {}) },
  to: { notes: s.table({ body: s.string() }, {}) },
});
`,
  );
  await validateProject({ schemaDir: root });
  expect(existsSync(join(migrationsDir, legacyName))).toBe(false);
  expect(existsSync(join(migrationsDir, "legacy", legacyName))).toBe(true);
  await validateProject({ schemaDir: root, check: true });
});

/** An alpha.59 project whose snapshots hold its old schema and the one
 * schema.ts has now, both named by BLAKE3 hash, with the legacy migration
 * between them. */
async function legacyFixtureWithCurrentSnapshot() {
  const project = await legacyFixture();
  const after = s.defineApp({ notes: s.table({ body: s.string() }, {}) }, { migrations: "none" });
  const currentLegacy = `20260925T000001-${short(project.hashes.oldTo)}.json`;
  await writeFile(join(project.snapshots, currentLegacy), JSON.stringify(after.wasmSchema));
  return {
    ...project,
    currentLegacy,
    currentSnapshot: `20260925T000001-${short(project.hashes.toHash)}.json`,
  };
}

it("moves the snapshots only legacy migrations led from to legacy/snapshots/", async () => {
  const { root, migrationsDir, snapshots, legacySnapshot, currentSnapshot, legacyName } =
    await legacyFixtureWithCurrentSnapshot();
  const { warnings } = await validateProject({ schemaDir: root });
  expect(existsSync(join(migrationsDir, "legacy", legacyName))).toBe(true);
  // Only schema.ts's snapshot stays, so the history has one root.
  expect(await readdir(snapshots)).toEqual([currentSnapshot]);
  expect(await readdir(join(migrationsDir, "legacy", "snapshots"))).toEqual([legacySnapshot]);
  expect(warnings.some((warning) => warning.startsWith(`Moved snapshot ${legacySnapshot}`))).toBe(
    true,
  );
  expect((await readArtifact(migrationsDir)).schemas).toHaveLength(1);
  expect(warnings.find((warning) => warning.startsWith(`Moved ${legacyName}`))).toContain(
    "The app runs on schema.ts as it is",
  );
  await validateProject({ schemaDir: root, check: true });
});

it("lists the snapshots it would move when checking", async () => {
  const { root, legacySnapshot, currentLegacy, currentSnapshot } =
    await legacyFixtureWithCurrentSnapshot();
  await expect(validateProject({ schemaDir: root, check: true })).rejects.toThrow(
    `move snapshot ${legacySnapshot} to legacy/snapshots/; rename snapshot ${currentLegacy} to ${currentSnapshot}`,
  );
});

it("keeps a snapshot a current migration leads from", async () => {
  const { root, file, snapshots, snapshot, currentSnapshot } =
    await legacyFixtureWithCurrentSnapshot();
  const { source } = await fixture();
  await writeFile(file, source);
  await validateProject({ schemaDir: root });
  expect((await readdir(snapshots)).sort()).toEqual([snapshot, currentSnapshot].sort());
});

it("refuses to publish legacy migrations and points at validate", async () => {
  const { root, legacyName, fetchMock } = await legacyFixture();
  await expect(deploy({ ...options, schemaDir: root })).rejects.toThrow(
    new RegExp(`${legacyName}.*jazz-tools validate`),
  );
  expect(fetchMock).not.toHaveBeenCalled();
});

it("publishes the current schema once validate moved the legacy migrations", async () => {
  const { root, hashes } = await legacyFixture();
  await validateProject({ schemaDir: root });
  const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.endsWith("/migrations/graph"))
      return Response.json({ latestBundleSchemaHash: null, schemas: [], migrations: [] });
    expect(url.endsWith("/admin/bundles")).toBe(true);
    const body = JSON.parse(String(init?.body));
    expect(body.migrations).toEqual([]);
    return Response.json({
      changed: true,
      published: { schemas: [hashes.toHash], migrations: [] },
    });
  });
  vi.stubGlobal("fetch", fetchMock);
  expect((await deploy({ ...options, schemaDir: root })).changed).toBe(true);
});
