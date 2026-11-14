import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { buildAppFiles } from "../dev/app-files.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { computeSchemaHash } from "../schema-hash.js";
import { appMigrations } from "../testing/client-migrations.js";
import type { WasmSchema } from "../drivers/types.js";
import { createJazz, memory } from "./create-jazz.js";

const notes = { notes: s.table({ body: s.string() }, {}) };
const notesWithDone = { notes: s.table({ body: s.string(), done: s.boolean().optional() }, {}) };

function readable(app: object) {
  return s.definePermissions(app as s.App<s.Schema<typeof notes>>, ({ policy }) => {
    policy.notes.allowRead.always();
  });
}

function writableToo(app: object) {
  return s.definePermissions(app as s.App<s.Schema<typeof notes>>, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
  });
}

/** The files validate would write for an app whose only version is `definition`. */
async function filesFor(definition: typeof notes | typeof notesWithDone, rules: typeof readable) {
  const app = s.defineApp(definition, { migrations: "none" });
  const schema = mergePermissionsIntoWasmSchema(app.wasmSchema as WasmSchema, {});
  const built = await buildAppFiles({
    current: await computeSchemaHash(schema),
    schemas: [schema],
    migrations: [],
    permissions: rules(app),
    wasmSchema: app.wasmSchema as WasmSchema,
  });
  if (!("files" in built)) throw new Error(built.unsupported);
  return built.files;
}

const previousCwd = process.cwd();
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  process.chdir(previousCwd);
  while (cleanups.length > 0) await cleanups.pop()!();
});

/** A project directory holding `files`, as the process's working directory. */
async function inProject(files?: { app: string; permissions: string }) {
  const dir = await mkdtemp(join(tmpdir(), "jazz-app-files-"));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  if (files) {
    await writeFile(join(dir, "jazz.app.json"), files.app);
    await writeFile(join(dir, "jazz.permissions.json"), files.permissions);
  }
  process.chdir(dir);
}

/** Start `createJazz` for `notes` with `rules`, as an app on its own store would. */
async function start(rules = readable) {
  const app = s.defineApp(notes, { migrations: await appMigrations(notes) });
  const jazz = createJazz({ app, permissions: rules(app), store: memory() });
  cleanups.push(() => jazz.close());
  return jazz.ready();
}

describe("createJazz checks the project's jazz.app.json and jazz.permissions.json", () => {
  it("starts when the project has no files (it never ran validate)", async () => {
    await inProject();
    await expect(start()).resolves.toBeTruthy();
  });

  it("starts when the files are what validate writes for the app", async () => {
    await inProject(await filesFor(notes, readable));
    await expect(start()).resolves.toBeTruthy();
  });

  it("refuses, naming jazz.app.json, when schema.ts changed since validate ran", async () => {
    // The committed files are for another schema: schema.ts was edited.
    await inProject(await filesFor(notesWithDone, readable));
    await expect(start()).rejects.toThrow(
      /jazz\.app\.json is out of date with schema\.ts \(or its migrations\): run `npx jazz-tools validate`/,
    );
  });

  it("refuses, naming jazz.permissions.json, when permissions.ts changed since validate ran", async () => {
    await inProject(await filesFor(notes, readable));
    await expect(start(writableToo)).rejects.toThrow(
      /jazz\.permissions\.json is out of date with permissions\.ts: run `npx jazz-tools validate`/,
    );
  });

  it('skips the check for an app on { migrations: "none" }', async () => {
    await inProject(await filesFor(notesWithDone, readable));
    const app = s.defineApp(notes, { migrations: "none" });
    const jazz = createJazz({ app, permissions: readable(app), store: memory() });
    cleanups.push(() => jazz.close());
    await expect(jazz.ready()).resolves.toBeTruthy();
  });
});
