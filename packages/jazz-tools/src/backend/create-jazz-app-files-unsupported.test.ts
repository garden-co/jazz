import { copyFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { computeSchemaHash } from "../schema-hash.js";
import { buildAppFiles, checkRunningApp } from "../dev/app-files.js";

// The files can't express every engine schema yet: a row column, which
// Jazz's Rust writer refuses with the code JAZZ_APP_FILES_UNSUPPORTED (the TS
// DSL can't build one, but the engine schema can hold it). Not mocked: the
// real writer refuses through jazz-napi, and createJazz's startup check warns
// and lets the app start as on .62.

const EXAMPLE = fileURLToPath(new URL("../../../../examples/todo-server-ts/", import.meta.url));
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()!();
});

const schema = {
  notes: {
    columns: [
      { name: "body", column_type: { type: "Text" }, nullable: false },
      {
        name: "status",
        column_type: {
          type: "Row",
          columns: [{ name: "at", column_type: { type: "Timestamp" }, nullable: false }],
        },
        nullable: true,
      },
    ],
  },
} as unknown as WasmSchema;

async function input() {
  return {
    current: await computeSchemaHash(schema),
    schemas: [schema],
    migrations: [],
    permissions: { notes: { select: { using: { type: "True" } } } } as never,
    wasmSchema: schema,
  };
}

it("the real writer refuses a row column with JAZZ_APP_FILES_UNSUPPORTED", async () => {
  const built = await buildAppFiles(await input());
  expect(built).toEqual({ unsupported: expect.stringMatching(/row column/) });
});

it("createJazz's startup check warns and starts when the committed files can't describe the app yet", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-app-files-unsupported-"));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  // Committed files, so the startup check runs.
  for (const file of ["jazz.app.json", "jazz.permissions.json"]) {
    await copyFile(join(EXAMPLE, file), join(dir, file));
  }
  const warnings: string[] = [];
  await expect(
    checkRunningApp(await input(), dir, (message) => warnings.push(message)),
  ).resolves.toBeUndefined();
  expect(warnings.join("\n")).toMatch(/can't describe this app yet .*row column.*as on \.62/);
});

it("createJazz's startup check never runs the writer for a project without the files", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-app-files-absent-"));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  const warnings: string[] = [];
  await checkRunningApp(await input(), dir, (message) => warnings.push(message));
  // The writer would have refused this app; with no files it never ran.
  expect(warnings).toEqual([]);
});
