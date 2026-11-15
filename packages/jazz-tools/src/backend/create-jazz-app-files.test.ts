import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { testApp, type TestAppFile } from "../testing/test-app.js";
import { createJazz, memory } from "./create-jazz.js";

const notes: TestAppFile = { format: 1, tables: { notes: { columns: { body: "string" } } } };
const notesWithDone: TestAppFile = {
  format: 1,
  tables: { notes: { columns: { body: "string", done: "boolean?" } } },
};
const readable = { format: 1, tables: { notes: { read: "true" } } };

const previousCwd = process.cwd();
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  process.chdir(previousCwd);
  while (cleanups.length > 0) await cleanups.pop()!();
});

/** A project directory holding `app` as its jazz.app.json, as the process's working directory. */
async function inProject(app?: TestAppFile) {
  const dir = await mkdtemp(join(tmpdir(), "jazz-app-files-"));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  if (app) {
    // The text testApp compiles, so its bindings' `source` is this file's hash.
    await writeFile(join(dir, "jazz.app.json"), JSON.stringify(app));
    await writeFile(join(dir, "jazz.permissions.json"), JSON.stringify(readable));
  }
  process.chdir(dir);
}

/** Start `createJazz` with bindings generated from `notes`, as an app on its own store would. */
async function start() {
  const jazz = createJazz({ app: testApp(notes), permissions: readable, store: memory() });
  cleanups.push(() => jazz.close());
  return jazz.ready();
}

describe("createJazz checks its bindings against the project's jazz.app.json", () => {
  it("starts when the project has no jazz.app.json (a deployed bundle ships only the bindings)", async () => {
    await inProject();
    await expect(start()).resolves.toBeTruthy();
  });

  it("starts when the bindings were generated from the project's jazz.app.json", async () => {
    await inProject(notes);
    await expect(start()).resolves.toBeTruthy();
  });

  it("refuses, naming jazz.generated.ts, when jazz.app.json changed since the bindings were generated", async () => {
    // The project's file is another schema: jazz.app.json was edited.
    await inProject(notesWithDone);
    await expect(start()).rejects.toThrow(
      /bindings out of date: jazz\.generated\.ts was generated from another jazz\.app\.json\. Run `npx jazz-tools generate`/,
    );
  });
});
