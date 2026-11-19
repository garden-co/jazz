import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { testApp, type TestAppFile } from "../testing/test-app.js";
import { createJazz, memory, remote, sqlite } from "./create-jazz.js";

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

describe("createJazz takes jazz.permissions.json in every form a project may hold", () => {
  const tables = { notes: { read: "true" } };
  async function startWith(permissions: unknown) {
    const jazz = createJazz({
      app: testApp(notes),
      permissions: permissions as never,
      store: memory(),
    });
    cleanups.push(() => jazz.close());
    return jazz.ready();
  }

  it("starts with a v1 file, whose $schema names its version", async () => {
    await inProject();
    await expect(
      startWith({ $schema: "https://jazz.tools/schemas/jazz.permissions.v1.json", tables }),
    ).resolves.toBeTruthy();
  });

  it("starts with a legacy (pre-alpha.66) file: format and the unversioned URL", async () => {
    await inProject();
    await expect(
      startWith({
        $schema: "https://jazz.tools/schemas/jazz.permissions.schema.json",
        format: 1,
        tables,
      }),
    ).resolves.toBeTruthy();
  });

  it("starts with a legacy file whose $schema is a relative path", async () => {
    await inProject();
    await expect(
      startWith({ $schema: "./jazz.permissions.schema.json", format: 1, tables }),
    ).resolves.toBeTruthy();
  });

  it("refuses a file with neither $schema nor format", async () => {
    await inProject();
    await expect(startWith({ tables })).rejects.toThrow(
      /`permissions` must be the app's jazz\.permissions\.json/,
    );
  });
});

describe("createJazz refuses apps that declare end-to-end encryption", () => {
  // Projects are spaces; a task's title is encrypted in its project's space,
  // and its notes are being decrypted into `notesText`.
  const encrypted: TestAppFile = {
    format: 1,
    tables: {
      projects: { columns: { name: "string", ownerId: "uuid" } },
      tasks: {
        columns: {
          projectId: "uuid",
          title: "bytes",
          notes: "bytes",
          notesText: "string?",
        },
        relations: { project: { rel: "projects", column: "projectId" } },
        encryption: {
          space: "projectId",
          scope: "projects",
          columns: { title: "string", notes: { type: "string", to: "notesText" } },
        },
      },
    },
  };
  const members =
    "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)";
  const permissions = {
    $schema: "https://jazz.tools/schemas/jazz.permissions.v1.json",
    tables: {
      projects: {
        read: members,
        insert: "$createdBy.account = session.user",
        update: "ownerId = session.user",
      },
      tasks: { read: "allowedTo.read(project)", insert: "allowedTo.update(project)" },
    },
  } as never;

  function startEncrypted(store: ReturnType<typeof sqlite> | ReturnType<typeof remote>) {
    const jazz = createJazz({
      app: testApp(encrypted),
      permissions,
      store,
      confirmDecrypt: ["tasks.notes"],
    });
    cleanups.push(() => jazz.close());
    return jazz.ready();
  }

  it("leaves the SQLite store untouched even when the decrypt conversion is confirmed", async () => {
    await inProject();
    await expect(startEncrypted(sqlite({ path: join(process.cwd(), "store") }))).rejects.toThrow(
      /declares end-to-end encryption/,
    );
    expect(await readdir(process.cwd())).toEqual([]);
  });

  it("refuses before sending a deployment request to a remote store", async () => {
    await inProject();
    const fetch = vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("unexpected request"));
    try {
      const store = remote({
        url: "http://127.0.0.1:1",
        appId: "00000000-0000-4000-8000-000000000001",
        secret: "test-secret",
      });
      await expect(startEncrypted(store)).rejects.toThrow(/declares end-to-end encryption/);
      expect(fetch).not.toHaveBeenCalled();
    } finally {
      fetch.mockRestore();
    }
  });
});
