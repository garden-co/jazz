import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { testApp, type TestAppFile } from "../testing/test-app.js";
import { createJazz } from "./create-jazz.js";
import { memory, sqlite } from "./native-provider.js";
import { remote } from "./remote-provider.js";
import { keyless } from "../encryption/keyless-types.js";

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

describe("createJazz validates and opens encrypted apps with keyless hooks", () => {
  // Each task's title and notes are encrypted in its project's space.
  const encrypted = {
    format: 1,
    tables: {
      projects: { columns: { name: "string", ownerId: "uuid" } },
      tasks: {
        columns: {
          projectId: "uuid",
          title: "bytes",
          notes: "bytes",
        },
        relations: { project: { rel: "projects", column: "projectId" } },
        encryption: {
          space: "projectId",
          scope: "projects",
          columns: { title: "string", notes: "string" },
        },
      },
    },
  } as const satisfies TestAppFile;
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

  function startEncrypted(
    store: ReturnType<typeof memory> | ReturnType<typeof sqlite> | ReturnType<typeof remote>,
    permissionsFile: unknown = permissions,
  ) {
    const jazz = createJazz({
      app: testApp(encrypted),
      permissions: permissionsFile as never,
      store,
    });
    cleanups.push(() => jazz.close());
    return jazz.ready();
  }

  it("opens an encrypted app with a keyless backend", async () => {
    await inProject();
    const jazz = await startEncrypted(
      sqlite({ path: join(process.cwd(), "store"), blobs: "sqlite" }),
    );
    // The backend's types have no encrypted column: its reads never carry
    // one, and writing one is refused at runtime too.
    const app = keyless(testApp(encrypted));
    expect(() =>
      jazz.db.insert(app.tasks, {
        projectId: crypto.randomUUID(),
        // @ts-expect-error a keyless backend's insert has no encrypted column
        title: "typed by the backend",
        notes: "x",
      }),
    ).toThrow(
      "the backend holds no encryption keys; encrypted columns are written by clients (`tasks.notes`)",
    );
    expect(await jazz.db.all(app.tasks)).toEqual([]);
  });

  it("leaves the SQLite store untouched when the app has invalid permissions", async () => {
    await inProject();
    await expect(
      startEncrypted(sqlite({ path: join(process.cwd(), "store"), blobs: "sqlite" }), {
        tables: {},
      }),
    ).rejects.toThrow(/permissions.*must be the app's jazz\.permissions\.json/);
    expect(await readdir(process.cwd())).toEqual([]);
  });

  const remoteStore = () =>
    remote({
      url: "http://127.0.0.1:1",
      appId: "00000000-0000-4000-8000-000000000001",
      secret: "test-secret",
    });

  it("refuses an invalid encrypted app before sending a deployment request to a remote store", async () => {
    // Current bindings, so the app itself is what is refused: its
    // permissions are not a jazz.permissions.json.
    await inProject();
    const fetch = vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("unexpected request"));
    try {
      await expect(startEncrypted(remoteStore(), { tables: {} })).rejects.toThrow(
        /permissions.*must be the app's jazz\.permissions\.json/,
      );
      expect(fetch).not.toHaveBeenCalled();
    } finally {
      fetch.mockRestore();
    }
  });

  it("publishes a valid encrypted app to a remote store with its declarations", async () => {
    await inProject();
    const published: Record<string, unknown>[] = [];
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = String(input instanceof Request ? input.url : input);
      if (url.endsWith("/admin/migrations/graph")) {
        return Response.json({ latestBundleSchemaHash: null, schemas: [], migrations: [] });
      }
      if (url.endsWith("/admin/bundles")) {
        const body = init?.body;
        const text = typeof body === "string" ? body : new TextDecoder().decode(body as Uint8Array);
        published.push(JSON.parse(text) as Record<string, unknown>);
        // The test ends here: the publish was what it checks.
        return Response.json({ error: "stopped by the test" }, { status: 500 });
      }
      throw new Error(`unexpected request to ${url}`);
    });
    try {
      await expect(startEncrypted(remoteStore())).rejects.toThrow(/stopped by the test/);
      expect(published).toHaveLength(1);
      const [deployment] = published as [{ encryption?: unknown; schemas: { schema: unknown }[] }];
      expect(JSON.stringify(deployment.encryption)).toContain('"tasks"');
      // The stored schema keeps the sealed columns as bytes and Jazz's system tables.
      expect(JSON.stringify(deployment.schemas)).toContain("__e2ee_members");
    } finally {
      fetch.mockRestore();
    }
  });

  it("rejects stale bindings before sending a deployment request to a remote store", async () => {
    await inProject(notes);
    const fetch = vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("unexpected request"));
    try {
      await expect(startEncrypted(remoteStore())).rejects.toThrow(/bindings out of date/);
      expect(fetch).not.toHaveBeenCalled();
    } finally {
      fetch.mockRestore();
    }
  });
});
