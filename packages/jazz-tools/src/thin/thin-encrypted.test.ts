import { afterEach, describe, expect, it } from "vitest";
import { createAccountManagerWithMinter } from "../accounts/account-manager-core.js";
import type { Db } from "../runtime/db.js";
import { EncryptionNeedsLocalFirstError } from "../runtime/encryption-needs-local-first.js";
import { setLocalFirstUpgrader } from "../runtime/local-first-upgrade.js";
import { startTestJazz, testApp, type TestJazz } from "../testing/index.js";
import { createClient, mintLocalFirstTokenInJs } from "./index.js";

// `tasks.title` (text, compared for equality) and `files.blob` (bytes) are
// encrypted, in the space their `projectId` names; every other column, and
// `projects` and `notes`, are plain.
const app = testApp({
  format: 1,
  tables: {
    projects: { columns: { name: "string" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes", __e2ee_eq_title: "bytes", note: "string?" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: {
        space: "projectId",
        scope: "projects",
        columns: { title: "string" },
        equality: ["title"],
      },
    },
    files: {
      columns: { projectId: "uuid", blob: "bytes", name: "string?" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: { space: "projectId", scope: "projects", columns: { blob: "bytes" } },
    },
    notes: { columns: { text: "string" } },
  },
}) as never as Record<"projects" | "tasks" | "files" | "notes", never>;
const everyone = { read: "true", insert: "true", update: "true", delete: "true" };
const permissions = {
  format: 1,
  tables: {
    projects: {
      ...everyone,
      read: "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
    },
    tasks: everyone,
    files: everyone,
    notes: everyone,
  },
};

let server: TestJazz | undefined;
const opened: Db[] = [];
afterEach(async () => {
  for (const db of opened.splice(0)) await db.shutdown();
  await server?.stop();
  server = undefined;
});

/** Every frame the thin client sends. */
const frames: Uint8Array[] = [];

class RecordingWebSocket extends WebSocket {
  override send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (typeof data === "string") frames.push(new TextEncoder().encode(data));
    else if (ArrayBuffer.isView(data))
      frames.push(new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice());
    else if (data instanceof ArrayBuffer) frames.push(new Uint8Array(data).slice());
    super.send(data as never);
  }
}

async function thinDb(): Promise<Db> {
  server ??= await startTestJazz({
    app: app as never,
    permissions: permissions as never,
    coreOptions: { inMemory: true },
  });
  let stored: string | null = null;
  const accounts = await createAccountManagerWithMinter(
    {
      appId: server.appId,
      serverUrl: server.url,
      store: {
        read: async () => stored,
        update: async (transform) => void (stored = transform(stored)),
      },
    },
    mintLocalFirstTokenInJs,
  );
  const db = await createClient({
    appId: server.appId,
    serverUrl: server.url,
    account: accounts.createLocalFirst(),
    thin: { WebSocket: RecordingWebSocket as never },
  });
  opened.push(db);
  return db;
}

const marker = `plaintext-marker-${crypto.randomUUID()}`;
const markerBytes = new TextEncoder().encode(marker);

/** Whether a frame holds the marker: raw, or as a JSON array of its bytes. */
function markerSent(): boolean {
  const asJson = markerBytes.join(",");
  return frames.some((frame) => {
    for (let at = 0; at + markerBytes.length <= frame.length; at++) {
      if (markerBytes.every((byte, offset) => frame[at + offset] === byte)) return true;
    }
    return new TextDecoder("latin1").decode(frame).includes(asJson);
  });
}

/** What running `operation` ends with: its error, or undefined. */
async function outcome(operation: () => unknown): Promise<unknown> {
  try {
    const result = await operation();
    const wait = (result as { wait?: (options: object) => Promise<unknown> } | undefined)?.wait;
    if (typeof wait === "function") await wait.call(result, { tier: "global" });
    return undefined;
  } catch (error) {
    return error;
  }
}

const project = "00000000-0000-7000-8000-000000000001";
const task = "00000000-0000-7000-8000-000000000002";

async function* chunks(): AsyncGenerator<Uint8Array> {
  yield markerBytes;
}

type Writer = Pick<
  Db,
  "insert" | "update" | "upsert" | "restore" | "insertStreaming" | "updateStreaming" | "all"
>;
const tasks = app.tasks as never as { select(...columns: string[]): { where(w: object): never } };

// Each operation names a marker in an encrypted column. `partial`: refused
// as a partial edit of an encrypted column, as the local-first client does.
const operations: Array<[string, (db: Writer) => unknown, "partial"?]> = [
  [
    "an insert of a bytes column",
    (db) => db.insert(app.files, { projectId: project, blob: markerBytes } as never),
  ],
  [
    "an insert of a text column",
    (db) => db.insert(app.tasks, { projectId: project, title: marker } as never),
  ],
  [
    "an update of a bytes column",
    (db) => db.update(app.files, task, { blob: markerBytes } as never),
  ],
  ["an update of a text column", (db) => db.update(app.tasks, task, { title: marker } as never)],
  [
    "an upsert",
    (db) => db.upsert(app.files, task, { projectId: project, blob: markerBytes } as never),
  ],
  [
    "a restore",
    (db) => db.restore(app.files, task, { projectId: project, blob: markerBytes } as never),
  ],
  [
    "an editedFrom of a text column",
    (db) =>
      db.update(
        app.tasks,
        task,
        { title: marker } as never,
        { editedFrom: { title: "before" } } as never,
      ),
    "partial",
  ],
  [
    "an applyDiffs of a bytes column",
    (db) =>
      db.update(
        app.files,
        task,
        {} as never,
        {
          applyDiffs: {
            blob: {
              within: { from: 0, to: 0 },
              splices: [{ at: 0, delete: 0, insert: markerBytes }],
            },
          },
        } as never,
      ),
    "partial",
  ],
  [
    "a streaming insert",
    (db) => db.insertStreaming(app.files, { projectId: project, blob: chunks() } as never),
  ],
  [
    "a streaming update with no other values",
    (db) => db.updateStreaming(app.files, task, { blob: chunks() } as never),
  ],
  ["an equality filter", (db) => db.all(tasks.select("note").where({ title: marker }))],
];

describe("a thin client of an app with encrypted columns", () => {
  it.each(operations)(
    "sends no plaintext of an encrypted column: %s",
    async (_, run, partial) => {
      const db = await thinDb();
      for (const inTransaction of [false, true]) {
        frames.length = 0;
        const error = await outcome(() =>
          inTransaction ? db.transaction((tx) => run(tx as never as Writer)) : run(db),
        );
        // Whatever went out has gone by the time a later read is answered.
        await db.all(app.notes);
        expect(markerSent(), inTransaction ? "in a transaction" : "on its own").toBe(false);
        if (partial) {
          expect(error).not.toBeInstanceOf(EncryptionNeedsLocalFirstError);
          expect((error as Error).message).toContain(
            "reading or writing part of encrypted column `",
          );
        } else {
          expect(error).toBeInstanceOf(EncryptionNeedsLocalFirstError);
        }
      }
    },
    30_000,
  );

  it("sends a plain column's value, so the recording sees what goes out", async () => {
    const db = await thinDb();
    frames.length = 0;
    expect(
      await outcome(() => db.update(app.tasks, task, { note: marker } as never)),
    ).not.toBeInstanceOf(EncryptionNeedsLocalFirstError);
    await db.all(app.notes);
    expect(markerSent()).toBe(true);
  }, 30_000);

  it("reads plain columns of the encrypted table, filtered by its space", async () => {
    const db = await thinDb();
    await expect(
      db.all(tasks.select("projectId", "note").where({ projectId: project })),
    ).resolves.toEqual([]);
  }, 30_000);

  it("a refused transaction cannot commit, even when its callback caught the refusal", async () => {
    const db = await thinDb();
    const written = db.transaction((tx) => {
      tx.insert(app.notes, { text: "staged beside a refused write" } as never);
      try {
        tx.insert(app.files, { projectId: project, blob: markerBytes } as never);
      } catch {
        // The app carries on as if nothing happened.
      }
    });
    expect(await outcome(() => written)).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect(await db.all(app.notes, { tier: "remote" })).toEqual([]);

    const explicit = db.beginTransaction();
    explicit.insert(app.notes, { text: "staged in an explicit transaction" } as never);
    expect(await outcome(() => explicit.all(tasks.select("title").where({})))).toBeInstanceOf(
      EncryptionNeedsLocalFirstError,
    );
    expect(await outcome(() => explicit.commit())).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect(await db.all(app.notes, { tier: "remote" })).toEqual([]);
  }, 30_000);

  it("a directly opened thin Db names upgradeToLocalFirst from jazz-tools/thin", async () => {
    const db = await thinDb();
    const direct = (await outcome(() =>
      db.insert(app.tasks, { projectId: project } as never),
    )) as EncryptionNeedsLocalFirstError;
    expect(direct).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect(direct.reason).toBe("direct-thin");
    expect(direct.operation).toBe("insert");
    expect(direct.table).toBe("tasks");
    expect(direct.message).toContain(
      "upgrade it with `upgradeToLocalFirst(thin, open)` from `jazz-tools/thin`",
    );

    // One a session opened, and so upgrades, advises a loader instead.
    setLocalFirstUpgrader(db, async () => undefined);
    const session = (await outcome(() =>
      db.update(app.tasks, task, { title: marker } as never),
    )) as EncryptionNeedsLocalFirstError;
    expect(session.reason).toBe("no-loader");
    expect(session.column).toBe("title");
    expect(session.message).toBe(
      '`tasks.title` is end-to-end encrypted, which needs the local-first client. Give Jazz a loader for it, `client: () => import("jazz-tools/local-first")`, where you create the client or provider, or call `db.upgradeToLocalFirst(loader)` first.',
    );
  }, 30_000);
});
