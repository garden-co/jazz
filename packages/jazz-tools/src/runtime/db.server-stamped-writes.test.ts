import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import type { Db } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz, type TestJazz } from "../testing/index.js";
import { createAccountManagerWithMinter } from "../accounts/account-manager-core.js";
import { createClient, mintLocalFirstTokenInJs } from "../thin/index.js";

// An ordinary write names no time (R19): the server stamps it when it accepts
// it. So a device clock running ahead is never refused as a write "in the
// future", and an edit made offline is stamped when it syncs, after the edits
// it overwrites (SPEC 4, INV-HIST-8: the write the server accepts last wins).

const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
  policy.notes.allowUpdate.always();
});

const HOUR_MS = 60 * 60 * 1000;

const opened: Db[] = [];
let server: TestJazz | undefined;

afterEach(async () => {
  vi.restoreAllMocks();
  for (const db of opened.splice(0)) await db.shutdown();
  await server?.stop();
  server = undefined;
});

async function startServer(): Promise<TestJazz> {
  server = await startTestJazz({
    app,
    permissions,
    coreOptions: { inMemory: true },
  });
  return server;
}

async function localFirstDb(server: TestJazz): Promise<Db> {
  const db = await createDb(await localAccountConfig(server.appId, server.url));
  opened.push(db);
  return db;
}

async function thinDb(server: TestJazz): Promise<Db> {
  let stored: string | null = null;
  // The account manager the thin entry used to export: it signs in JavaScript.
  const accounts = await createAccountManagerWithMinter(
    {
      appId: server.appId,
      serverUrl: server.url,
      store: {
        async read() {
          return stored;
        },
        async update(transform: (current: string | null) => string | null) {
          stored = transform(stored);
        },
      },
    },
    mintLocalFirstTokenInJs,
  );
  const db = await createClient({
    appId: server.appId,
    serverUrl: server.url,
    account: accounts.createLocalFirst(),
  });
  opened.push(db);
  return db;
}

/** Run `write` while the device clock reads an hour ahead. */
function withClockAhead<T>(write: () => T): T {
  const ahead = Date.now() + HOUR_MS;
  const spy = vi.spyOn(Date, "now").mockReturnValue(ahead);
  try {
    return write();
  } finally {
    spy.mockRestore();
  }
}

async function serverUpdatedAt(db: Db, id: string): Promise<number> {
  const [row] = await db.all(app.notes.where({ id }).select("title", "$updatedAt"), {
    tier: "remote",
  });
  return row!.$updatedAt.getTime();
}

// Thin writes go straight to the server. (A local-first client's own token
// handling reads the same clock, so a local-first replica is covered by the
// offline test below and the adapter's unit test instead.)
{
  it("thin: a device clock running ahead stamps nothing and is not refused", async () => {
    const db = await thinDb(await startServer());
    const before = Date.now();
    const inserted = withClockAhead(() => db.insert(app.notes, { title: "fast clock" }));
    const row = await inserted.wait({ tier: "global" });
    const updated = withClockAhead(() => db.update(app.notes, row.id, { title: "still fast" }));
    await updated.wait({ tier: "global" });

    const stamped = await serverUpdatedAt(db, row.id);
    expect(stamped).toBeGreaterThanOrEqual(before);
    expect(stamped).toBeLessThan(before + HOUR_MS / 2);
  }, 60_000);
}

it("an offline edit synced later wins and is accepted after the edit it overwrote", async () => {
  const server = await startServer();
  const alice = await localFirstDb(server);
  const bob = await localFirstDb(server);
  const note = await alice.insert(app.notes, { title: "first" }).wait({ tier: "global" });
  expect(await bob.all(app.notes.where({ id: note.id }), { tier: "remote" })).toHaveLength(1);
  expect(await alice.all(app.notes.where({ id: note.id }), { tier: "remote" })).toHaveLength(1);

  await alice.disconnect();
  const offline = alice.update(app.notes, note.id, { title: "alice offline" });
  await offline.wait({ tier: "local" });
  // Bob's edit is made, and accepted, after Alice's.
  await new Promise((resolve) => setTimeout(resolve, 20));
  await bob.update(app.notes, note.id, { title: "bob online" }).wait({ tier: "global" });
  await new Promise((resolve) => setTimeout(resolve, 20));
  await alice.reconnect();
  await offline.wait({ tier: "global" });

  for (const db of [alice, bob]) {
    const [current] = await db.all(app.notes.where({ id: note.id }), { tier: "remote" });
    expect(current!.title).toBe("alice offline");
  }
  const newestFirst = await bob.all(
    app.$history.notes
      .where({ id: note.id })
      .orderBy("$acceptedAt", "desc")
      .select("title", "$acceptedAt"),
    { tier: "remote" },
  );
  expect(newestFirst.map((version) => version.title)).toEqual([
    "alice offline",
    "bob online",
    "first",
  ]);
  expect(newestFirst[0]!.$acceptedAt.getTime()).toBeGreaterThan(
    newestFirst[1]!.$acceptedAt.getTime(),
  );
}, 60_000);
