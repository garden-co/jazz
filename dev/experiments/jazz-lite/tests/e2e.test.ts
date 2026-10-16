import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { app, type Todo } from "../app/schema.js";
import { LiteRemoteError, type LiteDb } from "../src/client.js";
import { startHarness, type Harness } from "./harness.js";

let harness: Harness;
let proxy: Harness["proxy"];
const identity = (userId: string) => harness.identity(userId);
const lite = (token: string) => harness.lite(token);

/** Resolve with the first subscription result that satisfies `predicate`. */
function nextResult<T>(
  db: LiteDb,
  query: Parameters<LiteDb["subscribe"]>[0],
  predicate: (rows: T[]) => boolean,
): { result: Promise<T[]>; stop: () => void } {
  let stop = () => {};
  const result = new Promise<T[]>((resolve, reject) => {
    stop = db.subscribe(query as never, {
      onUpdate: (rows: unknown[]) => {
        if (predicate(rows as T[])) resolve(rows as T[]);
      },
      onError: reject,
    });
  });
  return { result, stop };
}

beforeAll(async () => {
  harness = await startHarness();
  proxy = harness.proxy;
}, 120_000);

afterAll(async () => {
  await harness?.stop();
});

describe("Jazz Lite against a server-side client proxy", () => {
  it("streams per-subscription results for inserts, updates and deletes", async () => {
    const alice = lite(await identity("lite-alice"));
    await alice.ready();
    const owner = alice.accountId!;
    expect(owner).toBeTruthy();

    const initial = nextResult<Todo>(alice, app.todos, () => true);
    expect(await initial.result).toEqual([]);
    initial.stop();

    const added = nextResult<Todo>(alice, app.todos, (rows) => rows.length === 1);
    const inserted = await alice.insert(app.todos, { title: "milk", done: false, owner_id: owner });
    expect(inserted).toMatchObject({ title: "milk", done: false });
    expect((await added.result)[0]).toMatchObject({ id: inserted.id, title: "milk" });
    added.stop();

    const toggled = nextResult<Todo>(alice, app.todos, (rows) => rows[0]?.done === true);
    await alice.update(app.todos, inserted.id, { done: true });
    expect((await toggled.result)[0]).toMatchObject({ id: inserted.id, done: true });

    const gone = nextResult<Todo>(alice, app.todos, (rows) => rows.length === 0);
    await alice.delete(app.todos, inserted.id);
    expect(await gone.result).toEqual([]);
    toggled.stop();
    gone.stop();
  });

  it("evaluates reads and writes with the caller's permissions", async () => {
    const alice = lite(await identity("lite-alice-2"));
    const bob = lite(await identity("lite-bob-2"));
    await Promise.all([alice.ready(), bob.ready()]);

    const todo = await alice.insert(app.todos, {
      title: "private",
      done: false,
      owner_id: alice.accountId!,
    });
    expect(await bob.all(app.todos.where({ id: todo.id }))).toEqual([]);
    await expect(bob.update(app.todos, todo.id, { done: true })).rejects.toBeInstanceOf(
      LiteRemoteError,
    );
    await expect(
      bob.insert(app.todos, { title: "forged", done: false, owner_id: alice.accountId! }),
    ).rejects.toBeInstanceOf(LiteRemoteError);
    expect(await alice.one(app.todos.where({ id: todo.id }))).toMatchObject({ done: false });
  });

  it("supports filtered queries with includes", async () => {
    const alice = lite(await identity("lite-alice-3"));
    await alice.ready();
    const project = await alice.insert(app.projects, { name: "home" });
    await alice.insert(app.todos, {
      title: "in project",
      done: false,
      owner_id: alice.accountId!,
      projectId: project.id,
    });
    await alice.insert(app.todos, { title: "loose", done: true, owner_id: alice.accountId! });

    const open = await alice.all(app.todos.where({ done: false }).include({ project: true }));
    expect(open).toHaveLength(1);
    expect(open[0]).toMatchObject({
      title: "in project",
      project: { id: project.id, name: "home" },
    });
  });

  it("runs interactive transactions with read-your-writes and rollback", async () => {
    const alice = lite(await identity("lite-alice-4"));
    await alice.ready();
    const owner = alice.accountId!;

    const seen = await alice.transaction(async (tx) => {
      const a = await tx.insert(app.todos, { title: "tx-a", done: false, owner_id: owner });
      await tx.update(app.todos, a.id, { done: true });
      return tx.all(app.todos.where({ title: "tx-a" }));
    });
    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatchObject({ done: true });
    expect(await alice.all(app.todos.where({ title: "tx-a" }))).toHaveLength(1);

    await expect(
      alice.transaction(async (tx) => {
        await tx.insert(app.todos, { title: "tx-b", done: false, owner_id: owner });
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    expect(await alice.all(app.todos.where({ title: "tx-b" }))).toEqual([]);

    await alice.exclusiveTransaction(async (tx) => {
      await tx.insert(app.todos, { title: "tx-c", done: false, owner_id: owner });
    });
    expect(await alice.all(app.todos.where({ title: "tx-c" }))).toHaveLength(1);
  });

  it("sends only changed rows on subscription updates", async () => {
    const alice = lite(await identity("lite-alice-5"));
    await alice.ready();
    const owner = alice.accountId!;
    for (let i = 0; i < 50; i++) {
      await alice.insert(app.todos, { title: `bulk-${i}`, done: false, owner_id: owner });
    }
    let latest: Todo[] = [];
    let wake = () => {};
    const stop = alice.subscribe(app.todos, (rows) => {
      latest = rows;
      wake();
    });
    const until = (predicate: () => boolean) =>
      new Promise<void>((resolve) => {
        wake = () => predicate() && resolve();
        wake();
      });
    await until(() => latest.length === 50);
    const before = proxy.stats.bytesOut;
    await alice.update(app.todos, latest[7]!.id, { done: true });
    await until(() => latest.some((t) => t.done));
    const delta = proxy.stats.bytesOut - before;
    // A full 50-row result is ~9 KB; the update should carry roughly one row plus the ack.
    expect(delta).toBeLessThan(600);
    expect(latest).toHaveLength(50);
    stop();
  });
});
