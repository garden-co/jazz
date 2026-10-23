import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { app, type Todo } from "../app/schema.js";
import { createUpgradableDb } from "../src/upgrade.js";
import { startHarness, type Harness } from "./harness.js";

let harness: Harness;

beforeAll(async () => {
  harness = await startHarness();
}, 120_000);

afterAll(async () => {
  await harness?.stop();
});

async function eventually(check: () => boolean, ms = 2_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("condition not reached");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** Record every result a subscription delivers, with a way to await a condition. */
function record(
  db: ReturnType<typeof createUpgradableDb>,
  query: Parameters<typeof db.subscribe>[0],
) {
  const results: Todo[][] = [];
  let wake = () => {};
  const stop = db.subscribe(query as never, (rows: Todo[]) => {
    results.push(rows);
    wake();
  });
  const until = (predicate: (rows: Todo[]) => boolean) =>
    new Promise<void>((resolve) => {
      wake = () => results.length > 0 && predicate(results.at(-1)!) && resolve();
      wake();
    });
  return { results, until, stop };
}

describe("upgrading from Jazz Lite to full Jazz", () => {
  it("keeps live subscriptions without gaps, flashes or duplicate renders", async () => {
    const token = await harness.identity("upgrade-alice");
    const lite = harness.lite(token);
    await lite.ready();
    const owner = lite.accountId!;
    const db = createUpgradableDb({ lite, loadFull: () => harness.fullDb(token) });

    const all = record(db, app.todos);
    const open = record(db, app.todos.where({ done: false }));
    for (let i = 0; i < 5; i++)
      await db.insert(app.todos, { title: `t${i}`, done: false, owner_id: owner });
    await all.until((rows) => rows.length === 5);
    await open.until((rows) => rows.length === 5);
    const deliveredBefore = all.results.length;

    const timings = await db.upgrade();
    if (process.env.DEBUG_SWAP) console.log(JSON.stringify(all.results.slice(-2), null, 1));
    expect(db.mode).toBe("full");
    expect(timings.subscriptions).toBe(2);
    expect(db.handovers.every((h) => !h.forced)).toBe(true);
    // The lite proxy releases every subscription and the connection.
    await eventually(
      () => harness.proxy.stats.subscriptions === 0 && harness.proxy.stats.connections === 0,
    );
    // Unchanged results are not re-delivered by the swap.
    expect(all.results.length).toBe(deliveredBefore);
    // No result during the swap was empty or shrank.
    expect(
      all.results.every((rows, i) => i === 0 || rows.length >= all.results[i - 1]!.length),
    ).toBe(true);

    // Writes now go through full Jazz and reach the same subscriptions.
    const added = await db.insert(app.todos, { title: "after", done: false, owner_id: owner });
    await all.until((rows) => rows.some((t) => t.id === added.id));
    await db.update(app.todos, added.id, { done: true });
    await open.until((rows) => rows.length === 5 && !rows.some((t) => t.id === added.id));
    all.stop();
    open.stop();
  });

  it("sees writes from other clients after the swap", async () => {
    const token = await harness.identity("upgrade-bob");
    const lite = harness.lite(token);
    await lite.ready();
    const owner = lite.accountId!;
    const db = createUpgradableDb({ lite, loadFull: () => harness.fullDb(token) });
    const all = record(db, app.todos);
    await all.until(() => true);
    await db.upgrade();

    // Same user on another lite client (e.g. a second tab still on lite).
    const other = harness.lite(token);
    await other.insert(app.todos, { title: "from elsewhere", done: false, owner_id: owner });
    await all.until((rows) => rows.some((t) => t.title === "from elsewhere"));
    all.stop();
  });

  it("delivers changes that land while full Jazz is loading", async () => {
    const token = await harness.identity("upgrade-carol");
    const lite = harness.lite(token);
    await lite.ready();
    const owner = lite.accountId!;
    let releaseLoad = () => {};
    const gate = new Promise<void>((resolve) => (releaseLoad = resolve));
    const db = createUpgradableDb({
      lite,
      loadFull: async () => {
        await gate;
        return harness.fullDb(token);
      },
    });
    const all = record(db, app.todos);
    await all.until(() => true);
    const upgrading = db.upgrade();
    expect(db.mode).toBe("upgrading");
    // Still on lite: this write and its subscription update go through the proxy.
    await db.insert(app.todos, { title: "during", done: false, owner_id: owner });
    await all.until((rows) => rows.length === 1);
    // A subscription opened mid-upgrade also moves.
    const late = record(db, app.todos.where({ title: "during" }));
    await late.until((rows) => rows.length === 1);
    releaseLoad();
    const timings = await upgrading;
    if (process.env.DEBUG_SWAP) console.log("T3", JSON.stringify(all.results));
    expect(timings.subscriptions).toBe(2);
    await eventually(() => harness.proxy.stats.subscriptions === 0);
    await db.update(app.todos, all.results.at(-1)![0]!.id, { title: "renamed" });
    await late.until((rows) => rows.length === 0);
    all.stop();
    late.stop();
  });

  it("finishes a lite transaction that is open when the upgrade starts", async () => {
    const token = await harness.identity("upgrade-dave");
    const lite = harness.lite(token);
    await lite.ready();
    const owner = lite.accountId!;
    const db = createUpgradableDb({ lite, loadFull: () => harness.fullDb(token) });
    let upgrading: Promise<unknown> | undefined;
    const ids = await db.transaction(async (tx) => {
      const a = await tx.insert(app.todos, { title: "tx-1", done: false, owner_id: owner });
      upgrading = db.upgrade();
      const b = await tx.insert(app.todos, { title: "tx-2", done: false, owner_id: owner });
      return [a.id, b.id];
    });
    await upgrading;
    const rows = await db.all(app.todos, { tier: "remote" } as never);
    expect(rows.map((r) => r.id).sort()).toEqual([...ids].sort());
  });
});
