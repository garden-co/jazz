/**
 * Browser integration tests for history & conflict management.
 *
 * Exercises the full browser stack: WASM bindings, Web Worker bridge,
 * OPFS persistence, and binary sync transport — layers the Rust E2E
 * tests don't cover.
 *
 * All tests assert **convergence** (both clients see the same final value)
 * rather than specific LWW winners, making them timing-tolerant.
 */

import { describe, it, expect, afterEach } from "vitest";
import type { TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import {
  TestCleanup,
  createSyncedDb,
  makeQuery,
  waitForCondition,
  waitForQuery,
  withTimeout,
} from "./support.js";

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

interface Todo {
  id: string;
  title: string;
  done: boolean;
}

interface TodoInit {
  title: string;
  done: boolean;
}

const todos: TableProxy<Todo, TodoInit> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as TodoInit,
};

const allTodos = makeQuery<Todo>("todos", schema);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("History & Conflict Management", () => {
  const ctx = new TestCleanup();
  afterEach(() => ctx.cleanup());

  /**
   * Two browser clients update the same todo concurrently. Both must
   * eventually converge to the same title.
   *
   *   dbAlice ──insert todo──► server ◄──update same todo── dbBob
   *            (both update title concurrently)
   *
   *            waitForQuery on both → same title
   *
   *
   * KNOWN BUG: the server does not relay concurrent commits between
   * browser clients. Each client only ever sees its own update
   * (alice=alice-edit, bob=bob-edit — verified via 40s polling).
   * The server scope-based forwarding (forward_update_to_clients_except)
   * appears to work in Rust E2E tests (in-process RuntimeCore) but not
   * through the HTTP /sync + /events pipeline.
   */
  it.skip("concurrent updates converge in browser", async () => {
    const token = `hc-concurrent-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const dbAlice = await createSyncedDb(ctx, "hc-alice-concurrent", token);
    const dbBob = await createSyncedDb(ctx, "hc-bob-concurrent", token);

    // Alice inserts a todo
    const uniqueTitle = `original-${Date.now()}`;
    const { id } = await withTimeout(
      dbAlice.insertDurable(todos, { title: uniqueTitle, done: false }, { tier: "worker" }),
      10000,
      "Alice insert(worker) did not resolve",
    );

    // Wait for Bob to see it
    await waitForQuery(
      dbBob,
      allTodos,
      (rows) => rows.some((row) => row.id === id),
      "Bob sees Alice's todo",
      20000,
    );

    // Both update concurrently — creates diverged tips (true conflict).
    // Promise.all ensures neither awaits the other's round-trip first.
    await Promise.all([
      dbAlice.updateDurable(todos, id, { title: "alice-edit" }, { tier: "worker" }),
      dbBob.updateDurable(todos, id, { title: "bob-edit" }, { tier: "worker" }),
    ]);

    // Both must converge to the same final title.
    await waitForCondition(
      async () => {
        const aliceRows = await dbAlice.all(allTodos);
        const bobRows = await dbBob.all(allTodos);
        const aliceTodo = aliceRows.find((r) => r.id === id);
        const bobTodo = bobRows.find((r) => r.id === id);
        if (!aliceTodo || !bobTodo) return false;
        return (
          aliceTodo.title !== uniqueTitle &&
          bobTodo.title !== uniqueTitle &&
          aliceTodo.title === bobTodo.title
        );
      },
      40000,
      "Alice and Bob should converge to the same title",
    );
  }, 90000);

  it("sequential update propagates from A to B", async () => {
    const token = `hc-seq-update-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const dbAlice = await createSyncedDb(ctx, "hc-alice-seq-upd", token);
    const dbBob = await createSyncedDb(ctx, "hc-bob-seq-upd", token);

    // Alice inserts
    const { id } = await withTimeout(
      dbAlice.insertDurable(todos, { title: "original", done: false }, { tier: "worker" }),
      10000,
      "Alice insert did not resolve",
    );

    // Bob sees the insert
    await waitForQuery(
      dbBob,
      allTodos,
      (rows) => rows.some((row) => row.id === id && row.title === "original"),
      "Bob sees original",
      20000,
    );

    // Alice updates
    await dbAlice.updateDurable(todos, id, { title: "updated-by-alice" }, { tier: "worker" });

    // Alice sees her own update locally
    await waitForQuery(
      dbAlice,
      allTodos,
      (rows) => rows.some((row) => row.id === id && row.title === "updated-by-alice"),
      "Alice sees her own update",
      10000,
    );

    // Bob should see the update — THIS is the question
    await waitForQuery(
      dbBob,
      allTodos,
      (rows) => rows.some((row) => row.id === id && row.title === "updated-by-alice"),
      "Bob sees Alice's update",
      20000,
    );
  }, 60000);

  /**
   * Two browser clients each create a todo concurrently. Both should
   * eventually see 2 todos.
   *
   *   dbAlice ──insert "buy milk"──► server ◄──insert "buy eggs"── dbBob
   *
   *            waitForQuery on both → see 2 todos
   */
  it("concurrent creates both visible in browser", async () => {
    const token = `hc-creates-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const dbAlice = await createSyncedDb(ctx, "hc-alice-creates", token);
    const dbBob = await createSyncedDb(ctx, "hc-bob-creates", token);

    const milkTitle = `buy-milk-${Date.now()}`;
    const eggsTitle = `buy-eggs-${Date.now()}`;

    // Both create concurrently
    await withTimeout(
      dbAlice.insertDurable(todos, { title: milkTitle, done: false }, { tier: "worker" }),
      10000,
      "Alice insert did not resolve",
    );
    await withTimeout(
      dbBob.insertDurable(todos, { title: eggsTitle, done: false }, { tier: "worker" }),
      10000,
      "Bob insert did not resolve",
    );

    // Both should eventually see 2 todos
    const aliceRows = await waitForQuery(
      dbAlice,
      allTodos,
      (rows) => {
        const titles = rows.map((r) => r.title);
        return titles.includes(milkTitle) && titles.includes(eggsTitle);
      },
      "Alice sees both todos",
      20000,
    );
    expect(aliceRows.length).toBeGreaterThanOrEqual(2);

    const bobRows = await waitForQuery(
      dbBob,
      allTodos,
      (rows) => {
        const titles = rows.map((r) => r.title);
        return titles.includes(milkTitle) && titles.includes(eggsTitle);
      },
      "Bob sees both todos",
      20000,
    );
    expect(bobRows.length).toBeGreaterThanOrEqual(2);
  }, 60000);

  /**
   * Alice subscribes, Bob updates a todo — Alice's subscription fires
   * with a delta containing the change.
   *
   *   dbAlice subscribes via subscribeAll
   *   dbBob updates a todo
   *   subscription callback fires with delta containing bob's update
   */
  it("subscription fires on remote concurrent update", async () => {
    const token = `hc-sub-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const dbAlice = await createSyncedDb(ctx, "hc-alice-sub", token);
    const dbBob = await createSyncedDb(ctx, "hc-bob-sub", token);

    // Alice inserts a todo
    const originalTitle = `sub-test-${Date.now()}`;
    const { id } = await withTimeout(
      dbAlice.insertDurable(todos, { title: originalTitle, done: false }, { tier: "worker" }),
      10000,
      "Alice insert did not resolve",
    );

    // Wait for Bob to see it
    await waitForQuery(
      dbBob,
      allTodos,
      (rows) => rows.some((row) => row.id === id),
      "Bob sees Alice's todo",
      20000,
    );

    // Alice subscribes
    const snapshots: Todo[][] = [];
    const unsub = ctx.trackSubscription(
      dbAlice.subscribeAll(allTodos, (delta) => {
        snapshots.push([...delta.all]);
      }),
    );

    // Wait for initial snapshot
    await waitForCondition(
      async () => snapshots.length > 0,
      5000,
      "Alice should get initial subscription snapshot",
    );

    // Bob updates (durable so it propagates)
    const bobTitle = `bob-updated-${Date.now()}`;
    await dbBob.updateDurable(todos, id, { title: bobTitle }, { tier: "worker" });

    // Alice's subscription should fire with the update
    await waitForCondition(
      async () => {
        return snapshots.some((snap) =>
          snap.some((row) => row.id === id && row.title === bobTitle),
        );
      },
      20000,
      "Alice's subscription should reflect Bob's update",
    );

    unsub();
  }, 60000);

  /**
   * Alice and Bob create a conflict. Charlie connects fresh and sees
   * the same converged value.
   *
   *   dbAlice + dbBob conflict on a todo ──► server
   *                                             │
   *                  dbCharlie connects fresh, queries
   *                                             │
   *                                             └──► sees same winner
   *
   * KNOWN BUG: same root cause as "concurrent updates converge" —
   * server doesn't relay concurrent commits between browser clients
   * via the HTTP /sync + /events pipeline.
   */
  it.skip("fresh db sees converged state", async () => {
    const token = `hc-fresh-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const dbAlice = await createSyncedDb(ctx, "hc-alice-fresh", token);
    const dbBob = await createSyncedDb(ctx, "hc-bob-fresh", token);

    // Alice inserts a todo
    const originalTitle = `fresh-test-${Date.now()}`;
    const { id } = await withTimeout(
      dbAlice.insertDurable(todos, { title: originalTitle, done: false }, { tier: "worker" }),
      10000,
      "Alice insert did not resolve",
    );

    // Wait for Bob to see it
    await waitForQuery(
      dbBob,
      allTodos,
      (rows) => rows.some((row) => row.id === id),
      "Bob sees Alice's todo",
      20000,
    );

    // Both update concurrently — creates diverged tips (true conflict).
    await Promise.all([
      dbAlice.updateDurable(todos, id, { title: "alice-edit" }, { tier: "worker" }),
      dbBob.updateDurable(todos, id, { title: "bob-edit" }, { tier: "worker" }),
    ]);

    // Wait for convergence between Alice and Bob
    let convergedTitle = "";
    await waitForCondition(
      async () => {
        const aliceRows = await dbAlice.all(allTodos);
        const bobRows = await dbBob.all(allTodos);
        const aliceTodo = aliceRows.find((r) => r.id === id);
        const bobTodo = bobRows.find((r) => r.id === id);
        if (!aliceTodo || !bobTodo) return false;
        if (
          aliceTodo.title !== originalTitle &&
          bobTodo.title !== originalTitle &&
          aliceTodo.title === bobTodo.title
        ) {
          convergedTitle = aliceTodo.title;
          return true;
        }
        return false;
      },
      40000,
      "Alice and Bob should converge on same title",
    );

    // Charlie connects fresh — must see the same winner
    const dbCharlie = await createSyncedDb(ctx, "hc-charlie-fresh", token);

    const charlieRows = await waitForQuery(
      dbCharlie,
      allTodos,
      (rows) => rows.some((row) => row.id === id && row.title === convergedTitle),
      "Charlie sees converged title",
      20000,
    );
    const charlieTodo = charlieRows.find((r) => r.id === id);
    expect(charlieTodo?.title).toBe(convergedTitle);
  }, 120000);
});
