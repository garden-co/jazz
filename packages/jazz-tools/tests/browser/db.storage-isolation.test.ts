import { afterEach, describe, expect, it } from "vitest";
import type { QueryBuilder, TableProxy } from "../../src/runtime/index.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import {
  acquireBrowserTestAccount,
  createBrowserTestDb,
  TestCleanup,
  sleep,
  waitForQuery,
} from "./support.js";

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

type Todo = {
  id: string;
  title: string;
  done: boolean;
};

type TodoInit = {
  title: string;
  done: boolean;
};

const todos: TableProxy<Todo, TodoInit> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as TodoInit,
};

const allTodos: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });
  },
};

async function expectRowsToStayEmpty(
  queryDb: Awaited<ReturnType<typeof createBrowserTestDb>>,
  durationMs: number,
): Promise<void> {
  const deadline = Date.now() + durationMs;

  while (Date.now() < deadline) {
    expect(await queryDb.all(allTodos)).toEqual([]);
    await sleep(100);
  }
}

describe("Db browser storage isolation", () => {
  const ctx = new TestCleanup();

  afterEach(async () => {
    await ctx.cleanup();
  });

  it("isolates default persistent storage by user_id while preserving same-user continuity", async () => {
    const appId = `browser-storage-isolation-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const alice = await acquireBrowserTestAccount({ appId, key: "alice" });
    const bob = await acquireBrowserTestAccount({ appId, key: "bob" });

    const aliceWriter = ctx.track(
      await createBrowserTestDb({
        appId,
        account: alice,
      }),
    );

    aliceWriter.insert(todos, { title: "alice-only", done: false });
    await waitForQuery(
      aliceWriter,
      allTodos,
      (rows) => rows.some((row) => row.title === "alice-only"),
      "alice should see her local row before restart",
    );

    await aliceWriter.shutdown();
    ctx.untrack(aliceWriter);
    await sleep(200);

    const bobReader = ctx.track(
      await createBrowserTestDb({
        appId,
        account: bob,
      }),
    );

    await expectRowsToStayEmpty(bobReader, 1500);

    await bobReader.shutdown();
    ctx.untrack(bobReader);
    await sleep(200);

    const aliceReader = ctx.track(
      await createBrowserTestDb({
        appId,
        account: alice,
      }),
    );

    await waitForQuery(
      aliceReader,
      allTodos,
      (rows) => rows.some((row) => row.title === "alice-only"),
      "alice should recover her local rows when reopening",
    );
  });

  it("logout with wipeData only clears the current user's scoped storage", async () => {
    const appId = `browser-storage-logout-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const alice = await acquireBrowserTestAccount({ appId, key: "alice" });
    const bob = await acquireBrowserTestAccount({ appId, key: "bob" });

    const aliceWriter = ctx.track(
      await createBrowserTestDb({
        appId,
        account: alice,
      }),
    );
    const bobWriter = ctx.track(
      await createBrowserTestDb({
        appId,
        account: bob,
      }),
    );

    aliceWriter.insert(todos, { title: "alice-only", done: false });
    bobWriter.insert(todos, { title: "bob-only", done: false });

    await waitForQuery(
      aliceWriter,
      allTodos,
      (rows) => rows.length === 1 && rows[0]?.title === "alice-only",
      "alice should only see alice rows before logout",
    );
    await waitForQuery(
      bobWriter,
      allTodos,
      (rows) => rows.length === 1 && rows[0]?.title === "bob-only",
      "bob should only see bob rows before alice logout",
    );

    await aliceWriter.logout({ wipeData: true });
    ctx.untrack(aliceWriter);
    await sleep(200);

    await waitForQuery(
      bobWriter,
      allTodos,
      (rows) => rows.length === 1 && rows[0]?.title === "bob-only",
      "bob should retain scoped rows after alice logout wipe",
    );

    const aliceReader = ctx.track(
      await createBrowserTestDb({
        appId,
        account: alice,
      }),
    );

    await expectRowsToStayEmpty(aliceReader, 1500);
  });
});
