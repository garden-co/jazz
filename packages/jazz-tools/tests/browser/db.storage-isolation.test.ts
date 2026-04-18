import { afterEach, describe, expect, it } from "vitest";
import { createDb, type QueryBuilder, type TableProxy } from "../../src/runtime/index.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { TestCleanup, sleep, waitForQuery } from "./support.js";

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
  queryDb: Awaited<ReturnType<typeof createDb>>,
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
    const aliceJwt = makeFakeJwt({
      sub: "alice",
      claims: { role: "member" },
      exp: Math.floor(Date.now() / 1000) + 3600,
    });
    const bobJwt = makeFakeJwt({
      sub: "bob",
      claims: { role: "member" },
      exp: Math.floor(Date.now() / 1000) + 3600,
    });

    const aliceWriter = ctx.track(
      await createDb({
        appId,
        jwtToken: aliceJwt,
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
      await createDb({
        appId,
        jwtToken: bobJwt,
      }),
    );

    await expectRowsToStayEmpty(bobReader, 1500);

    await bobReader.shutdown();
    ctx.untrack(bobReader);
    await sleep(200);

    const aliceReader = ctx.track(
      await createDb({
        appId,
        jwtToken: aliceJwt,
      }),
    );

    await waitForQuery(
      aliceReader,
      allTodos,
      (rows) => rows.some((row) => row.title === "alice-only"),
      "alice should recover her local rows when reopening",
    );
  });
});

function toBase64Url(value: unknown): string {
  return btoa(JSON.stringify(value)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeFakeJwt(payload: Record<string, unknown>): string {
  return `${toBase64Url({ alg: "HS256", typ: "JWT" })}.${toBase64Url(payload)}.bad-signature`;
}
