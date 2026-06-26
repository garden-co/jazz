import { afterEach, describe, expect, it } from "vitest";
import {
  createDb,
  generateAuthSecret,
  publishStoredPermissions,
  schema,
  type CompiledPermissions,
  type Db,
  type RowOf,
} from "../../src/index.js";
import { fetchPermissionsHead, publishStoredSchema } from "../../src/runtime/schema-fetch.js";
import {
  TestCleanup,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
  withTimeout,
} from "./support.js";
import { getJazzServerInfo } from "./testing-server.js";

const app = schema.defineApp({
  todos: schema.table({
    title: schema.string(),
    done: schema.boolean(),
    list: schema.string(),
  }),
});

const permissions = schema.definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

type Todo = RowOf<typeof app.todos>;

const ctx = new TestCleanup();

afterEach(async () => {
  await ctx.cleanup();
});

describe("alpha public package flow", () => {
  it("opens public createDb with persistent OPFS and direct websocket server config, then converges todo CRUD", async () => {
    const requestedAppId = uniqueDbName("alpha-public-flow");
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(requestedAppId);
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const sharedSecret = generateAuthSecret();
    const db = await openAlphaDb(appId, serverUrl, adminSecret, "alpha-public-a", sharedSecret);
    const snapshots: Todo[][] = [];
    const unsubscribe = ctx.trackSubscription(
      db.subscribeAll(app.todos.orderBy("title"), (delta) => {
        snapshots.push([...delta.all]);
      }),
    );

    const created = db.insert(app.todos, {
      title: "Adopt alpha public flow",
      done: false,
      list: "launch",
    });
    const createdRow = await withTimeout(
      created.wait({ tier: "edge" }),
      10_000,
      "initial insert was not accepted at the server",
    );
    await expectTodoTitles(db, snapshots, ["Adopt alpha public flow"]);

    const second = db.insert(app.todos, {
      title: "Prove public imports",
      done: false,
      list: "launch",
    });
    const secondRow = await withTimeout(
      second.wait({ tier: "edge" }),
      10_000,
      "second insert was not accepted at the server",
    );
    await withTimeout(
      db.update(app.todos, createdRow.id, { done: true }).wait({ tier: "edge" }),
      10_000,
      "update was not accepted at the server",
    );
    await expectTodoSummaries(db, ["Adopt alpha public flow:done", "Prove public imports:open"]);

    await withTimeout(
      db.delete(app.todos, secondRow.id).wait({ tier: "edge" }),
      10_000,
      "delete was not accepted at the server",
    );
    await expectTodoSummaries(db, ["Adopt alpha public flow:done"]);
    const dbB = await openAlphaDb(appId, serverUrl, adminSecret, "alpha-public-b", sharedSecret);
    const rowsOnB = await waitForSubscribedTodoSummaries(dbB, ["Adopt alpha public flow:done"]);
    expect(rowsOnB.some((todo) => todo.id === createdRow.id && todo.done)).toBe(true);
    expect((await dbB.all(app.todos)).some((todo) => todo.id === secondRow.id)).toBe(false);

    unsubscribe();
    expect(snapshots.some((rows) => rows.length === 0)).toBe(true);
    expect(
      snapshots.some((rows) =>
        rows.some((todo) => todo.title === "Adopt alpha public flow" && todo.done),
      ),
    ).toBe(true);
  });
});

async function openAlphaDb(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  label: string,
  secret: string,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      serverUrl,
      adminSecret,
      secret,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
    }),
  );
}

async function publishSchemaAndPermissions(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  permissions: CompiledPermissions,
): Promise<void> {
  const { hash: schemaHash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema: app.wasmSchema,
  });
  const { head } = await fetchPermissionsHead(serverUrl, {
    appId,
    adminSecret,
  });
  await publishStoredPermissions(serverUrl, {
    appId,
    adminSecret,
    schemaHash,
    permissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });
}

async function expectTodoTitles(db: Db, snapshots: Todo[][], titles: string[]): Promise<void> {
  const rows = await waitForQuery(
    db,
    app.todos.orderBy("title"),
    (todos) => titlesEqual(todos, titles),
    `todos converge to ${titles.join(", ")}`,
    15_000,
  );
  expect(rows.map((todo) => todo.title)).toEqual(titles);
  await waitForCondition(
    async () => snapshots.some((todos) => titlesEqual(todos, titles)),
    5_000,
    `subscription snapshot for ${titles.join(", ")}`,
  );
}

async function expectTodoSummaries(
  db: Db,
  summaries: string[],
  tier?: "local" | "edge",
): Promise<void> {
  const rows = await waitForQuery(
    db,
    app.todos.orderBy("title"),
    (todos) => summariesEqual([...todos].sort(byTitle), summaries),
    `todos converge to ${summaries.join(", ")}`,
    15_000,
    tier,
  );
  expect([...rows].sort(byTitle).map(summary)).toEqual(summaries);
}

async function waitForSubscribedTodoSummaries(db: Db, summaries: string[]): Promise<Todo[]> {
  return await new Promise<Todo[]>((resolve, reject) => {
    let lastRows: Todo[] = [];
    let unsubscribe: () => void = () => {};
    const timeout = setTimeout(() => {
      unsubscribe();
      reject(
        new Error(
          `subscription snapshot for ${summaries.join(", ")} timed out; ` +
            `lastRows=${JSON.stringify(lastRows.slice(0, 10))}`,
        ),
      );
    }, 15_000);
    unsubscribe = ctx.trackSubscription(
      db.subscribeAll(app.todos.orderBy("title"), (delta) => {
        lastRows = [...delta.all];
        if (summariesEqual([...lastRows].sort(byTitle), summaries)) {
          clearTimeout(timeout);
          unsubscribe();
          resolve(lastRows);
        }
      }),
    );
  });
}

function titlesEqual(rows: Todo[], titles: string[]): boolean {
  return rows.map((todo) => todo.title).join("\n") === titles.join("\n");
}

function summariesEqual(rows: Todo[], summaries: string[]): boolean {
  return rows.map(summary).join("\n") === summaries.join("\n");
}

function summary(todo: Todo): string {
  return `${todo.title}:${todo.done ? "done" : "open"}`;
}

function byTitle(left: Todo, right: Todo): number {
  return left.title.localeCompare(right.title);
}
