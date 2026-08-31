import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { Db, getDbSubscriptionSource, type DbConfig } from "./db.js";
import { isInspectorLocalQueryOptions } from "../internal/inspector-query.js";
import type { JazzClient, Row } from "./client.js";
import { RuntimeSource, type RuntimeClientContext } from "./runtime-source.js";
import type { WasmSchema } from "../drivers/types.js";

const todoSchema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};
type TodoSchema = s.Schema<typeof todoSchema>;
const app: s.App<TodoSchema> = s.defineApp(todoSchema);

const todoRow: Row = {
  id: "todo-1",
  values: [
    { type: "Text", value: "Buy milk" },
    { type: "Boolean", value: false },
  ],
};

type QuerySpy = ReturnType<typeof vi.fn<(queryJson: string, options?: unknown) => Promise<Row[]>>>;

class TestRuntimeSource extends RuntimeSource<DbConfig> {
  constructor(private readonly client: JazzClient) {
    super();
  }

  override createClient(_context: RuntimeClientContext<DbConfig>): JazzClient {
    return this.client;
  }
}

class TestDb extends Db {
  constructor(client: JazzClient, config: DbConfig = { appId: "db-one-limit-test" }) {
    super(config, new TestRuntimeSource(client));
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return super.getClient(_schema);
  }
}

function makeClient() {
  const query = vi.fn(async (_queryJson: string, _options?: unknown) => [todoRow]);
  const beginTransaction = vi.fn(() => "00000000000070008000000000000001");
  const client = {
    getSchema: () => new Map(Object.entries(app.wasmSchema)),
    query,
    queryInternal: query,
    beginTransaction,
    onMutationError: vi.fn(),
  } as unknown as JazzClient;

  return { client, query, beginTransaction };
}

function firstQueryJson(query: QuerySpy): string {
  const firstCall = query.mock.calls[0];
  expect(firstCall).toBeDefined();
  return firstCall![0] as string;
}

function rootLimit(queryJson: string): number | undefined {
  const parsed = JSON.parse(queryJson) as {
    limit?: unknown;
  };
  const limit = parsed.limit;
  return typeof limit === "number" ? limit : undefined;
}

describe("Db.one", () => {
  it("adds limit 1 before executing a query without a limit", async () => {
    const { client, query } = makeClient();
    const db = new TestDb(client);

    await db.one(app.todos.where({ done: false }));

    expect(rootLimit(firstQueryJson(query))).toBe(1);
  });

  it("narrows explicit limits above one", async () => {
    const { client, query } = makeClient();
    const db = new TestDb(client);

    await db.one(app.todos.limit(10));

    expect(rootLimit(firstQueryJson(query))).toBe(1);
  });

  it("overrides explicit limit 0", async () => {
    const { client, query } = makeClient();
    const db = new TestDb(client);

    await db.one(app.todos.limit(0));

    expect(rootLimit(firstQueryJson(query))).toBe(1);
  });

  it("adds limit 1 before executing an explicit mergeable transaction query", async () => {
    const { client, query, beginTransaction } = makeClient();
    const db = new TestDb(client);
    const tx = db.beginTransaction();

    await tx.one(app.todos.where({ done: false }));

    expect(beginTransaction).toHaveBeenCalledWith("mergeable", undefined, undefined);
    expect(rootLimit(firstQueryJson(query))).toBe(1);
    expect(query.mock.calls[0]?.[1]).toMatchObject({
      openTransactionId: "00000000000070008000000000000001",
    });
  });

  it("lowers public options before adding transaction-owned read controls", async () => {
    const { client, query } = makeClient();
    const db = new TestDb(client);
    const tx = db.beginTransaction();

    await tx.all(app.todos.where({ done: false }), {
      tier: "local",
      // JavaScript callers can supply these despite their absence from the
      // public type. They must not override transaction semantics.
      propagation: "local-only",
      localUpdates: "visible",
      openTransactionId: "forged-open-transaction",
      runtimeSettledTier: "global",
    } as any);

    expect(query.mock.calls[0]?.[1]).toEqual({
      tier: "local",
      localUpdates: "deferred",
      openTransactionId: "00000000000070008000000000000001",
    });
  });

  it("mints local-only read options only for an Inspector control-port attachment", async () => {
    const { client, query } = makeClient();
    const ordinary = new TestDb(client);
    expect(getDbSubscriptionSource(ordinary).prepareQueryOptions).toBeUndefined();

    // The host publishes this coordinate before an Inspector asks the worker
    // to attach a peer. It is not itself a local-read authority.
    const hostCoordinateOnly = new TestDb(client, {
      appId: "db-one-host-coordinate-only",
      runtimeSources: { inspectorHostPhysicalDbName: "jazz-host-coordinate" },
    });
    expect(getDbSubscriptionSource(hostCoordinateOnly).prepareQueryOptions).toBeUndefined();

    const attached = new TestDb(client, {
      appId: "db-one-inspector-attachment",
      runtimeSources: {
        browserWorkerPort: {} as MessagePort,
        inspectorHostPhysicalDbName: "jazz-inspector-authenticated-context",
      },
    });
    const source = getDbSubscriptionSource(attached);
    const options = source.prepareQueryOptions?.({ tier: "local-first" });

    expect(isInspectorLocalQueryOptions(options)).toBe(true);
    expect(options).toMatchObject({ tier: "local-first" });

    await source.all!(app.todos.where({ done: false }), { tier: "remote" });
    expect(query.mock.calls[0]?.[1]).toMatchObject({ tier: "local-only" });
  });
});
