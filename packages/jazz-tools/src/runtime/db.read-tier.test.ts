import { afterEach, describe, expect, it, vi } from "vitest";
import { ReadTier, type JazzClient } from "./client.js";
import { createDbWithRuntimeSource, type Db, type DbConfig, type QueryBuilder } from "./db.js";
import { RuntimeSource, type RuntimeClientContext } from "./runtime-source.js";
import type { WasmSchema } from "../drivers/types.js";

const schema: WasmSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
};

function query(): QueryBuilder<{ id: string; title: string }> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as { id: string; title: string },
    _build: () => JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] }),
  };
}

class TestRuntimeSource extends RuntimeSource<DbConfig> {
  constructor(private readonly client: JazzClient) {
    super();
  }

  override createClient(_context: RuntimeClientContext<DbConfig>): JazzClient {
    return this.client;
  }
}

function makeClient() {
  let nextSubscription = 1;
  return {
    connectTransport: vi.fn(),
    disconnectTransport: vi.fn(async () => undefined),
    onMutationError: vi.fn(),
    query: vi.fn(async () => []),
    subscribe: vi.fn(() => nextSubscription++),
    unsubscribe: vi.fn(),
    shutdown: vi.fn(async () => undefined),
  } as unknown as JazzClient & {
    query: ReturnType<typeof vi.fn>;
    subscribe: ReturnType<typeof vi.fn>;
    unsubscribe: ReturnType<typeof vi.fn>;
  };
}

const dbs: Db[] = [];

afterEach(async () => {
  while (dbs.length > 0) await dbs.pop()!.shutdown();
});

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

describe("Db ReadTier.RemoteIfPossible", () => {
  it("chooses local once for one-shot reads during an explicit disconnect", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-one-shot",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);

    await db.disconnect();
    await db.all(query(), { tier: ReadTier.RemoteIfPossible });
    await db.reconnect();

    expect(client.query).toHaveBeenCalledOnce();
    expect(client.query.mock.calls[0]?.[1]).toMatchObject({ tier: "local" });
  });

  it("does not fall back to local when an edge read fails or times out", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-transport-error",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    const connection = (
      db as unknown as { connection: { ensureReady: (tier?: string) => Promise<void> } }
    ).connection;
    const timeout = new Error("edge transport timed out");
    vi.spyOn(connection, "ensureReady").mockRejectedValue(timeout);

    await expect(db.all(query(), { tier: ReadTier.RemoteIfPossible })).rejects.toBe(timeout);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("replaces an explicitly-offline local subscription with edge exactly on reconnect", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-subscription",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);

    await db.disconnect();
    const unsubscribe = db.subscribeAll(query(), () => undefined, {
      tier: ReadTier.RemoteIfPossible,
    });

    expect(client.subscribe).toHaveBeenCalledOnce();
    expect(client.subscribe.mock.calls[0]?.[2]).toMatchObject({ tier: "local" });

    await db.reconnect();
    await settle();

    expect(client.unsubscribe).toHaveBeenCalledWith(1);
    expect(client.subscribe).toHaveBeenCalledTimes(2);
    expect(client.subscribe.mock.calls[1]?.[2]).toMatchObject({ tier: "edge" });

    unsubscribe();
  });
});
