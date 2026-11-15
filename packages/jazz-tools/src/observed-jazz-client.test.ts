import { describe, expect, it, vi } from "vitest";
import type { DbDeltaSubscriptionCallbacks, QueryBuilder } from "./runtime/db.js";
import type { LinkStatus, SyncStatus } from "./runtime/sync-status.js";
import { createObservedJazzClient } from "./observed-jazz-client.js";
import { getSubscriptionStore } from "./subscription-store-internal.js";

vi.mock("./runtime/db.js", () => ({
  getDbSubscriptionSource: (db: unknown) => db,
}));

type Todo = { id: string; title: string };

const query: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: {},
  _rowType: {} as Todo,
  _build: () => JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] }),
} as unknown as QueryBuilder<Todo>;

function status(link: LinkStatus): SyncStatus {
  return {
    link,
    pendingUploads: 0,
    oldestPendingAt: null,
    rejected: 0,
    lastError: null,
    synced: true,
  } as SyncStatus;
}

function fakeDb() {
  let current = status({ state: "connected" });
  const statusListeners = new Set<(status: SyncStatus) => void>();
  const subscriptions: DbDeltaSubscriptionCallbacks<Todo>[] = [];
  const db = {
    getAuthState: () => ({ authMode: "local-first" as const, session: null }),
    onAuthChanged: () => () => {},
    getSyncStatus: () => current,
    onSyncStatusChange(listener: (status: SyncStatus) => void) {
      statusListeners.add(listener);
      return () => statusListeners.delete(listener);
    },
    subscribeDelta(_query: QueryBuilder<Todo>, callbacks: DbDeltaSubscriptionCallbacks<Todo>) {
      subscriptions.push(callbacks);
      return () => {};
    },
    shutdown: vi.fn(async () => {}),
  };
  return {
    db,
    subscriptions,
    setLink(link: LinkStatus) {
      current = status(link);
      for (const listener of statusListeners) listener(current);
    },
  };
}

describe("createObservedJazzClient", () => {
  it("retries a mounted reader's rejected subscription once the link connects again", async () => {
    const fake = fakeDb();
    const client = await createObservedJazzClient(fake.db as never, "observed-retry");
    const store = getSubscriptionStore(client);
    const entry = store.getCacheEntry<Todo>(store.makeQueryKey(query));
    const onError = vi.fn();
    const onfulfilled = vi.fn();
    const unsubscribe = entry.subscribe({ onError, onfulfilled });

    // The server's authority changed: the link stops and the subscription ends.
    fake.setLink({ state: "offline", reason: "authority-changed", retrying: false });
    const refusal = Object.assign(new Error("authority changed"), { code: "authority_changed" });
    fake.subscriptions[0]!.onError?.(refusal);
    expect(entry.status).toBe("rejected");

    // `db.reconnect()` to the old server: the reader gets its rows back.
    fake.setLink({ state: "connected" });
    expect(fake.subscriptions).toHaveLength(2);
    fake.subscriptions[1]!.onDelta({ all: [{ id: "1", title: "one" }], delta: [] });
    expect(entry.status).toBe("fulfilled");
    expect(onfulfilled).toHaveBeenLastCalledWith([{ id: "1", title: "one" }]);

    // Staying connected retries nothing.
    fake.setLink({ state: "connected" });
    expect(fake.subscriptions).toHaveLength(2);

    unsubscribe();
    await client.shutdown();
  });
});
