import { describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import type { Session } from "../runtime/context.js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { SubscriptionsOrchestrator, computeQueryKey } from "../subscriptions-orchestrator.js";
import { applySnapshot } from "./apply-snapshot.js";
import type { DehydratedSnapshot } from "../backend/ssr.js";

type Todo = { id: string; title: string };

const SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "id", column_type: { type: "Text" }, nullable: false },
      { name: "title", column_type: { type: "Text" }, nullable: false },
    ],
  },
};

function makeQuery(): QueryBuilder<Todo> {
  return {
    _table: "todos",
    _schema: {},
    _rowType: {} as Todo,
    _build() {
      return JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] });
    },
  };
}

function makeOrchestrator(appId: string): SubscriptionsOrchestrator {
  const db = {
    subscribeAll<T extends { id: string }>(
      _query: QueryBuilder<T>,
      _callback: (delta: SubscriptionDelta<T>) => void,
      _options?: QueryOptions,
      _session?: Session,
    ): () => void {
      return () => {};
    },
  };
  return new SubscriptionsOrchestrator({ appId }, db);
}

describe("applySnapshot", () => {
  it("seeds the orchestrator so a subsequent makeQueryKey hits a fulfilled entry", () => {
    const manager = makeOrchestrator("app-x");
    const query = makeQuery();
    const key = computeQueryKey("app-x", query);
    const snapshot: DehydratedSnapshot = {
      appId: "app-x",
      principalId: null,
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key, result: [{ id: "1", title: "seeded" }] }],
    };

    const outcome = applySnapshot({
      manager,
      snapshot,
      expected: {
        appId: "app-x",
        principalId: null,
        schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      },
    });

    expect(outcome).toBe("applied");

    manager.makeQueryKey(query);
    const entry = manager.getCacheEntry<Todo>(key);
    expect(entry.status).toBe("fulfilled");
    expect(entry.state).toEqual({
      status: "fulfilled",
      data: [{ id: "1", title: "seeded" }],
      error: null,
    });
  });

  it("discards the snapshot when appId mismatches", () => {
    const manager = makeOrchestrator("app-real");
    const snapshot: DehydratedSnapshot = {
      appId: "app-stale",
      principalId: null,
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key: "app-stale:{}:irrelevant", result: [] }],
    };

    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const outcome = applySnapshot({
      manager,
      snapshot,
      expected: {
        appId: "app-real",
        principalId: null,
        schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      },
    });

    expect(outcome).toBe("appId-mismatch");
    expect((manager as any).queryDefinitions.size).toBe(0);
    expect(warn).toHaveBeenCalledOnce();
    warn.mockRestore();
  });

  it("throws when a user-scoped snapshot targets a different live principal", () => {
    const manager = makeOrchestrator("app-y");
    const snapshot: DehydratedSnapshot = {
      appId: "app-y",
      principalId: "user-old",
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key: "app-y:{}:x", result: [] }],
    };

    expect(() =>
      applySnapshot({
        manager,
        snapshot,
        expected: {
          appId: "app-y",
          principalId: "user-new",
          schemaFingerprint: computeSchemaFingerprint(SCHEMA),
        },
      }),
    ).toThrow(/refusing to seed/);
    expect((manager as any).queryDefinitions.size).toBe(0);
  });

  it("seeds a user-scoped snapshot even when no live principal is known yet (display is not gated)", () => {
    const manager = makeOrchestrator("app-seed");
    const query = makeQuery();
    const key = computeQueryKey("app-seed", query);
    const snapshot: DehydratedSnapshot = {
      appId: "app-seed",
      principalId: "user-old",
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key, result: [{ id: "1", title: "scoped" }] }],
    };

    const outcome = applySnapshot({
      manager,
      snapshot,
      expected: {
        appId: "app-seed",
        principalId: null,
        schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      },
    });

    expect(outcome).toBe("applied");

    manager.makeQueryKey(query);
    const entry = manager.getCacheEntry<Todo>(key);
    expect(entry.status).toBe("fulfilled");
    expect(entry.state).toEqual({
      status: "fulfilled",
      data: [{ id: "1", title: "scoped" }],
      error: null,
    });
  });

  it("seeds a null-principal (public) snapshot into an authenticated session", () => {
    const manager = makeOrchestrator("app-pub");
    const query = makeQuery();
    const key = computeQueryKey("app-pub", query);
    const snapshot: DehydratedSnapshot = {
      appId: "app-pub",
      principalId: null,
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key, result: [{ id: "1", title: "public" }] }],
    };

    const outcome = applySnapshot({
      manager,
      snapshot,
      expected: {
        appId: "app-pub",
        principalId: "user-abc",
        schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      },
    });

    expect(outcome).toBe("applied");

    manager.makeQueryKey(query);
    const entry = manager.getCacheEntry<Todo>(key);
    expect(entry.status).toBe("fulfilled");
    expect(entry.state).toEqual({
      status: "fulfilled",
      data: [{ id: "1", title: "public" }],
      error: null,
    });
  });

  it("discards the snapshot when schemaFingerprint mismatches", () => {
    const manager = makeOrchestrator("app-z");
    const snapshot: DehydratedSnapshot = {
      appId: "app-z",
      principalId: null,
      schemaFingerprint: "deadbeef",
      entries: [{ key: "app-z:{}:x", result: [] }],
    };

    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const outcome = applySnapshot({
      manager,
      snapshot,
      expected: {
        appId: "app-z",
        principalId: null,
        schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      },
    });

    expect(outcome).toBe("schema-mismatch");
    expect((manager as any).queryDefinitions.size).toBe(0);
    expect(warn).toHaveBeenCalledOnce();
    warn.mockRestore();
  });

  it("is a no-op when snapshot is undefined", () => {
    const manager = makeOrchestrator("app-q");
    const outcome = applySnapshot({
      manager,
      snapshot: undefined,
      expected: {
        appId: "app-q",
        principalId: null,
        schemaFingerprint: "0",
      },
    });

    expect(outcome).toBe("no-snapshot");
    expect((manager as any).queryDefinitions.size).toBe(0);
  });
});
