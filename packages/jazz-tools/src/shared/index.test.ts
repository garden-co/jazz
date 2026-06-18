import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createDb } from "../runtime/db.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
// The public surface under test. The vanilla binding below imports Jazz from
// here and nowhere else — if any symbol it needs were missing, this file would
// not compile, which is the proof the surface is complete.
import { applyDelta, reconcileArray, RowChangeKind } from "./index.js";
import type {
  CacheEntryHandle,
  QueryBuilder,
  QueryOptions,
  RowDelta,
  SubscriptionDelta,
  SubscriptionsOrchestrator as Orchestrator,
  UseAllState,
} from "./index.js";

// ---------------------------------------------------------------------------
// A minimal framework-agnostic binding — the same job the React/Svelte/Vue
// `useAll` bindings do, with no framework — built ONLY on `jazz-tools/shared`.
// ---------------------------------------------------------------------------

interface VanillaResult<T extends { id: string }> {
  /** The live result set, reconciled in place across deltas. */
  readonly current: T[];
  /** Count of `Updated` row-changes seen, to exercise the delta-kind surface. */
  updatedRowCount: number;
  stop(): void;
}

function useAllVanilla<T extends { id: string }>(
  manager: Orchestrator,
  query: QueryBuilder<T>,
  options?: QueryOptions,
): VanillaResult<T> {
  const current: T[] = [];
  const result: VanillaResult<T> = { current, updatedRowCount: 0, stop: () => {} };

  // Render-safe peek: seed from cache without registering or subscribing.
  const initial: UseAllState<T> = manager.peekState<T>(manager.computeKey(query, options));
  if (initial.status === "fulfilled") {
    reconcileArray(current, initial.data);
  }

  const key = manager.makeQueryKey(query, options);
  const entry: CacheEntryHandle<T> = manager.getCacheEntry<T>(key);

  result.stop = entry.subscribe({
    onfulfilled: (data: T[]) => {
      reconcileArray(current, data);
    },
    onDelta: (delta: SubscriptionDelta<T>) => {
      result.updatedRowCount += delta.delta.filter(
        (change: RowDelta<T>) => change.kind === RowChangeKind.Updated,
      ).length;
      applyDelta(current, delta);
    },
    onError: () => {},
    onReset: () => {
      current.length = 0;
    },
  });

  return result;
}

// ---------------------------------------------------------------------------

async function waitFor(
  predicate: () => boolean,
  message: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(message);
}

describe("jazz-tools/shared", () => {
  it("a binding built only on the public surface tracks a live query and reconciles in place", async () => {
    const appId = `shared-surface-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const db = await createDb({ appId });
    const manager = new SubscriptionsOrchestrator({ appId }, db);

    // Schema + query via the real builder API (no JSON-shaped literals).
    const app = s.defineApp({
      todos: s.table({ title: s.string(), done: s.boolean() }),
    });
    const query = app.todos.where({ done: { eq: false } });

    const binding = useAllVanilla(manager, query);

    try {
      const { value: first } = await db.insert(app.todos, { title: "first", done: false });
      await waitFor(
        () => binding.current.some((row) => row.id === first.id),
        "expected the first inserted row to reach the binding",
      );

      // Capture the live object so we can assert it is mutated in place, not replaced.
      const firstRef = binding.current.find((row) => row.id === first.id)!;

      const { value: second } = await db.insert(app.todos, { title: "second", done: false });
      await waitFor(
        () => binding.current.length === 2,
        "expected the second inserted row to reach the binding",
      );
      // The first row's identity survived the delta — proof of in-place reconciliation.
      expect(binding.current.find((row) => row.id === first.id)).toBe(firstRef);

      await db.update(app.todos, first.id, { title: "first-edited" });
      await waitFor(
        () => binding.current.find((row) => row.id === first.id)?.title === "first-edited",
        "expected the update to merge into the binding",
      );

      // Same object reference, field merged in place; the update was seen as `Updated`.
      expect(binding.current.find((row) => row.id === first.id)).toBe(firstRef);
      expect(firstRef.title).toBe("first-edited");
      expect(binding.updatedRowCount).toBeGreaterThan(0);
      expect(binding.current.map((row) => row.id).sort()).toEqual([first.id, second.id].sort());
    } finally {
      binding.stop();
      await manager.shutdown();
      await db.shutdown();
    }
  });
});
