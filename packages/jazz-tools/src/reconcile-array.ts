import type { SubscriptionDelta } from "./runtime/subscription-manager.js";

/**
 * Apply a subscription delta to a reactive array, deep-merging only
 * the rows that actually changed.
 */
export function applyDelta<T extends { id: string }>(
  target: T[],
  delta: SubscriptionDelta<T>,
): void {
  const changedIds = new Set<string>();
  for (const change of delta.delta) {
    if (change.kind === 2) changedIds.add(change.id);
  }

  for (const id of changedIds) {
    const existing = target.find((item) => item.id === id);
    const source = delta.all.find((item) => item.id === id);
    if (existing && source) {
      deepMerge(existing as Record<string, unknown>, source as Record<string, unknown>);
    }
  }
  // Without reconciliation, ordering is not guaranteed.
  reconcileArray(target, delta.all);
}

/**
 * Reconcile a target array in-place to match a source array,
 * preserving object identity for items with matching `id` fields.
 *
 * Designed for reactive proxy systems (Svelte 5 $state, Vue ref) where
 * minimising property writes avoids unnecessary signal triggers.
 *
 * Assumptions (bounded by Jazz's data model):
 * - Source items are fresh objects from the WASM runtime, not shared
 *   references. deepMerge mutates `target` in-place, so structural
 *   sharing between source and target would corrupt the source.
 * - Objects are plain POJOs (no class instances, Map, Set, or cycles).
 *   isPlainObject excludes Date and Uint8Array as leaf values; anything
 *   else with a prototype would be incorrectly deep-merged field-by-field.
 * - Keyed arrays always contain objects with `id` at every index.
 *   isKeyedArray only checks the first element as a fast-path heuristic.
 */
export function reconcileArray<T extends { id: string }>(target: T[], source: T[]): void {
  const existing = new Map<string, T>();
  for (const item of target) {
    existing.set(item.id, item);
  }

  const result: T[] = [];
  for (const srcItem of source) {
    const prev = existing.get(srcItem.id);
    if (prev) {
      deepMerge(prev as Record<string, unknown>, srcItem as Record<string, unknown>);
      result.push(prev);
    } else {
      result.push(srcItem);
    }
  }

  for (let i = 0; i < result.length; i++) {
    if (target[i] !== result[i]) {
      target[i] = result[i]!;
    }
  }
  if (target.length > result.length) {
    target.length = result.length;
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    !(value instanceof Date) &&
    !(value instanceof Uint8Array)
  );
}

// Heuristic: checks only the first element. Safe because Jazz row objects
// always have `id` — mixed arrays (some with id, some without) don't occur.
function isKeyedArray(value: unknown[]): value is Array<{ id: string }> {
  return value.length > 0 && isPlainObject(value[0]) && "id" in value[0];
}

function valuesEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    return a.length === b.length && a.every((v, i) => v === b[i]);
  }
  return false;
}

// Recursive with no depth/cycle guard — Jazz row objects are shallow POJOs
// from the WASM runtime, so this is safe. Would stack-overflow on cyclic graphs.
function deepMerge(target: Record<string, unknown>, source: Record<string, unknown>): void {
  const sourceKeys = new Set(Object.keys(source));

  for (const key of sourceKeys) {
    const tv = target[key];
    const sv = source[key];

    if (valuesEqual(tv, sv)) continue;

    if (isPlainObject(tv) && isPlainObject(sv)) {
      deepMerge(tv, sv);
    } else if (Array.isArray(tv) && Array.isArray(sv) && isKeyedArray(sv)) {
      reconcileArray(tv as Array<{ id: string }>, sv as Array<{ id: string }>);
    } else {
      target[key] = sv;
    }
  }

  for (const key of Object.keys(target)) {
    if (!sourceKeys.has(key)) {
      delete target[key];
    }
  }
}
