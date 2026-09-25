import {
  normalizeRowDelta,
  resultIdentity,
  RowChangeKind,
  type RowDelta,
  type SubscriptionDelta,
} from "./runtime/subscription-manager.js";

/**
 * Apply a subscription delta to a reactive array, deep-merging only
 * the rows that actually changed.
 *
 * A non-reset delta is applied change by change, so a one-row change costs
 * O(1) row merges instead of reconciling every row. Matched rows keep their
 * identity and are deep-merged, exactly as {@link reconcileArray} would
 * leave them. When the delta carries `all`, that is the authority for row
 * content: each changed row is merged from its entry in `all`, and if the
 * length or any changed row's position disagrees with `all`, the target is
 * reconciled against `all` in full instead.
 */
export function applyDelta<T extends { id: string }>(
  target: T[],
  delta: SubscriptionDelta<T>,
): void {
  if (delta.reset) {
    reconcileArray(target, delta.all);
    return;
  }

  const changes = normalizeRowDelta(delta.delta);
  const all = delta.all;
  if (all !== undefined && exceedsStructuralSplices(target, changes)) {
    // Each insert, removal or move is one splice, and a splice on a reactive
    // array (Vue, Svelte) rewrites every later index, so many of them in one
    // frame cost far more than one full reconcile.
    reconcileArray(target, all);
    return;
  }
  applyRowChanges(target, changes, all === undefined);
  if (all !== undefined && !mergeChangedRowsFrom(target, all, changes)) {
    reconcileArray(target, all);
  }
}

/**
 * Above this many splices in one frame, a full reconcile against `all` is
 * cheaper on a reactive array than applying the splices one by one.
 */
const MAX_STRUCTURAL_SPLICES = 8;

/**
 * Whether the changes need more than {@link MAX_STRUCTURAL_SPLICES} splices.
 * An update counts unless its row already sits at its index; checking only
 * the hinted slot keeps this O(1) per change, so it may over-count moves.
 */
function exceedsStructuralSplices<T extends { id: string }>(
  target: T[],
  changes: RowDelta<T>[],
): boolean {
  if (changes.length <= MAX_STRUCTURAL_SPLICES) return false;
  let splices = 0;
  for (const change of changes) {
    if (change.kind === RowChangeKind.Updated) {
      const hinted = target[change.index];
      if (hinted !== undefined && resultIdentity(hinted) === change.id) continue;
    }
    if (++splices > MAX_STRUCTURAL_SPLICES) return true;
  }
  return false;
}

/**
 * Apply the changes' structure (inserts, removals, moves) in delta order.
 * Rows that are already present keep their identity; their content is
 * merged here only when `mergeItems` is set.
 */
function applyRowChanges<T extends { id: string }>(
  target: T[],
  changes: RowDelta<T>[],
  mergeItems: boolean,
): void {
  for (const change of changes) {
    const position = locate(target, change.id, change.index);
    switch (change.kind) {
      case RowChangeKind.Added:
      case RowChangeKind.Updated: {
        const previous = position === -1 ? undefined : target[position];
        const next = previous ?? change.item;
        if (next === undefined) break;
        if (mergeItems && previous !== undefined && change.item !== undefined) {
          deepMerge(previous as Record<string, unknown>, change.item as Record<string, unknown>);
        }
        if (position === -1) {
          target.splice(clampIndex(change.index, target.length), 0, next);
          break;
        }
        const index = clampIndex(change.index, target.length - 1);
        if (index !== position) {
          target.splice(position, 1);
          target.splice(index, 0, next);
        }
        break;
      }
      case RowChangeKind.Removed:
        if (position !== -1) target.splice(position, 1);
        break;
    }
  }
}

/**
 * Merge every added or updated row from its entry in `all`. Returns false,
 * leaving the rest to a full reconcile, when the target's length or a
 * changed row's position disagrees with `all`.
 */
function mergeChangedRowsFrom<T extends { id: string }>(
  target: T[],
  all: T[],
  changes: RowDelta<T>[],
): boolean {
  if (target.length !== all.length) return false;
  for (const change of changes) {
    if (change.kind === RowChangeKind.Removed) continue;
    const position = locate(target, change.id, change.index);
    const source = all[position];
    if (position === -1 || source === undefined || resultIdentity(source) !== change.id) {
      return false;
    }
    const current = target[position]!;
    if (current !== source) {
      deepMerge(current as Record<string, unknown>, source as Record<string, unknown>);
    }
  }
  return true;
}

/** Position of `id`, trying the delta's index before scanning the array. */
function locate<T extends { id: string }>(target: T[], id: string, hint: number): number {
  const hinted = target[hint];
  if (hinted !== undefined && resultIdentity(hinted) === id) return hint;
  return target.findIndex((item) => resultIdentity(item) === id);
}

function clampIndex(index: number, length: number): number {
  return Math.max(0, Math.min(index, length));
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
    // splice, not `length =`: reactive proxies (e.g. deepsignal) leave the
    // dropped per-index signals stale on a length truncation, so reads past
    // the new length would return old rows.
    target.splice(result.length);
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
