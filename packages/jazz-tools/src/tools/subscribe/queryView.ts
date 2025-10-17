import type { CoList } from "../internal.js";
import {
  ItemsSym,
  isRefEncoded,
  accessChildLoadingStateByKey,
} from "../internal.js";

/**
 * When loading a CoValue, the view over it can be different from the actual CoValue's content.
 * The query view is a mapping that links the keys in the CoValue query view to the keys in the raw CoValue.
 */
export type QueryView = Record<number, number>;

export function computeQueryView(value: CoList): QueryView {
  const items = accessibleItems(value);
  return Object.fromEntries(items.map((rawIndex, idx) => [idx, rawIndex]));
}

/**
 * Get the keys of the CoValue that are loaded and accessible.
 */
function accessibleItems(coValue: CoList): number[] {
  const allItems = coValue.$jazz.raw.entries().map((_entry, i) => i);
  if (!isRefEncoded(coValue.$jazz.schema[ItemsSym])) {
    return allItems;
  }
  return allItems.filter((rawIndex) => isItemAccessible(coValue, rawIndex));
}

function isItemAccessible(coValue: CoList, key: number): boolean {
  const rawValue = coValue.$jazz.raw.get(key) as string | null;
  // Omit null references
  if (rawValue === null) {
    return false;
  }
  // CoList elements have already been loaded by the subscription scope.
  // Omit inaccessible elements from the query view.
  const isLoaded =
    accessChildLoadingStateByKey(coValue, rawValue, String(key))?.type ===
    "loaded";
  return isLoaded;
}
