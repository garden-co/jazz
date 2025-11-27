import type { CoValue, LoadedAndRequired, MaybeLoaded } from "../internal.js";

export function isCoValueId(id: unknown): id is `co_z${string}` {
  return typeof id === "string" && id.startsWith("co_z");
}

/**
 * Assert that a CoValue is loaded, otherwise throw an error
 */
export function assertLoaded<T extends MaybeLoaded<CoValue>>(
  coValue: T,
): asserts coValue is LoadedAndRequired<T> {
  if (!coValue.$isLoaded) {
    throw new Error("CoValue is not loaded");
  }
}

/**
 * Get a loaded CoValue or `undefined` if it is not loaded.
 *
 * Can be used to ease the transition between nullable loading states from Jazz 0.18
 * and explicit loading states from Jazz 0.19.
 */
export function getLoadedOrUndefined<T extends MaybeLoaded<CoValue>>(
  coValue: T | undefined | null,
): LoadedAndRequired<T> | undefined {
  if (!coValue?.$isLoaded) {
    return undefined;
  }

  return coValue as LoadedAndRequired<T>;
}
