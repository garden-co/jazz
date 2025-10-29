import { CoreResolveQuery } from "./zodSchema/schemaTypes/CoValueSchema.js";

/**
 * Remove getters from an object
 *
 * @param obj - The object to remove getters from.
 * @returns A new object with the getters removed.
 */
export function removeGetters<T extends object>(obj: T): Partial<T> {
  const result: any = {};

  for (const key of Object.keys(obj)) {
    const descriptor = Object.getOwnPropertyDescriptor(obj, key);
    if (!descriptor?.get) {
      result[key] = (obj as any)[key];
    }
  }

  return result;
}

/**
 * Adds a CoValue schema's default resolve query to a load options object
 * if no resolve query is provided.
 */
export function withDefaultResolveQuery<
  const T extends { resolve?: CoreResolveQuery },
>(loadOptions: T | undefined, defaultResolveQuery: CoreResolveQuery): T {
  const newOptions: CoreResolveQuery = loadOptions ? loadOptions : {};
  // If the default resolve query is `false`, don't add it
  if (defaultResolveQuery) {
    // TODO merge the default resolve query with the user-provided resolve query
    newOptions.resolve ||= defaultResolveQuery;
  }
  return loadOptions as T;
}
