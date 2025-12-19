import { ResolveQuery } from "../internal.js";
import {
  CoreCoValueSchema,
  CoreResolveQuery,
} from "./zodSchema/schemaTypes/CoValueSchema.js";

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
 * Adds a CoValue schema's resolve query to a load options object
 * if no resolve query is provided.
 *
 * The result type is simplified and relies on the calling function to default
 * the resolve query in the options to the schema resolve query.
 */
export function withSchemaResolveQuery<SR, O>(
  schema: { resolveQuery: SR },
  loadOptions: O,
): O {
  const newOptions: CoreResolveQuery = loadOptions ? { ...loadOptions } : {};
  // TODO merge the schema resolve query with the user-provided resolve query
  newOptions.resolve ||= schema.resolveQuery;
  return newOptions as any;
}
