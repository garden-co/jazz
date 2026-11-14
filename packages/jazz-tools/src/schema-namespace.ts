/**
 * Schema builders shared by every public binding, including React Native.
 *
 * An ES namespace, not an object: bundlers keep only the members an app uses
 * (a client that never calls `schema.definePermissions` doesn't ship the
 * permissions DSL).
 */
export * as schema from "./schema-builders.js";
