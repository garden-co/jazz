import { createDbWithRuntimeSource, type DbConfig } from "../db.js";
import { DefaultRuntimeSource } from "../default-runtime-source.js";

/** Internal protocol tests may exercise raw runtime admission. Public client
 * tests must use account-fixtures and the exported account-handle factories. */
export function createDb(config: DbConfig) {
  return createDbWithRuntimeSource(config, new DefaultRuntimeSource());
}
