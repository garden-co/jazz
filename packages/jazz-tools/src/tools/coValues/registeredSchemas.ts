import type { Account, CoMap, Group } from "../internal.js";

/**
 * Registering schemas into this Record to avoid circular dependencies.
 */
export const RegisteredSchemas = {} as {
  Account: typeof Account;
  Group: typeof Group;
  CoMap: typeof CoMap;
};
