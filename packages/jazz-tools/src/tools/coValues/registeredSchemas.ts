import type {
  Account,
  AccountSchema,
  CoMap,
  CoMapSchema,
  CoreAccountSchema,
  Group,
  GroupSchema,
} from "../internal.js";

/**
 * Regisering schemas into this Record to avoid circular dependencies.
 */
export const RegisteredSchemas = {} as {
  Account: CoreAccountSchema;
  Group: GroupSchema;
  CoMap: CoMapSchema<{}>;
};
