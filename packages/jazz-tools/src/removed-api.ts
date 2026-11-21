declare const removedApi: unique symbol;

/**
 * The type of a removed export kept as a stub that throws when used, naming
 * its replacement (see `removed-schema-api.ts`): calling or constructing it
 * throws, and parameters of type `never` keep a call with arguments from
 * type-checking.
 */
export type RemovedApi = ((...args: never[]) => never) &
  (new (...args: never[]) => never) & { readonly [removedApi]: never };
