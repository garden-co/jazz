import { CoValueUniqueness } from "cojson";
import {
  Account,
  BranchDefinition,
  type CoMap,
  CoMapSchemaDefinition,
  Group,
  ID,
  Settled,
  Loaded,
  Simplify,
  SubscribeListenerOptions,
  ResolveQuery,
  ResolveQueryStrict,
  CoreGroupSchema,
  CoreAccountSchema,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoFieldSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import { SchemaPermissions } from "../schemaPermissions.js";

type CoRecordInit<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> = {
  [key in z.output<K>]: CoFieldSchemaInit<V>;
};

export interface CoRecordSchema<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
  DefaultResolveQuery extends ResolveQuery<CoreCoRecordSchema<K, V>> = true,
> extends CoreCoRecordSchema<K, V> {
  create(
    init: Simplify<CoRecordInit<K, V>>,
    options?:
      | { owner: Group; unique?: CoValueUniqueness["uniqueness"] }
      | Group,
  ): Loaded<CoreCoRecordSchema<K, V>>;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: Simplify<CoRecordInit<K, V>>,
    options?:
      | {
          owner: Loaded<CoreAccountSchema, true> | Loaded<CoreGroupSchema>;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Loaded<CoreAccountSchema, true>
      | Loaded<CoreGroupSchema>,
  ): Loaded<CoreCoRecordSchema<K, V>>;

  load<
    const R extends ResolveQuery<
      CoreCoRecordSchema<K, V>
    > = DefaultResolveQuery,
  >(
    id: ID<CoreCoRecordSchema<K, V>>,
    options?: {
      resolve?: ResolveQueryStrict<CoreCoRecordSchema<K, V>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<CoreCoRecordSchema<K, V>, R>>;

  unstable_merge<
    const R extends ResolveQuery<
      CoreCoRecordSchema<K, V>
    > = DefaultResolveQuery,
  >(
    id: string,
    options: {
      resolve?: ResolveQueryStrict<CoreCoRecordSchema<K, V>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      branch: BranchDefinition;
    },
  ): Promise<void>;

  subscribe<
    const R extends ResolveQuery<
      CoreCoRecordSchema<K, V>
    > = DefaultResolveQuery,
  >(
    id: ID<CoreCoRecordSchema<K, V>>,
    options: SubscribeListenerOptions<CoreCoRecordSchema<K, V>, R>,
    listener: (
      value: Loaded<CoreCoRecordSchema<K, V>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;

  /** @deprecated Use `CoMap.upsertUnique` and `CoMap.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Loaded<CoreAccountSchema, true>> | ID<Loaded<CoreGroupSchema>>,
    as?:
      | Loaded<CoreAccountSchema, true>
      | Loaded<CoreGroupSchema>
      | AnonymousJazzAgent,
  ): ID<CoreCoRecordSchema<K, V>>;

  upsertUnique<
    const R extends ResolveQuery<
      CoreCoRecordSchema<K, V>
    > = DefaultResolveQuery,
  >(options: {
    value: Simplify<CoRecordInit<K, V>>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Loaded<CoreAccountSchema, true> | Loaded<CoreGroupSchema>;
    resolve?: ResolveQueryStrict<CoreCoRecordSchema<K, V>, R>;
  }): Promise<Settled<CoreCoRecordSchema<K, V>, R>>;

  loadUnique<
    const R extends ResolveQuery<
      CoreCoRecordSchema<K, V>
    > = DefaultResolveQuery,
  >(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Loaded<CoreAccountSchema, true>> | ID<Loaded<CoreGroupSchema>>,
    options?: {
      resolve?: ResolveQueryStrict<CoreCoRecordSchema<K, V>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
    },
  ): Promise<Settled<CoreCoRecordSchema<K, V>, R>>;

  optional(): CoOptionalSchema<this>;

  /**
   * Default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   * @default true
   */
  resolveQuery: DefaultResolveQuery;

  /**
   * Adds a default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   */
  resolved<const R extends ResolveQuery<CoreCoRecordSchema<K, V>> = true>(
    resolveQuery: ResolveQueryStrict<CoreCoRecordSchema<K, V>, R>,
  ): CoRecordSchema<K, V, R>;

  /**
   * Permissions to be used when creating or composing CoValues
   * @internal
   */
  permissions: SchemaPermissions;

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: SchemaPermissions,
  ): CoRecordSchema<K, V, DefaultResolveQuery>;
}

type CoRecordSchemaDefinition<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> = CoMapSchemaDefinition & {
  keyType: K;
  valueType: V;
};

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoRecordSchema<
  K extends z.core.$ZodString<string> = z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema = AnyZodOrCoValueSchema,
> extends CoreCoValueSchema {
  builtin: "CoMap";
  keyType: K;
  valueType: V;
  getDefinition: () => CoRecordSchemaDefinition<K, V>;
}
