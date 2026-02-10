import { CoValueUniqueness } from "cojson";
import {
  Account,
  BranchDefinition,
  type CoMap,
  CoMapSchemaDefinition,
  Group,
  ID,
  Settled,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeListenerOptions,
  LocalValidationMode,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoFieldSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded.js";
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
  DefaultResolveQuery extends CoreResolveQuery = true,
> extends CoreCoRecordSchema<K, V> {
  create(
    init: Simplify<CoRecordInit<K, V>>,
    options?:
      | {
          owner: Group;
          unique?: CoValueUniqueness["uniqueness"];
          validation?: LocalValidationMode;
        }
      | { owner?: Group; validation?: LocalValidationMode }
      | Group,
  ): CoRecordInstanceShape<K, V> & CoMap;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: Simplify<CoRecordInit<K, V>>,
    options?:
      | {
          owner: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
          validation?: LocalValidationMode;
        }
      | { owner?: Account | Group; validation?: LocalValidationMode }
      | Account
      | Group,
  ): CoRecordInstanceShape<K, V> & CoMap;

  load<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesMaybeLoaded<K, V>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: ID<CoRecordInstanceCoValuesMaybeLoaded<K, V>>,
    options?: {
      resolve?: RefsToResolveStrict<
        CoRecordInstanceCoValuesMaybeLoaded<K, V>,
        R
      >;
      loadAs?: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<Resolved<CoRecordInstanceCoValuesMaybeLoaded<K, V>, R>>>;

  unstable_merge<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesMaybeLoaded<K, V>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: string,
    options: {
      resolve?: RefsToResolveStrict<
        CoRecordInstanceCoValuesMaybeLoaded<K, V>,
        R
      >;
      loadAs?: Account | AnonymousJazzAgent;
      branch: BranchDefinition;
    },
  ): Promise<void>;

  subscribe<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesMaybeLoaded<K, V>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: ID<CoRecordInstanceCoValuesMaybeLoaded<K, V>>,
    options: SubscribeListenerOptions<
      CoRecordInstanceCoValuesMaybeLoaded<K, V>,
      R
    >,
    listener: (
      value: Resolved<CoRecordInstanceCoValuesMaybeLoaded<K, V>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;

  /** @deprecated Use `CoMap.upsertUnique` and `CoMap.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ): ID<CoRecordInstanceCoValuesMaybeLoaded<K, V>>;

  upsertUnique<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesMaybeLoaded<K, V>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(options: {
    value: Simplify<CoRecordInit<K, V>>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Account | Group;
    resolve?: RefsToResolveStrict<CoRecordInstanceCoValuesMaybeLoaded<K, V>, R>;
  }): Promise<Settled<Resolved<CoRecordInstanceCoValuesMaybeLoaded<K, V>, R>>>;

  loadUnique<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesMaybeLoaded<K, V>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    options?: {
      resolve?: RefsToResolveStrict<
        CoRecordInstanceCoValuesMaybeLoaded<K, V>,
        R
      >;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Settled<Resolved<CoRecordInstanceCoValuesMaybeLoaded<K, V>, R>>>;

  getCoValueClass: () => typeof CoMap;

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
  resolved<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesMaybeLoaded<K, V>
    > = true,
  >(
    resolveQuery: RefsToResolveStrict<
      CoRecordInstanceCoValuesMaybeLoaded<K, V>,
      R
    >,
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
  getDefinition: () => CoRecordSchemaDefinition<K, V>;
}

export type CoRecordInstance<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> = {
  [key in z.output<K>]: InstanceOrPrimitiveOfSchema<V>;
} & CoMap;

export type CoRecordInstanceCoValuesMaybeLoaded<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> = {
  readonly [key in z.output<K>]: InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<V>;
} & CoMap;

export type CoRecordInstanceShape<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> = {
  readonly [key in z.output<K>]: InstanceOrPrimitiveOfSchema<V>;
};
