import { CoValueUniqueness } from "cojson";
import {
  Account,
  BranchDefinition,
  type CoMap,
  CoMapSchemaDefinition,
  Group,
  ID,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeListenerOptions,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoFieldSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

type CoRecordInit<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> = {
  [key in z.output<K>]: CoFieldSchemaInit<V>;
};

export interface CoRecordSchema<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> extends CoreCoRecordSchema<K, V> {
  create(
    init: Simplify<CoRecordInit<K, V>>,
    options?:
      | { owner: Group; unique?: CoValueUniqueness["uniqueness"] }
      | Group,
  ): CoRecordInstanceShape<K, V> & CoMap;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: Simplify<CoRecordInit<K, V>>,
    options?:
      | { owner: Account | Group; unique?: CoValueUniqueness["uniqueness"] }
      | Account
      | Group,
  ): CoRecordInstanceShape<K, V> & CoMap;

  load<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesNullable<K, V>
    > = true,
  >(
    id: ID<CoRecordInstanceCoValuesNullable<K, V>>,
    options?: {
      resolve?: RefsToResolveStrict<CoRecordInstanceCoValuesNullable<K, V>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Resolved<CoRecordInstanceCoValuesNullable<K, V>, R> | null>;

  unstable_merge<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesNullable<K, V>
    > = true,
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoRecordInstanceCoValuesNullable<K, V>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      branch: BranchDefinition;
    },
  ): Promise<void>;

  subscribe<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesNullable<K, V>
    > = true,
  >(
    id: ID<CoRecordInstanceCoValuesNullable<K, V>>,
    options: SubscribeListenerOptions<
      CoRecordInstanceCoValuesNullable<K, V>,
      R
    >,
    listener: (
      value: Resolved<CoRecordInstanceCoValuesNullable<K, V>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;

  /** @deprecated Use `CoMap.upsertUnique` and `CoMap.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ): ID<CoRecordInstanceCoValuesNullable<K, V>>;

  upsertUnique<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesNullable<K, V>
    > = true,
  >(options: {
    value: Simplify<CoRecordInit<K, V>>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Account | Group;
    resolve?: RefsToResolveStrict<CoRecordInstanceCoValuesNullable<K, V>, R>;
  }): Promise<Resolved<CoRecordInstanceCoValuesNullable<K, V>, R> | null>;

  loadUnique<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesNullable<K, V>
    > = true,
  >(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    options?: {
      resolve?: RefsToResolveStrict<CoRecordInstanceCoValuesNullable<K, V>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<CoRecordInstanceCoValuesNullable<K, V>, R> | null>;

  getCoValueClass: () => typeof CoMap;

  optional(): CoOptionalSchema<this>;
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

export type CoRecordInstanceCoValuesNullable<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> = {
  readonly [key in z.output<K>]: InstanceOrPrimitiveOfSchemaCoValuesNullable<V>;
} & CoMap;

export type CoRecordInstanceShape<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> = {
  readonly [key in z.output<K>]: InstanceOrPrimitiveOfSchema<V>;
};
