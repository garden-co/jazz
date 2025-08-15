import { CoValueUniqueness } from "cojson";
import {
  Account,
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
import { CoFieldInit } from "../typeConverters/CoFieldInit.js";
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
  [key in z.output<K>]: CoFieldInit<V>;
};

export interface CoRecordSchema<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> extends CoreCoRecordSchema<K, V> {
  create: (
    init: Simplify<CoRecordInit<K, V>>,
    options?:
      | {
          owner: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ) => {
    readonly [key in z.output<K>]: InstanceOrPrimitiveOfSchema<V>;
  } & CoMap;

  load<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesNullable<K, V>
    > = true,
  >(
    id: ID<CoRecordInstanceCoValuesNullable<K, V>>,
    options?: {
      resolve?: RefsToResolveStrict<CoRecordInstanceCoValuesNullable<K, V>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<CoRecordInstanceCoValuesNullable<K, V>, R> | null>;

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

  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ): ID<CoRecordInstanceCoValuesNullable<K, V>>;

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
