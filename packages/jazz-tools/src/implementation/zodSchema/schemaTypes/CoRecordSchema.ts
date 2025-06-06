import { CoValueUniqueness } from "cojson";
import {
  Account,
  type CoMap,
  Group,
  ID,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeListenerOptions,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { z } from "../zodReExport.js";
import { WithHelpers } from "../zodSchema.js";

type CoRecordInit<
  K extends z.core.$ZodString<string>,
  V extends z.core.$ZodType,
> = {
  [key in z.output<K>]: V extends z.core.$ZodOptional<any>
    ? InstanceOrPrimitiveOfSchemaCoValuesNullable<V>
    : NonNullable<InstanceOrPrimitiveOfSchemaCoValuesNullable<V>>;
};

export type CoRecordSchema<
  K extends z.core.$ZodString<string>,
  V extends z.core.$ZodType,
> = z.core.$ZodRecord<K, V> & {
  collaborative: true;

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
    [key in z.output<K>]: InstanceOrPrimitiveOfSchema<V>;
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

  /** @deprecated Define your helper methods separately, in standalone functions. */
  withHelpers<S extends z.core.$ZodType, T extends object>(
    this: S,
    helpers: (Self: S) => T,
  ): WithHelpers<S, T>;
  getCoSchema: () => typeof CoMap;
};

// less precise verion to avoid circularity issues and allow matching against
export type AnyCoRecordSchema<
  K extends z.core.$ZodString<string> = z.core.$ZodString<string>,
  V extends z.core.$ZodType = z.core.$ZodType,
> = z.core.$ZodRecord<K, V> & { collaborative: true };

export type CoRecordInstance<
  K extends z.core.$ZodString<string>,
  V extends z.core.$ZodType,
> = {
  [key in z.output<K>]: InstanceOrPrimitiveOfSchema<V>;
} & CoMap;

export type CoRecordInstanceCoValuesNullable<
  K extends z.core.$ZodString<string>,
  V extends z.core.$ZodType,
> = {
  [key in z.output<K>]: InstanceOrPrimitiveOfSchemaCoValuesNullable<V>;
} & CoMap;
