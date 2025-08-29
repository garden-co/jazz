import { CoValueUniqueness } from "cojson";
import {
  Account,
  type CoMap,
  coOptionalDefiner,
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

export class CoRecordSchema<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> implements CoreCoRecordSchema<K, V>
{
  collaborative = true as const;
  builtin = "CoRecord" as const;

  getDefinition(): CoRecordSchemaDefinition<K, V> {
    return {
      keyType: this.keyType,
      valueType: this.valueType,
    };
  }

  constructor(
    private keyType: K,
    private valueType: V,
    private coValueClass: typeof CoMap,
  ) {}

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
  create(...args: [any, ...any[]]): CoRecordInstanceShape<K, V> & CoMap {
    return this.coValueClass.create(...args) as CoRecordInstanceShape<K, V> &
      CoMap;
  }

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
  ): Promise<Resolved<CoRecordInstanceCoValuesNullable<K, V>, R> | null> {
    // @ts-expect-error
    return this.coValueClass.load(id, options);
  }

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
  ): () => void {
    // @ts-expect-error
    return this.coValueClass.subscribe(id, options, listener);
  }

  /** @deprecated Use `CoMap.upsertUnique` and `CoMap.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ): ID<CoRecordInstanceCoValuesNullable<K, V>> {
    return this.coValueClass.findUnique(unique, ownerID, as);
  }

  upsertUnique<
    const R extends RefsToResolve<
      CoRecordInstanceCoValuesNullable<K, V>
    > = true,
  >(options: {
    value: Simplify<CoRecordInit<K, V>>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Account | Group;
    resolve?: RefsToResolveStrict<CoRecordInstanceCoValuesNullable<K, V>, R>;
  }): Promise<Resolved<CoRecordInstanceCoValuesNullable<K, V>, R> | null> {
    // @ts-expect-error
    return this.coValueClass.upsertUnique(options);
  }

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
  ): Promise<Resolved<CoRecordInstanceCoValuesNullable<K, V>, R> | null> {
    // @ts-expect-error
    return this.coValueClass.loadUnique(unique, ownerID, options);
  }

  getCoValueClass(): typeof CoMap {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }
}

export function createCoreCoRecordSchema<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
>(keyType: K, valueType: V): CoreCoRecordSchema<K, V> {
  return {
    collaborative: true as const,
    builtin: "CoRecord" as const,
    getDefinition: () => ({
      keyType,
      valueType,
    }),
  };
}

type CoRecordSchemaDefinition<
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
> = {
  keyType: K;
  valueType: V;
};

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoRecordSchema<
  K extends z.core.$ZodString<string> = z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema = AnyZodOrCoValueSchema,
> extends CoreCoValueSchema {
  builtin: "CoRecord";
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
