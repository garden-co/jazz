import { CoValueUniqueness } from "cojson";
import {
  Account,
  CoMap,
  DiscriminableCoValueSchemaDefinition,
  DiscriminableCoreCoValueSchema,
  Group,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeListenerOptions,
  coMapDefiner,
  coOptionalDefiner,
  hydrateCoreCoValueSchema,
  isAnyCoValueSchema,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { removeGetters } from "../../schemaUtils.js";
import { CoMapSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema, AnyZodSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

export interface CoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  Owner extends Account | Group = Account | Group,
> extends CoreCoMapSchema<Shape> {
  create(
    init: CoMapSchemaInit<Shape>,
    options?:
      | {
          owner?: Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Group,
  ): CoMapInstanceShape<Shape> & CoMap;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: CoMapSchemaInit<Shape>,
    options?:
      | {
          owner?: Owner;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Owner,
  ): CoMapInstanceShape<Shape> & CoMap;

  load<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap
    > = true,
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<
        Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
        R
      >;
      loadAs?: Account | AnonymousJazzAgent;
      skipRetry?: boolean;
    },
  ): Promise<Resolved<
    Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
    R
  > | null>;

  subscribe<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap
    > = true,
  >(
    id: string,
    options: SubscribeListenerOptions<
      Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
      R
    >,
    listener: (
      value: Resolved<
        Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
        R
      >,
      unsubscribe: () => void,
    ) => void,
  ): () => void;

  /** @deprecated Use `CoMap.upsertUnique` and `CoMap.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: string,
    as?: Account | Group | AnonymousJazzAgent,
  ): string;

  upsertUnique: <
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap
    > = true,
  >(options: {
    value: Simplify<CoMapSchemaInit<Shape>>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Owner;
    resolve?: RefsToResolveStrict<
      Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
      R
    >;
  }) => Promise<Resolved<
    Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
    R
  > | null>;

  loadUnique<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap
    > = true,
  >(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: string,
    options?: {
      resolve?: RefsToResolveStrict<
        Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
        R
      >;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<
    Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
    R
  > | null>;

  withMigration(
    migration: (
      value: Resolved<
        Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
        true
      >,
    ) => undefined,
  ): CoMapSchema<Shape, Owner>;

  getCoValueClass: () => typeof CoMap;

  optional(): CoOptionalSchema<this>;

  /**
   * Creates a new CoMap schema by picking the specified keys from the original schema.
   *
   * @param keys - The keys to pick from the original schema.
   * @returns A new CoMap schema with the picked keys.
   */
  pick<Keys extends keyof Shape>(
    keys: { [key in Keys]: true },
  ): CoMapSchema<Simplify<Pick<Shape, Keys>>, Owner>;

  /**
   * Creates a new CoMap schema by making all fields optional.
   *
   * @returns A new CoMap schema with all fields optional.
   */
  partial(): CoMapSchema<PartialShape<Shape>, Owner>;
}

export function createCoreCoMapSchema<Shape extends z.core.$ZodLooseShape>(
  shape: Shape,
): CoreCoMapSchema<Shape> {
  return {
    collaborative: true as const,
    builtin: "CoMap" as const,
    shape,
    getDefinition: () => ({
      get shape() {
        return shape;
      },
      get discriminatorMap() {
        const propValues: DiscriminableCoValueSchemaDefinition["discriminatorMap"] =
          {};
        // remove getters to avoid circularity issues. Getters are not used as discriminators
        for (const key in removeGetters(shape)) {
          if (isAnyCoValueSchema(shape[key])) {
            // CoValues cannot be used as discriminators either
            continue;
          }
          const field = shape[key]._zod;
          if (field.values) {
            propValues[key] ??= new Set();
            for (const v of field.values) propValues[key].add(v);
          }
        }
        return propValues;
      },
    }),
  };
}

export function enrichCoMapSchema<Shape extends z.core.$ZodLooseShape>(
  schema: CoreCoMapSchema<Shape>,
  coValueClass: typeof CoMap,
): CoMapSchema<Shape> {
  const coValueSchema = Object.assign(schema, {
    create: (...args: [any, ...any[]]) => {
      return coValueClass.create(...args);
    },
    load: (...args: [any, ...any[]]) => {
      return coValueClass.load(...args);
    },
    subscribe: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.subscribe(...args);
    },
    findUnique: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.findUnique(...args);
    },
    upsertUnique: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.upsertUnique(...args);
    },
    loadUnique: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.loadUnique(...args);
    },
    withMigration: (migration: (value: any) => undefined) => {
      // @ts-expect-error TODO check
      coValueClass.prototype.migrate = migration;

      return coValueSchema;
    },
    getCoValueClass: () => {
      return coValueClass;
    },
    optional: () => {
      return coOptionalDefiner(coValueSchema);
    },
    pick: <Keys extends keyof Shape>(keys: { [key in Keys]: true }) => {
      const keysSet = new Set(Object.keys(keys));
      const pickedShape: Record<string, AnyZodOrCoValueSchema> = {};

      for (const [key, value] of Object.entries(coValueSchema.shape)) {
        if (keysSet.has(key)) {
          pickedShape[key] = value;
        }
      }

      return coMapDefiner(pickedShape);
    },
    partial: () => {
      const partialShape: Record<string, AnyZodOrCoValueSchema> = {};

      for (const [key, value] of Object.entries(coValueSchema.shape)) {
        if (isAnyCoValueSchema(value)) {
          partialShape[key] = coOptionalDefiner(value);
        } else {
          partialShape[key] = z.optional(coValueSchema.shape[key]);
        }
      }
      return coMapDefiner(partialShape);
    },
  }) as unknown as CoMapSchema<Shape>;
  return coValueSchema;
}

export interface CoMapSchemaDefinition<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
> extends DiscriminableCoValueSchemaDefinition {
  shape: Shape;
}

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoMapSchema<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
> extends DiscriminableCoreCoValueSchema {
  builtin: "CoMap";
  shape: Shape;
  getDefinition: () => CoMapSchemaDefinition;
}

export type CoMapInstanceShape<Shape extends z.core.$ZodLooseShape> = {
  readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
};

export type CoMapInstanceCoValuesNullable<Shape extends z.core.$ZodLooseShape> =
  {
    readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchemaCoValuesNullable<
      Shape[key]
    >;
  };

export type PartialShape<Shape extends z.core.$ZodLooseShape> = Simplify<{
  -readonly [key in keyof Shape]: Shape[key] extends AnyZodSchema
    ? z.ZodOptional<Shape[key]>
    : Shape[key] extends CoreCoValueSchema
      ? CoOptionalSchema<Shape[key]>
      : never;
}>;
