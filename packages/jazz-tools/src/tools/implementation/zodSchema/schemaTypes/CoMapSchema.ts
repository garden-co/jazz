import { CoValueUniqueness } from "cojson";
import {
  Account,
  CoMap,
  DiscriminableCoValueSchemaDefinition,
  DiscriminableCoreCoValueSchema,
  Group,
  NotNull,
  OptionalizeUndefinedKeys,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeListenerOptions,
  coOptionalDefiner,
  hydrateCoreCoValueSchema,
  isAnyCoValueSchema,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { removeGetters } from "../../schemaUtils.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";

export interface CoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
  Owner extends Account | Group = Account | Group,
> extends CoreCoMapSchema<Shape, CatchAll> {
  create: (
    init: Simplify<CoMapInitZod<Shape>>,
    options?:
      | {
          owner: Owner;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Owner,
  ) => CoMapInstanceShape<Shape, CatchAll> & CoMap;

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
    value: Simplify<CoMapInitZod<Shape>>;
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

  catchall<T extends AnyZodOrCoValueSchema>(schema: T): CoMapSchema<Shape, T>;

  withMigration(
    migration: (
      value: Resolved<
        Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
        true
      >,
    ) => undefined,
  ): CoMapSchema<Shape, CatchAll, Owner>;

  getCoValueClass: () => typeof CoMap;

  optional(): CoOptionalSchema<this>;
}

export function createCoreCoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
>(shape: Shape, catchAll?: CatchAll): CoreCoMapSchema<Shape, CatchAll> {
  return {
    collaborative: true as const,
    builtin: "CoMap" as const,
    shape,
    catchAll,
    getDefinition: () => ({
      get shape() {
        return shape;
      },
      get catchall() {
        return catchAll;
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

export function enrichCoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown,
>(
  schema: CoreCoMapSchema<Shape, CatchAll>,
  coValueClass: typeof CoMap,
): CoMapSchema<Shape, CatchAll> {
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
    catchall: (catchAll: AnyZodOrCoValueSchema) => {
      const schemaWithCatchAll = createCoreCoMapSchema(
        coValueSchema.getDefinition().shape,
        catchAll,
      );
      return hydrateCoreCoValueSchema(schemaWithCatchAll);
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
  }) as unknown as CoMapSchema<Shape, CatchAll>;
  return coValueSchema;
}

export type CoMapInitZod<Shape extends z.core.$ZodLooseShape> =
  OptionalizeUndefinedKeys<{
    [key in keyof Shape]: NotNull<
      InstanceOrPrimitiveOfSchemaCoValuesNullable<Shape[key]>
    >;
  }>;

export interface CoMapSchemaDefinition<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
> extends DiscriminableCoValueSchemaDefinition {
  shape: Shape;
  catchall?: CatchAll;
}

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoMapSchema<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
> extends DiscriminableCoreCoValueSchema {
  builtin: "CoMap";
  shape: Shape;
  catchAll?: CatchAll;
  getDefinition: () => CoMapSchemaDefinition;
}

export type CoMapInstanceShape<
  Shape extends z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
> = {
  -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
} & (CatchAll extends AnyZodOrCoValueSchema
  ? {
      [key: string]: InstanceOrPrimitiveOfSchema<CatchAll>;
    }
  : {});

export type CoMapInstanceCoValuesNullable<Shape extends z.core.$ZodLooseShape> =
  {
    -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchemaCoValuesNullable<
      Shape[key]
    >;
  };
