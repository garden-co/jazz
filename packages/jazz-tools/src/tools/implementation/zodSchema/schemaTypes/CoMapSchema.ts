import { CoValueUniqueness } from "cojson";
import {
  Account,
  BranchDefinition,
  CoMap,
  DiscriminableCoValueSchemaDefinition,
  DiscriminableCoreCoValueSchema,
  Group,
  MaybeLoaded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeListenerOptions,
  coMapDefiner,
  coOptionalDefiner,
  hydrateCoreCoValueSchema,
  isAnyCoValueSchema,
  ResolveQuery,
  unstable_mergeBranchWithResolve,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { removeGetters } from "../../schemaUtils.js";
import { CoMapSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded.js";
import { DefaultResolveQueryOfSchema } from "../typeConverters/DefaultResolveQueryOfSchema.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema, AnyZodSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import { withDefaultResolveQuery } from "../../schemaUtils.js";

export class CoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
  Owner extends Account | Group = Account | Group,
  EagerlyLoaded extends boolean = false,
> implements CoreCoMapSchema<Shape, CatchAll>
{
  collaborative = true as const;
  builtin = "CoMap" as const;
  shape: Shape;
  catchAll?: CatchAll;
  getDefinition: () => CoMapSchemaDefinition;

  private isEagerlyLoaded: EagerlyLoaded = false as EagerlyLoaded;
  /**
   * The default resolve query to be used when loading instances of this schema.
   * Defaults to `false`, meaning that no resolve query will be used by default.
   * @internal
   */
  get defaultResolveQuery(): DefaultResolveQuery<this> {
    if (!this.isEagerlyLoaded) {
      return false as DefaultResolveQuery<this>;
    }
    const fieldResolveQueries = Object.entries(this.shape)
      .map(([fieldName, fieldSchema]) => [
        fieldName,
        fieldSchema.defaultResolveQuery,
      ])
      .filter(([_, resolveQuery]) => Boolean(resolveQuery));
    if (
      isAnyCoValueSchema(this.catchAll) &&
      this.catchAll.defaultResolveQuery
    ) {
      fieldResolveQueries.push(["$each", this.catchAll.defaultResolveQuery]);
    }
    if (fieldResolveQueries.length > 0) {
      return Object.fromEntries(fieldResolveQueries);
    } else {
      return true as DefaultResolveQuery<this>;
    }
  }

  constructor(
    coreSchema: CoreCoMapSchema<Shape, CatchAll>,
    private coValueClass: typeof CoMap,
  ) {
    this.shape = coreSchema.shape;
    this.catchAll = coreSchema.catchAll;
    this.getDefinition = coreSchema.getDefinition;
  }

  create(
    init: CoMapSchemaInit<Shape>,
    options?:
      | {
          owner?: Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Group,
  ): CoMapInstanceShape<Shape, CatchAll> & CoMap;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: CoMapSchemaInit<Shape>,
    options?:
      | {
          owner?: Owner;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Owner,
  ): CoMapInstanceShape<Shape, CatchAll> & CoMap;
  create(...args: [any, ...any[]]) {
    return this.coValueClass.create(...args);
  }

  load<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
      // @ts-expect-error
    > = EagerlyLoaded extends false ? true : this["defaultResolveQuery"],
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<
        Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
        R
      >;
      loadAs?: Account | AnonymousJazzAgent;
      skipRetry?: boolean;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<
    MaybeLoaded<
      Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
    >
  > {
    // @ts-expect-error
    return this.coValueClass.load(
      id,
      // @ts-expect-error
      withDefaultResolveQuery(options, this.defaultResolveQuery),
    );
  }

  unstable_merge<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
    > = true,
  >(
    id: string,
    options: {
      resolve?: RefsToResolveStrict<
        Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
        R
      >;
      loadAs?: Account | AnonymousJazzAgent;
      branch: BranchDefinition;
    },
  ): Promise<void> {
    // @ts-expect-error
    return unstable_mergeBranchWithResolve(this.coValueClass, id, options);
  }

  subscribe<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
    > = true,
  >(
    id: string,
    options: SubscribeListenerOptions<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
      R
    >,
    listener: (
      value: Resolved<
        Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
        R
      >,
      unsubscribe: () => void,
    ) => void,
  ): () => void {
    // @ts-expect-error
    return this.coValueClass.subscribe(id, options, listener);
  }

  /** @deprecated Use `CoMap.upsertUnique` and `CoMap.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: string,
    as?: Account | Group | AnonymousJazzAgent,
  ): string {
    return this.coValueClass.findUnique(unique, ownerID, as);
  }

  upsertUnique<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
    > = true,
  >(options: {
    value: Simplify<CoMapSchemaInit<Shape>>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Owner;
    resolve?: RefsToResolveStrict<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
      R
    >;
  }): Promise<
    MaybeLoaded<
      Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
    >
  > {
    // @ts-expect-error
    return this.coValueClass.upsertUnique(options);
  }

  loadUnique<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
    > = true,
  >(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: string,
    options?: {
      resolve?: RefsToResolveStrict<
        Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
        R
      >;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<
    MaybeLoaded<
      Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
    >
  > {
    // @ts-expect-error
    return this.coValueClass.loadUnique(unique, ownerID, options);
  }

  /**
   * @deprecated `co.map().catchall` will be removed in an upcoming version.
   *
   * Use a `co.record` nested inside a `co.map` if you need to store key-value properties.
   *
   * @example
   * ```ts
   * // Instead of:
   * const Image = co.map({
   *   original: co.fileStream(),
   * }).catchall(co.fileStream());
   *
   * // Use:
   * const Image = co.map({
   *   original: co.fileStream(),
   *   resolutions: co.record(z.string(), co.fileStream()),
   * });
   * ```
   */
  catchall<T extends AnyZodOrCoValueSchema>(
    schema: T,
  ): CoMapSchema<Shape, T, Owner, false> {
    const schemaWithCatchAll = createCoreCoMapSchema(this.shape, schema);
    return hydrateCoreCoValueSchema(schemaWithCatchAll);
  }

  withMigration(
    migration: (
      value: Resolved<
        Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
        true
      >,
    ) => undefined,
  ): CoMapSchema<Shape, CatchAll, Owner, EagerlyLoaded> {
    // @ts-expect-error
    this.coValueClass.prototype.migrate = migration;
    return this;
  }

  getCoValueClass(): typeof CoMap {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  resolved(): CoMapSchema<Shape, CatchAll, Owner, true> {
    if (this.isEagerlyLoaded) {
      return this as CoMapSchema<Shape, CatchAll, Owner, true>;
    }
    const coreSchema: CoreCoMapSchema<Shape, CatchAll> = createCoreCoMapSchema(
      this.shape,
      this.catchAll,
    );
    const copy = new CoMapSchema<Shape, CatchAll, Owner, true>(
      coreSchema,
      this.coValueClass,
    );
    copy.isEagerlyLoaded = true;
    return copy;
  }

  /**
   * Creates a new CoMap schema by picking the specified keys from the original schema.
   *
   * @param keys - The keys to pick from the original schema.
   * @returns A new CoMap schema with the picked keys.
   */
  pick<Keys extends keyof Shape>(
    keys: { [key in Keys]: true },
  ): CoMapSchema<Simplify<Pick<Shape, Keys>>, unknown, Owner> {
    const keysSet = new Set(Object.keys(keys));
    const pickedShape: Record<string, AnyZodOrCoValueSchema> = {};

    for (const [key, value] of Object.entries(this.shape)) {
      if (keysSet.has(key)) {
        pickedShape[key] = value;
      }
    }

    // @ts-expect-error the picked shape contains all required keys
    return coMapDefiner(pickedShape);
  }

  /**
   * Creates a new CoMap schema by making all fields optional.
   *
   * @returns A new CoMap schema with all fields optional.
   */
  partial<Keys extends keyof Shape = keyof Shape>(
    keys?: {
      [key in Keys]: true;
    },
  ): CoMapSchema<PartialShape<Shape, Keys>, CatchAll, Owner> {
    const partialShape: Record<string, AnyZodOrCoValueSchema> = {};

    for (const [key, value] of Object.entries(this.shape)) {
      if (keys && !keys[key as Keys]) {
        partialShape[key] = value;
        continue;
      }

      if (isAnyCoValueSchema(value)) {
        partialShape[key] = coOptionalDefiner(value);
      } else {
        partialShape[key] = z.optional(this.shape[key]);
      }
    }

    const partialCoMapSchema = coMapDefiner(partialShape);
    if (this.catchAll) {
      // @ts-expect-error the partial shape contains all required keys
      return partialCoMapSchema.catchall(
        this.catchAll as unknown as AnyZodOrCoValueSchema,
      );
    }
    // @ts-expect-error the partial shape contains all required keys
    return partialCoMapSchema;
  }
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
    defaultResolveQuery: false,
  };
}

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
  readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
} & (CatchAll extends AnyZodOrCoValueSchema
  ? {
      readonly [key: string]: InstanceOrPrimitiveOfSchema<CatchAll>;
    }
  : {});

export type CoMapInstanceCoValuesMaybeLoaded<
  Shape extends z.core.$ZodLooseShape,
> = {
  readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<
    Shape[key]
  >;
};

export type PartialShape<
  Shape extends z.core.$ZodLooseShape,
  PartialKeys extends keyof Shape = keyof Shape,
> = Simplify<{
  -readonly [key in keyof Shape]: key extends PartialKeys
    ? Shape[key] extends AnyZodSchema
      ? z.ZodOptional<Shape[key]>
      : Shape[key] extends CoreCoValueSchema
        ? CoOptionalSchema<Shape[key]>
        : never
    : Shape[key];
}>;

type FalseIfEmpty<T> = keyof T extends never ? false : T;

type DefaultResolveQueryOfShape<Shape extends z.core.$ZodLooseShape> =
  FalseIfEmpty<{
    [K in keyof Shape as DefaultResolveQueryOfSchema<Shape[K]> extends false
      ? never
      : K]: DefaultResolveQueryOfSchema<Shape[K]>;
  }>;

type DefaultResolveQuery<S> = S extends CoMapSchema<
  infer Shape,
  infer CatchAll,
  infer Owner,
  infer EagerlyLoaded
>
  ? EagerlyLoaded extends false
    ? false
    : DefaultResolveQueryOfShape<Shape> extends false
      ? true
      : DefaultResolveQueryOfShape<Shape>
  : never;
