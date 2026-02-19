import { CoValueUniqueness } from "cojson";
import {
  Account,
  BranchDefinition,
  CoMap,
  DiscriminableCoValueSchemaDefinition,
  DiscriminableCoreCoValueSchema,
  Group,
  Settled,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeCallback,
  SubscribeListenerOptions,
  coMapDefiner,
  coOptionalDefiner,
  hydrateCoreCoValueSchema,
  isAnyCoValueSchema,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
  isCoValueSchema,
  type Schema,
  CoValueCreateOptions,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { removeGetters, withSchemaResolveQuery } from "../../schemaUtils.js";
import { CoMapSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema, AnyZodSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";
import {
  coValueValidationSchema,
  generateValidationSchemaFromItem,
} from "./schemaValidators.js";
import { resolveSchemaField } from "../runtimeConverters/schemaFieldToCoFieldDef.js";

type CoMapSchemaInstance<Shape extends z.core.$ZodLooseShape> = Simplify<
  CoMapInstanceCoValuesMaybeLoaded<Shape>
> &
  CoMap;

export class CoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
  Owner extends Account | Group = Account | Group,
  DefaultResolveQuery extends CoreResolveQuery = true,
> implements CoreCoMapSchema<Shape, CatchAll>
{
  collaborative = true as const;
  builtin = "CoMap" as const;
  shape: Shape;
  catchAll?: CatchAll;
  #descriptorsSchema: CoMapDescriptorsSchema | undefined = undefined;
  getDefinition: () => CoMapSchemaDefinition;

  #validationSchema: z.ZodType | undefined = undefined;
  getValidationSchema = () => {
    if (this.#validationSchema) {
      return this.#validationSchema;
    }

    const plainShape: Record<string, z.ZodTypeAny> = {};

    for (const key in this.shape) {
      const item = this.shape[key];
      if (isCoValueSchema(item)) {
        // Inject as getter to avoid circularity issues
        Object.defineProperty(plainShape, key, {
          get: () => generateValidationSchemaFromItem(item),
          enumerable: true,
          configurable: true,
        });
      } else {
        plainShape[key] = generateValidationSchemaFromItem(item);
      }
    }

    let validationSchema = z.strictObject(plainShape);
    if (this.catchAll) {
      validationSchema = validationSchema.catchall(
        generateValidationSchemaFromItem(
          this.catchAll as unknown as AnyZodOrCoValueSchema,
        ),
      );
    }

    this.#validationSchema = coValueValidationSchema(validationSchema, CoMap);

    return this.#validationSchema;
  };

  /**
   * Default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   * @default true
   */
  resolveQuery: DefaultResolveQuery = true as DefaultResolveQuery;

  #permissions: SchemaPermissions | null = null;
  /**
   * Permissions to be used when creating or composing CoValues
   * @internal
   */
  get permissions(): SchemaPermissions {
    return this.#permissions ?? DEFAULT_SCHEMA_PERMISSIONS;
  }

  constructor(
    coreSchema: CoreCoMapSchema<Shape, CatchAll>,
    private coValueClass: typeof CoMap,
  ) {
    this.shape = coreSchema.shape;
    this.catchAll = coreSchema.catchAll;
    this.getDefinition = coreSchema.getDefinition;
  }

  getDescriptorsSchema = (): CoMapDescriptorsSchema => {
    if (this.#descriptorsSchema) {
      return this.#descriptorsSchema;
    }

    const descriptorShape: Record<string, Schema> = {};
    for (const key of Object.keys(this.shape)) {
      const field = this.shape[key as keyof Shape];
      descriptorShape[key] = resolveSchemaField(field as any);
    }

    const descriptorCatchall =
      this.catchAll === undefined
        ? undefined
        : resolveSchemaField(this.catchAll as any);

    this.#descriptorsSchema = {
      shape: descriptorShape,
      catchall: descriptorCatchall,
    };

    return this.#descriptorsSchema;
  };

  create(
    init: CoMapSchemaInit<Shape>,
    options?: CoValueCreateOptions,
  ): CoMapInstanceShape<Shape, CatchAll> & CoMap;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: CoMapSchemaInit<Shape>,
    options?: CoValueCreateOptions<{}, Account | Group>,
  ): CoMapInstanceShape<Shape, CatchAll> & CoMap;
  create(init: any, options?: any) {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );

    return this.coValueClass.create(init, optionsWithPermissions);
  }

  load<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
      // @ts-expect-error
    > = DefaultResolveQuery,
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
    Settled<
      Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
    >
  > {
    // @ts-expect-error
    return this.coValueClass.load(
      id,
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  unstable_merge<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
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
    return unstable_mergeBranchWithResolve(
      this.coValueClass,
      id,
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  subscribe<
    const R extends RefsToResolve<
      CoMapSchemaInstance<Shape>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: string,
    listener: SubscribeCallback<Resolved<CoMapSchemaInstance<Shape>, R>>,
  ): () => void;
  subscribe<
    const R extends RefsToResolve<
      CoMapSchemaInstance<Shape>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<CoMapSchemaInstance<Shape>, R>,
    listener: SubscribeCallback<Resolved<CoMapSchemaInstance<Shape>, R>>,
  ): () => void;
  subscribe<const R extends RefsToResolve<CoMapSchemaInstance<Shape>>>(
    id: string,
    optionsOrListener:
      | SubscribeListenerOptions<CoMapSchemaInstance<Shape>, R>
      | SubscribeCallback<Resolved<CoMapSchemaInstance<Shape>, R>>,
    maybeListener?: SubscribeCallback<Resolved<CoMapSchemaInstance<Shape>, R>>,
  ): () => void {
    if (typeof optionsOrListener === "function") {
      // @ts-expect-error
      return this.coValueClass.subscribe(
        id,
        withSchemaResolveQuery({}, this.resolveQuery),
        optionsOrListener,
      );
    }
    // @ts-expect-error
    return this.coValueClass.subscribe(
      id,
      withSchemaResolveQuery(optionsOrListener, this.resolveQuery),
      maybeListener,
    );
  }

  /** @deprecated Use `loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: string,
    as?: Account | Group | AnonymousJazzAgent,
  ): string {
    return this.coValueClass.findUnique(unique, ownerID, as);
  }

  /**
   * Get an existing unique CoMap or create a new one if it doesn't exist.
   *
   * Unlike `upsertUnique`, this method does NOT update existing values with the provided value.
   * The provided value is only used when creating a new CoMap.
   *
   * @example
   * ```ts
   * const settings = await UserSettings.getOrCreateUnique({
   *   value: { theme: "dark", language: "en" },
   *   unique: "user-settings",
   *   owner: me,
   * });
   * ```
   *
   * @param options The options for creating or loading the CoMap.
   * @returns Either an existing CoMap (unchanged), or a new initialised CoMap if none exists.
   * @category Subscription & Loading
   */
  getOrCreateUnique<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(options: {
    value: Simplify<CoMapSchemaInit<Shape>>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Owner;
    resolve?: RefsToResolveStrict<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
      R
    >;
  }): Promise<
    Settled<
      Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
    >
  > {
    // @ts-expect-error
    return this.coValueClass.getOrCreateUnique(
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  /**
   * @deprecated Use `getOrCreateUnique` instead. Note: getOrCreateUnique does not update existing values.
   * If you need to update, use getOrCreateUnique followed by direct property assignment.
   */
  upsertUnique<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(options: {
    value: Simplify<CoMapSchemaInit<Shape>>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Owner;
    resolve?: RefsToResolveStrict<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
      R
    >;
  }): Promise<
    Settled<
      Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
    >
  > {
    // @ts-expect-error
    return this.coValueClass.upsertUnique(
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  loadUnique<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
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
    Settled<
      Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
    >
  > {
    // @ts-expect-error
    return this.coValueClass.loadUnique(
      unique,
      ownerID,
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
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
  catchall<T extends AnyZodOrCoValueSchema>(schema: T): CoMapSchema<Shape, T> {
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
  ): this {
    // @ts-expect-error avoid exposing 'migrate' at the type level
    this.coValueClass.prototype.migrate = migration;
    return this;
  }

  getCoValueClass(): typeof CoMap {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
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

  /**
   * Adds a default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   */
  resolved<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
    > = true,
  >(
    resolveQuery: RefsToResolveStrict<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap,
      R
    >,
  ): CoMapSchema<Shape, CatchAll, Owner, R> {
    return this.copy({ resolveQuery: resolveQuery as R });
  }

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: Omit<SchemaPermissions, "writer">,
  ): CoMapSchema<Shape, CatchAll, Owner, DefaultResolveQuery> {
    return this.copy({ permissions });
  }

  /**
   * Creates a copy of this schema, preserving all previous configuration
   */
  private copy<ResolveQuery extends CoreResolveQuery = DefaultResolveQuery>({
    permissions,
    resolveQuery,
  }: {
    permissions?: SchemaPermissions;
    resolveQuery?: ResolveQuery;
  }): CoMapSchema<Shape, CatchAll, Owner, ResolveQuery> {
    const coreSchema = createCoreCoMapSchema(this.shape, this.catchAll);
    // @ts-expect-error
    const copy: CoMapSchema<Shape, CatchAll, Owner, ResolveQuery> =
      hydrateCoreCoValueSchema(coreSchema);
    // @ts-expect-error avoid exposing 'migrate' at the type level
    copy.coValueClass.prototype.migrate = this.coValueClass.prototype.migrate;
    // @ts-expect-error TS cannot infer that the resolveQuery type is valid
    copy.resolveQuery = resolveQuery ?? this.resolveQuery;
    copy.#permissions = permissions ?? this.#permissions;
    return copy;
  }
}

export function createCoreCoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
>(shape: Shape, catchAll?: CatchAll): CoreCoMapSchema<Shape, CatchAll> {
  let descriptorsSchema: CoMapDescriptorsSchema | undefined;

  return {
    collaborative: true as const,
    builtin: "CoMap" as const,
    shape,
    catchAll,
    getDescriptorsSchema: () => {
      if (descriptorsSchema) {
        return descriptorsSchema;
      }

      const descriptorShape: Record<string, Schema> = {};
      for (const key of Object.keys(shape)) {
        const field = shape[key as keyof Shape];
        descriptorShape[key] = resolveSchemaField(field as any);
      }

      const descriptorCatchall =
        catchAll === undefined
          ? undefined
          : resolveSchemaField(catchAll as any);

      descriptorsSchema = {
        shape: descriptorShape,
        catchall: descriptorCatchall,
      };

      return descriptorsSchema;
    },
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
    resolveQuery: true as const,
    getValidationSchema: () => z.any(),
  };
}

export interface CoMapSchemaDefinition<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
> extends DiscriminableCoValueSchemaDefinition {
  shape: Shape;
  catchall?: CatchAll;
}

export type CoMapDescriptorsSchema = {
  shape: Record<string, Schema>;
  catchall?: Schema;
};

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoMapSchema<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
> extends DiscriminableCoreCoValueSchema {
  builtin: "CoMap";
  shape: Shape;
  catchAll?: CatchAll;
  getDescriptorsSchema: () => CoMapDescriptorsSchema;
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
