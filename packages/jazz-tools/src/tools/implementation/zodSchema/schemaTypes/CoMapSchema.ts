import { CoValueUniqueness, RawCoID } from "cojson";
import {
  Account,
  BranchDefinition,
  CoMap,
  DiscriminableCoValueSchemaDefinition,
  DiscriminableCoreCoValueSchema,
  Group,
  ID,
  Settled,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  co,
  coMapDefiner,
  coOptionalDefiner,
  asConstructable,
  internalLoadUnique,
  isAnyCoValueSchema,
  loadCoValueWithoutMe,
  parseCoValueCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
  getIdFromHeader,
  schemaFieldToFieldDescriptor,
  SchemaField,
  CoMapFieldSchema,
  ItemsMarker,
  RegisteredSchemas,
  ResolveQueryStrict,
  ResolveQuery,
  Loaded,
} from "../../../internal.js";
import { RawCoMap } from "cojson";
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

export class CoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
  Owner extends Account | Group = Account | Group,
  DefaultResolveQuery extends ResolveQuery<
    CoMapSchema<Shape, CatchAll, Owner>
  > = true,
> implements CoreCoMapSchema<Shape, CatchAll>
{
  collaborative = true as const;
  builtin = "CoMap" as const;
  shape: Shape;
  catchAll?: CatchAll;
  getDefinition: () => CoMapSchemaDefinition;

  /**
   * Default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   * @default true
   */
  resolveQuery: DefaultResolveQuery = true as DefaultResolveQuery;

  /**
   * Permissions to be used when creating or composing CoValues
   * @internal
   */
  permissions: SchemaPermissions = DEFAULT_SCHEMA_PERMISSIONS;

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
  create(init: any, options?: any) {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    const { owner, uniqueness } = parseCoValueCreateOptions(
      optionsWithPermissions,
    );

    // Convert schema fields to field descriptors
    const def = this.getDefinition();
    const fields: CoMapFieldSchema = {};
    for (const [key, value] of Object.entries(def.shape)) {
      fields[key] = schemaFieldToFieldDescriptor(value as SchemaField);
    }
    if (def.catchall) {
      fields[ItemsMarker] = schemaFieldToFieldDescriptor(
        def.catchall as SchemaField,
      );
    }

    const raw = CoMap.rawFromInit(fields, init, owner, uniqueness);
    return new this.coValueClass(fields, raw, this);
  }

  fromRaw(raw: RawCoMap): CoMapInstanceShape<Shape, CatchAll> & CoMap {
    // Convert schema fields to field descriptors
    const def = this.getDefinition();
    const fields: CoMapFieldSchema = {};
    for (const [key, value] of Object.entries(def.shape)) {
      fields[key] = schemaFieldToFieldDescriptor(value as SchemaField);
    }
    if (def.catchall) {
      fields[ItemsMarker] = schemaFieldToFieldDescriptor(
        def.catchall as SchemaField,
      );
    }
    return new this.coValueClass(fields, raw, this) as CoMapInstanceShape<
      Shape,
      CatchAll
    > &
      CoMap;
  }

  load<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
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
    return loadCoValueWithoutMe(
      this,
      id,
      withSchemaResolveQuery(options, this.resolveQuery),
    ) as Promise<
      Settled<
        Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
      >
    >;
  }

  unstable_merge<const R extends ResolveQuery<this> = DefaultResolveQuery>(
    id: string,
    options: {
      resolve?: ResolveQueryStrict<CoMapSchema<Shape, CatchAll>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      branch: BranchDefinition;
    },
  ): Promise<void> {
    const WithResolve = withSchemaResolveQuery(options, this.resolveQuery);
    return unstable_mergeBranchWithResolve(this, id, WithResolve as any); // TODO: fix cast
  }

  subscribe<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
    > = DefaultResolveQuery,
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
    return subscribeToCoValueWithoutMe(
      this,
      id,
      withSchemaResolveQuery(options, this.resolveQuery),
      listener as any,
    );
  }

  /** @deprecated Use `CoMap.upsertUnique` and `CoMap.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: string,
    as?: Account | Group | AnonymousJazzAgent,
  ): string {
    const header = this._getUniqueHeader(unique, ownerID);
    return getIdFromHeader(header, as);
  }

  /** @internal */
  private _getUniqueHeader(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
  ) {
    return {
      type: "comap" as const,
      ruleset: {
        type: "ownedByGroup" as const,
        group: ownerID as RawCoID,
      },
      meta: null,
      uniqueness: unique,
    };
  }

  upsertUnique<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
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
    const header = this._getUniqueHeader(
      options.unique,
      options.owner.$jazz.id,
    );

    return internalLoadUnique(this, {
      header,
      owner: options.owner,
      resolve: withSchemaResolveQuery(
        options.resolve ? { resolve: options.resolve } : undefined,
        this.resolveQuery,
      )?.resolve as any,
      onCreateWhenMissing: () => {
        this.create(options.value, {
          owner: options.owner,
          unique: options.unique,
        });
      },
      onUpdateWhenFound(value) {
        (value as Loaded<CoreCoMapSchema>).$jazz.applyDiff(options.value);
      },
    }) as Promise<
      Settled<
        Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
      >
    >;
  }

  async loadUnique<
    const R extends RefsToResolve<
      Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap
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
    const header = this._getUniqueHeader(unique, ownerID);

    const owner = await co.group().load(ownerID, {
      loadAs: options?.loadAs,
    });

    if (!owner.$isLoaded) return owner as any;

    return internalLoadUnique(this, {
      header,
      owner,
      resolve: withSchemaResolveQuery(
        options?.resolve ? { resolve: options.resolve } : undefined,
        this.resolveQuery,
      )?.resolve as any,
    }) as Promise<
      Settled<
        Resolved<Simplify<CoMapInstanceCoValuesMaybeLoaded<Shape>> & CoMap, R>
      >
    >;
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
    return asConstructable(schemaWithCatchAll);
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
    permissions: SchemaPermissions,
  ): CoMapSchema<Shape, CatchAll, Owner, DefaultResolveQuery> {
    return this.copy({ permissions });
  }

  /**
   * Creates a copy of this schema, preserving all previous configuration
   */
  private copy<R extends ResolveQuery<this> = DefaultResolveQuery>({
    permissions,
    resolveQuery,
  }: {
    permissions?: SchemaPermissions;
    resolveQuery?: R;
  }): CoMapSchema<Shape, CatchAll, Owner, R> {
    const coreSchema = createCoreCoMapSchema(this.shape, this.catchAll);
    // @ts-expect-error
    const copy: CoMapSchema<Shape, CatchAll, Owner, ResolveQuery> =
      asConstructable(coreSchema);
    // @ts-expect-error avoid exposing 'migrate' at the type level
    copy.coValueClass.prototype.migrate = this.coValueClass.prototype.migrate;
    copy.resolveQuery = resolveQuery ?? this.resolveQuery;
    copy.permissions = permissions ?? this.permissions;
    return copy;
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
    resolveQuery: true as const,
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

RegisteredSchemas["CoMap"] = new CoMapSchema<{}>(
  createCoreCoMapSchema({}),
  CoMap,
);
