import { CoValueUniqueness, JsonValue, RawCoID } from "cojson";
import {
  Account,
  BranchDefinition,
  CoMap,
  DiscriminableCoValueSchemaDefinition,
  DiscriminableCoreCoValueSchema,
  Group,
  ID,
  Settled,
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
  SubscribeListener,
  CoreAccountSchema,
  CoreGroupSchema,
  isRefEncoded,
  instantiateRefEncodedWithInit,
  CoreCoRecordSchema,
} from "../../../internal.js";
import { RawCoMap } from "cojson";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { removeGetters, withSchemaResolveQuery } from "../../schemaUtils.js";
import { CoMapSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema, AnyZodSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";
import { TypeOfZodSchema } from "../typeConverters/TypeOfZodSchema.js";

export class CoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
  DefaultResolveQuery extends ResolveQuery<CoMapSchema<Shape, CatchAll>> = true,
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
    private coValueClass: typeof CoMap<CoreCoMapSchema<Shape, CatchAll>>,
  ) {
    this.shape = coreSchema.shape;
    this.catchAll = coreSchema.catchAll;
    this.getDefinition = coreSchema.getDefinition;
  }

  create(
    init: CoMapSchemaInit<Shape>,
    options?:
      | {
          owner?: Loaded<CoreAccountSchema, true> | Loaded<CoreGroupSchema>;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Loaded<CoreAccountSchema, true>
      | Loaded<CoreGroupSchema>,
  ): Loaded<CoreCoMapSchema<Shape, CatchAll>, true>;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: CoMapSchemaInit<Shape>,
    options?:
      | {
          owner?: Loaded<CoreAccountSchema, true> | Loaded<CoreGroupSchema>;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Loaded<CoreAccountSchema, true>
      | Loaded<CoreGroupSchema>,
  ): Loaded<CoreCoMapSchema<Shape, CatchAll>, true>;
  create(
    init: any,
    options?: any,
  ): Loaded<CoreCoMapSchema<Shape, CatchAll>, true> {
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

    const raw = this.rawFromInit(fields, init, owner, uniqueness);
    return new this.coValueClass(fields, raw, this);
  }

  private rawFromInit(
    fields: CoMapFieldSchema,
    init: object | undefined,
    owner: Group,
    uniqueness?: CoValueUniqueness,
  ) {
    const rawOwner = owner.$jazz.raw;

    const rawInit = {} as {
      [key: string]: JsonValue | undefined;
    };

    if (init)
      for (const key of Object.keys(init)) {
        const initValue = init[key as keyof typeof init];

        const descriptor = fields?.[key] || fields?.[ItemsMarker];

        if (!descriptor) {
          continue;
        }

        if (descriptor.type === "json") {
          rawInit[key] = initValue as JsonValue;
        } else if (isRefEncoded(descriptor)) {
          if (initValue != null) {
            let refId = (initValue as unknown as Loaded<CoreCoValueSchema>)
              .$jazz?.id;
            if (!refId) {
              const newOwnerStrategy =
                descriptor.permissions?.newInlineOwnerStrategy;
              const onCreate = descriptor.permissions?.onCreate;
              const coValue = instantiateRefEncodedWithInit(
                descriptor,
                initValue,
                owner,
                newOwnerStrategy,
                onCreate,
              );
              refId = coValue.$jazz.id;
            }
            rawInit[key] = refId;
          }
        } else if (descriptor.type == "encoded") {
          rawInit[key] = descriptor.encode(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            initValue as any,
          );
        }
      }

    return rawOwner.createMap(rawInit, null, "private", uniqueness);
  }

  fromRaw(raw: RawCoMap): Loaded<CoreCoMapSchema<Shape, CatchAll>> {
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
    return new this.coValueClass(fields, raw, this);
  }

  load<
    const R extends ResolveQuery<
      CoreCoMapSchema<Shape, CatchAll>
    > = DefaultResolveQuery,
  >(
    id: string,
    options?: {
      resolve?: ResolveQueryStrict<CoreCoMapSchema<Shape, CatchAll>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      skipRetry?: boolean;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<CoreCoMapSchema<Shape, CatchAll>, R>> {
    return loadCoValueWithoutMe(
      this,
      id,
      withSchemaResolveQuery(this, options),
    );
  }

  unstable_merge<
    const R extends ResolveQuery<
      CoreCoMapSchema<Shape, CatchAll>
    > = DefaultResolveQuery,
  >(
    id: string,
    options: {
      resolve?: ResolveQueryStrict<CoMapSchema<Shape, CatchAll>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      branch: BranchDefinition;
    },
  ): Promise<void> {
    const WithResolve = withSchemaResolveQuery(this, options);
    return unstable_mergeBranchWithResolve(this, id, WithResolve as any); // TODO: fix cast
  }

  subscribe<
    const R extends ResolveQuery<
      CoreCoMapSchema<Shape, CatchAll>
    > = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<CoreCoMapSchema<Shape, CatchAll>, R>,
    listener: SubscribeListener<CoreCoMapSchema<Shape, CatchAll>, R>,
  ): () => void {
    return subscribeToCoValueWithoutMe(
      this,
      id,
      withSchemaResolveQuery(this, options),
      listener,
    );
  }

  /** @deprecated Use `CoMap.upsertUnique` and `CoMap.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: string,
    as?:
      | Loaded<CoreAccountSchema, true>
      | Loaded<CoreGroupSchema>
      | AnonymousJazzAgent,
  ): string {
    const header = this._getUniqueHeader(unique, ownerID);
    return getIdFromHeader(header, as);
  }

  /** @internal */
  private _getUniqueHeader(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Loaded<CoreAccountSchema, true>> | ID<Loaded<CoreGroupSchema>>,
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
    const R extends ResolveQuery<
      CoreCoMapSchema<Shape, CatchAll>
    > = DefaultResolveQuery,
  >(options: {
    value: Simplify<CoMapSchemaInit<Shape>>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Loaded<CoreAccountSchema, true> | Loaded<CoreGroupSchema>;
    resolve?: ResolveQueryStrict<CoreCoMapSchema<Shape, CatchAll>, R>;
  }): Promise<Settled<CoreCoMapSchema<Shape, CatchAll>, R>> {
    const header = this._getUniqueHeader(
      options.unique,
      options.owner.$jazz.id,
    );

    return internalLoadUnique(this, {
      header,
      owner: options.owner,
      resolve: withSchemaResolveQuery(this, options)?.resolve,
      onCreateWhenMissing: () => {
        this.create(options.value, {
          owner: options.owner,
          unique: options.unique,
        });
      },
      onUpdateWhenFound(value) {
        (value as Loaded<CoreCoMapSchema>).$jazz.applyDiff(options.value);
      },
    });
  }

  async loadUnique<
    const R extends ResolveQuery<
      CoreCoMapSchema<Shape, CatchAll>
    > = DefaultResolveQuery,
  >(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: string,
    options?: {
      resolve?: ResolveQueryStrict<CoreCoMapSchema<Shape, CatchAll>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
    },
  ): Promise<Settled<CoreCoMapSchema<Shape, CatchAll>, R>> {
    const header = this._getUniqueHeader(unique, ownerID);

    const owner = await co.group().load(ownerID, {
      loadAs: options?.loadAs,
    });

    if (!owner.$isLoaded) return owner as any;

    return internalLoadUnique(this, {
      header,
      owner,
      resolve: withSchemaResolveQuery(this, options)?.resolve,
    });
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
    migration: (value: Loaded<CoreCoMapSchema<Shape, CatchAll>>) => undefined,
  ): this {
    // @ts-expect-error avoid exposing 'migrate' at the type level
    this.coValueClass.prototype.migrate = migration;
    return this;
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
  ): CoMapSchema<Simplify<Pick<Shape, Keys>>, unknown> {
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
  ): CoMapSchema<PartialShape<Shape, Keys>, CatchAll> {
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
    const R extends ResolveQuery<CoreCoMapSchema<Shape, CatchAll>> = true,
  >(
    resolveQuery: ResolveQueryStrict<CoreCoMapSchema<Shape, CatchAll>, R>,
  ): CoMapSchema<Shape, CatchAll, R> {
    return this.copy({ resolveQuery }) as CoMapSchema<Shape, CatchAll, R>;
  }

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: SchemaPermissions,
  ): CoMapSchema<Shape, CatchAll, DefaultResolveQuery> {
    return this.copy({ permissions });
  }

  /**
   * Creates a copy of this schema, preserving all previous configuration
   */
  private copy<
    R extends ResolveQuery<
      CoreCoMapSchema<Shape, CatchAll>
    > = DefaultResolveQuery,
  >({
    permissions,
    resolveQuery,
  }: {
    permissions?: SchemaPermissions;
    resolveQuery?: R;
  }): CoMapSchema<Shape, CatchAll, R> {
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

export type CoMapKeys<S extends CoreCoMapSchema | CoreCoRecordSchema> =
  S extends CoreCoMapSchema
    ? (keyof S["shape"] & string) | S["catchAll"] extends AnyZodOrCoValueSchema
      ? string
      : never
    : S extends CoreCoRecordSchema
      ? TypeOfZodSchema<S["keyType"]>
      : never;

export type SchemaAtKey<
  S extends CoreCoMapSchema | CoreCoRecordSchema,
  K extends CoMapKeys<S>,
> = S extends CoreCoMapSchema
  ? K extends keyof S["shape"]
    ? S["shape"][K]
    : S["catchAll"] extends AnyZodOrCoValueSchema
      ? S["catchAll"]
      : never
  : S extends CoreCoRecordSchema
    ? K extends S["keyType"]
      ? S["valueType"]
      : never
    : never;

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
