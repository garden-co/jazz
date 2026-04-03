import {
  Account,
  AnonymousJazzAgent,
  CoValueCreateOptions,
  RefsToResolve,
  RefsToResolveStrict,
  Settled,
  coOptionalDefiner,
  InstanceOfSchemaCoValuesMaybeLoaded,
  hydrateCoreCoValueSchema,
  coMapDefiner,
  CoMapSchema,
  SnapshotRef,
  Resolved,
  InstanceOfSchema,
  AsLoaded,
} from "../../../internal.js";
import { withSchemaResolveQuery } from "../../schemaUtils.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";
import { z } from "../zodReExport.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";

type SnapshotRefInstanceMaybeLoaded<S extends CoreCoValueSchema> = SnapshotRef<
  InstanceOfSchemaCoValuesMaybeLoaded<S>
>;

type SnapshotRefMapSchema = CoMapSchema<{
  ref: z.ZodString;
  cursor: z.ZodString;
}>;

/**
 * Core schema definition for a SnapshotRef CoValue.
 *
 * Contains the minimal shape information needed to hydrate a full {@link SnapshotRefSchema}.
 *
 * @category Schema
 */
export interface CoreSnapshotRefSchema<
  S extends CoreCoValueSchema = CoreCoValueSchema,
  R extends CoreResolveQuery = true,
> extends CoreCoValueSchema {
  builtin: "SnapshotRef";
  snapshotRefMapSchema: SnapshotRefMapSchema;
  innerSchema: S;
  cursorResolveQuery: R;
}

/**
 * Schema definition for a `SnapshotRef` CoValue.
 *
 * Provides methods to create, load, and configure SnapshotRef instances.
 * Use `co.snapshotRef(innerSchema)` to obtain a `SnapshotRefSchema`.
 *
 * @category Schema
 */
export class SnapshotRefSchema<
  S extends CoreCoValueSchema = CoreCoValueSchema,
  CursorResolveQuery extends CoreResolveQuery = true,
  DefaultResolveQuery extends CoreResolveQuery = true,
> implements CoreSnapshotRefSchema<S, CursorResolveQuery>
{
  readonly collaborative = true as const;
  readonly builtin = "SnapshotRef" as const;
  /** The inner CoValue schema that SnapshotRef instances will reference. */
  readonly innerSchema: S;
  /** The underlying CoMap schema used to persist the snapshot reference data. */
  readonly snapshotRefMapSchema: SnapshotRefMapSchema;
  /** The resolve query applied when loading the snapshot's cursor. */
  readonly cursorResolveQuery: CursorResolveQuery;

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

  /** @internal */
  getValidationSchema: CoreCoValueSchema["getValidationSchema"];

  /** @internal */
  static _refMapSchema:
    | CoMapSchema<{
        ref: z.ZodString;
        cursor: z.ZodString;
      }>
    | undefined;

  /** @internal */
  static get refMapSchema() {
    if (this._refMapSchema) {
      return this._refMapSchema;
    }

    this._refMapSchema = coMapDefiner({
      ref: z.string(),
      cursor: z.string(),
    });

    return this._refMapSchema;
  }

  /** @internal */
  constructor(
    coreSchema: CoreSnapshotRefSchema<S, CursorResolveQuery>,
    private coValueClass: typeof SnapshotRef,
  ) {
    this.innerSchema = coreSchema.innerSchema;
    this.cursorResolveQuery = coreSchema.cursorResolveQuery;
    this.getValidationSchema = coreSchema.getValidationSchema;
    this.snapshotRefMapSchema = coreSchema.snapshotRefMapSchema;
  }

  /**
   * Create a new `SnapshotRef` pointing to the given CoValue.
   *
   * The passed CoValue does not need to be fully loaded — the function will
   * deeply load and snapshot the structure based on the schema's `cursorResolveQuery`.
   *
   * The SnapshotRef will immediately be persisted and synced to connected peers.
   *
   * @category Creation
   */
  async create(
    value: AsLoaded<InstanceOfSchemaCoValuesMaybeLoaded<S>>,
    options?: CoValueCreateOptions,
  ): Promise<
    // @ts-expect-error - cannot statically enforce CursorResolveQuery validity
    Settled<SnapshotRef<Resolved<InstanceOfSchema<S>, CursorResolveQuery>>>
  > {
    // @ts-expect-error
    return this.coValueClass.create(
      {
        // @ts-expect-error
        value,
        // @ts-expect-error cannot statically enforce schema default resolve validity
        cursorResolve: this.cursorResolveQuery,
      },
      this.permissions,
      options,
    );
  }

  async load<
    const R extends RefsToResolve<
      SnapshotRefInstanceMaybeLoaded<S>
      // @ts-expect-error
    > = DefaultResolveQuery,
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<SnapshotRefInstanceMaybeLoaded<S>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      skipRetry?: boolean;
    },
  ): Promise<Settled<Resolved<SnapshotRefInstanceMaybeLoaded<S>, R>>> {
    // @ts-expect-error
    return this.coValueClass.load(
      id,
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  /** @internal */
  getCoValueClass(): typeof SnapshotRef {
    return this.coValueClass;
  }

  /**
   * Mark this schema field as optional when used inside a CoMap schema.
   */
  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  private copy<ResolveQuery extends CoreResolveQuery = DefaultResolveQuery>({
    permissions,
    resolveQuery,
  }: {
    permissions?: SchemaPermissions;
    resolveQuery?: ResolveQuery;
  }): SnapshotRefSchema<S, CursorResolveQuery, ResolveQuery> {
    const coreSchema = createCoreSnapshotRefSchema(this.innerSchema, {
      cursorResolve: this.cursorResolveQuery,
    });

    // @ts-expect-error
    const copy: SnapshotRefSchema<S, R, ResolveQuery> =
      hydrateCoreCoValueSchema(coreSchema);
    // @ts-expect-error TS cannot infer that the resolveQuery type is valid
    copy.resolveQuery = resolveQuery ?? this.resolveQuery;
    copy.#permissions = permissions ?? this.#permissions;
    return copy;
  }

  /**
   * Adds a default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   */
  resolved<
    const ResolveQuery extends RefsToResolve<
      SnapshotRefInstanceMaybeLoaded<S>
    > = true,
  >(
    resolveQuery: RefsToResolveStrict<
      SnapshotRefInstanceMaybeLoaded<S>,
      ResolveQuery
    >,
  ): SnapshotRefSchema<S, CursorResolveQuery, ResolveQuery> {
    return this.copy({ resolveQuery: resolveQuery as ResolveQuery });
  }

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: SchemaPermissions,
  ): SnapshotRefSchema<S, CursorResolveQuery, DefaultResolveQuery> {
    return this.copy({ permissions });
  }
}

/** @internal */
export function createCoreSnapshotRefSchema<
  S extends CoreCoValueSchema,
  R extends CoreResolveQuery = true,
>(
  schema: S,
  options?: {
    cursorResolve?: R;
  },
): CoreSnapshotRefSchema<S, R> {
  return {
    collaborative: true as const,
    builtin: "SnapshotRef" as const,
    innerSchema: schema,
    snapshotRefMapSchema: SnapshotRefSchema.refMapSchema,
    resolveQuery: true,
    // @ts-expect-error
    cursorResolveQuery: options?.cursorResolve ?? (true as const),
    getValidationSchema: () => z.any(),
  };
}
