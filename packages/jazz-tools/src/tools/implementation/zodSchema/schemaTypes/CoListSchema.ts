import {
  Account,
  BranchDefinition,
  CoList,
  Group,
  co,
  asConstructable,
  ID,
  Settled,
  SubscribeListenerOptions,
  coOptionalDefiner,
  internalLoadUnique,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
  getIdFromHeader,
  parseCoValueCreateOptions,
  toRawItems,
  schemaFieldToFieldDescriptor,
  SchemaField,
  ResolveQuery,
  ResolveQueryStrict,
  SubscribeListener,
  Loaded,
  CoreAccountSchema,
  CoreGroupSchema,
} from "../../../internal.js";
import { CoValueUniqueness, RawCoID, RawCoList } from "cojson";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoListSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { AnyZodOrCoValueSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import { withSchemaResolveQuery } from "../../schemaUtils.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";

export class CoListSchema<
  T extends AnyZodOrCoValueSchema,
  DefaultResolveQuery extends ResolveQuery<CoreCoListSchema<T>> = true,
> implements CoreCoListSchema<T>
{
  collaborative = true as const;
  builtin = "CoList" as const;

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
    public element: T,
    private coValueClass: typeof CoList,
  ) {}

  create(
    items: CoListSchemaInit<T>,
    options?:
      | { owner: Group; unique?: CoValueUniqueness["uniqueness"] }
      | Group,
  ): Loaded<CoreCoListSchema<T>>;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    items: CoListSchemaInit<T>,
    options?:
      | {
          owner: Loaded<CoreAccountSchema, true> | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Loaded<CoreAccountSchema, true>
      | Group,
  ): Loaded<CoreCoListSchema<T>>;
  create(
    items: CoListSchemaInit<T>,
    options?:
      | {
          owner: Loaded<CoreAccountSchema, true> | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Loaded<CoreAccountSchema, true>
      | Group,
  ): Loaded<CoreCoListSchema<T>> {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );

    const { owner, uniqueness } = parseCoValueCreateOptions(
      optionsWithPermissions,
    );

    const itemFieldDescriptor = schemaFieldToFieldDescriptor(
      this.element as SchemaField, // TODO we should enforce this at runtime
    );

    const raw = owner.$jazz.raw.createList(
      toRawItems([...items], itemFieldDescriptor, owner),
      null,
      "private",
      uniqueness,
    );

    return new this.coValueClass(itemFieldDescriptor, raw, this);
  }

  fromRaw(raw: RawCoList): Loaded<CoreCoListSchema<T>> {
    const itemFieldDescriptor = schemaFieldToFieldDescriptor(
      this.element as SchemaField, // TODO we should enforce this at runtime
    );

    return new this.coValueClass(itemFieldDescriptor, raw, this);
  }

  load<const R extends ResolveQuery<CoreCoListSchema<T>> = DefaultResolveQuery>(
    id: string,
    options?: {
      resolve?: ResolveQueryStrict<CoreCoListSchema<T>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<CoreCoListSchema<T>, R>> {
    return loadCoValueWithoutMe(
      this,
      id,
      withSchemaResolveQuery(this, options),
    );
  }

  unstable_merge<
    const R extends ResolveQuery<CoreCoListSchema<T>> = DefaultResolveQuery,
  >(
    id: string,
    options: {
      resolve?: ResolveQueryStrict<CoreCoListSchema<T>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      branch: BranchDefinition;
    },
  ): Promise<void> {
    return unstable_mergeBranchWithResolve(
      this,
      id,
      withSchemaResolveQuery(this, options),
    );
  }

  subscribe<
    const R extends ResolveQuery<CoreCoListSchema<T>> = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<CoreCoListSchema<T>, R>,
    listener: SubscribeListener<CoreCoListSchema<T>, R>,
  ): () => void {
    return subscribeToCoValueWithoutMe(
      this,
      id,
      withSchemaResolveQuery(this, options),
      listener,
    );
  }

  /** @deprecated Use `CoList.upsertUnique` and `CoList.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Loaded<CoreAccountSchema, true>> | ID<Group>,
    as?:
      | Loaded<CoreAccountSchema, true>
      | Loaded<CoreGroupSchema>
      | AnonymousJazzAgent,
  ): ID<CoreCoListSchema<T>> {
    const header = this._getUniqueHeader(unique, ownerID);
    return getIdFromHeader(header, as);
  }

  /** @internal */
  private _getUniqueHeader(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Loaded<CoreAccountSchema, true>> | ID<Loaded<CoreGroupSchema>>,
  ) {
    return {
      type: "colist" as const,
      ruleset: {
        type: "ownedByGroup" as const,
        group: ownerID as RawCoID,
      },
      meta: null,
      uniqueness: unique,
    };
  }

  upsertUnique<
    const R extends ResolveQuery<CoreCoListSchema<T>> = DefaultResolveQuery,
  >(options: {
    value: CoListSchemaInit<T>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Loaded<CoreAccountSchema, true> | Loaded<CoreGroupSchema>;
    resolve?: ResolveQueryStrict<CoreCoListSchema<T>, R>;
  }): Promise<Settled<CoreCoListSchema<T>, R>> {
    const header = this._getUniqueHeader(
      options.unique,
      options.owner.$jazz.id,
    );

    return internalLoadUnique<CoreCoListSchema<T>, R>(this, {
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
        (value as Loaded<CoreCoListSchema<T>, R>).$jazz.applyDiff(
          options.value,
        );
      },
    }) as Promise<Settled<CoreCoListSchema<T>, R>>;
  }

  async loadUnique<
    const R extends ResolveQuery<CoreCoListSchema<T>> = DefaultResolveQuery,
  >(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Loaded<CoreAccountSchema, true>> | ID<Loaded<CoreGroupSchema>>,
    options?: {
      resolve?: ResolveQueryStrict<CoreCoListSchema<T>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
    },
  ): Promise<Settled<CoreCoListSchema<T>, R>> {
    const header = this._getUniqueHeader(unique, ownerID);

    const owner = await co.group().load(ownerID, {
      loadAs: options?.loadAs,
    });
    if (!owner.$isLoaded) return owner;

    return internalLoadUnique<CoreCoListSchema<T>, R>(this, {
      header,
      owner,
      resolve: withSchemaResolveQuery(this, options)?.resolve,
    });
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  /**
   * Adds a default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   */
  resolved<const R extends ResolveQuery<CoreCoListSchema<T>> = true>(
    resolveQuery: ResolveQueryStrict<CoreCoListSchema<T>, R>,
  ): CoListSchema<T, R> {
    return this.copy({ resolveQuery: resolveQuery as R });
  }

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: SchemaPermissions,
  ): CoListSchema<T, DefaultResolveQuery> {
    return this.copy({ permissions });
  }

  private copy<
    R extends ResolveQuery<CoreCoListSchema<T>> = DefaultResolveQuery,
  >({
    permissions,
    resolveQuery,
  }: {
    permissions?: SchemaPermissions;
    resolveQuery?: R;
  }): CoListSchema<T, R> {
    const coreSchema = createCoreCoListSchema(this.element);
    const copy = asConstructable(coreSchema) as unknown as CoListSchema<T, R>;
    copy.resolveQuery = resolveQuery ?? (this.resolveQuery as unknown as R);
    copy.permissions = permissions ?? this.permissions;
    return copy;
  }
}

export function createCoreCoListSchema<T extends AnyZodOrCoValueSchema>(
  element: T,
): CoreCoListSchema<T> {
  return {
    collaborative: true as const,
    builtin: "CoList" as const,
    element,
    resolveQuery: true as const,
  };
}

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoListSchema<
  T extends AnyZodOrCoValueSchema = AnyZodOrCoValueSchema,
> extends CoreCoValueSchema {
  builtin: "CoList";
  element: T;
}
