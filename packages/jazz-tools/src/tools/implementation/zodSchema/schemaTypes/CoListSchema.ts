import {
  Account,
  BranchDefinition,
  CoList,
  Group,
  hydrateCoreCoValueSchema,
  ID,
  Settled,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SubscribeCallback,
  SubscribeListenerOptions,
  coOptionalDefiner,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
  type Schema,
  CoValueCreateOptions,
} from "../../../internal.js";
import { CoValueUniqueness } from "cojson";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoListSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded.js";
import { AnyZodOrCoValueSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import { withSchemaResolveQuery } from "../../schemaUtils.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";
import { z } from "../zodReExport.js";
import {
  coValueValidationSchema,
  generateValidationSchemaFromItem,
} from "./schemaValidators.js";
import { resolveSchemaField } from "../runtimeConverters/schemaFieldToCoFieldDef.js";

export class CoListSchema<
  T extends AnyZodOrCoValueSchema,
  DefaultResolveQuery extends CoreResolveQuery = true,
> implements CoreCoListSchema<T>
{
  collaborative = true as const;
  builtin = "CoList" as const;
  #descriptorsSchema: Schema | undefined = undefined;

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

  #validationSchema: z.ZodType | undefined = undefined;
  getValidationSchema = () => {
    if (this.#validationSchema) {
      return this.#validationSchema;
    }

    const validationSchema = z.array(
      generateValidationSchemaFromItem(this.element),
    );

    this.#validationSchema = coValueValidationSchema(validationSchema, CoList);
    return this.#validationSchema;
  };

  constructor(
    public element: T,
    private coValueClass: typeof CoList,
  ) {}

  getDescriptorsSchema = (): Schema => {
    if (this.#descriptorsSchema) {
      return this.#descriptorsSchema;
    }

    this.#descriptorsSchema = resolveSchemaField(this.element as any);

    return this.#descriptorsSchema;
  };

  create(
    items: CoListSchemaInit<T>,
    options?: CoValueCreateOptions,
  ): CoListInstance<T>;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    items: CoListSchemaInit<T>,
    options?: CoValueCreateOptions<{}, Account | Group>,
  ): CoListInstance<T>;
  create(items: any, options?: any): CoListInstance<T> {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    return this.coValueClass.create(
      items as any,
      optionsWithPermissions,
    ) as CoListInstance<T>;
  }

  load<
    const R extends RefsToResolve<
      CoListInstanceCoValuesMaybeLoaded<T>
    > = DefaultResolveQuery,
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.load(
      id,
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  unstable_merge<
    const R extends RefsToResolve<
      CoListInstanceCoValuesMaybeLoaded<T>
    > = DefaultResolveQuery,
  >(
    id: string,
    options: {
      resolve?: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>;
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
      CoListInstanceCoValuesMaybeLoaded<T>
    > = DefaultResolveQuery,
  >(
    id: string,
    listener: SubscribeCallback<
      Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>
    >,
  ): () => void;
  subscribe<
    const R extends RefsToResolve<
      CoListInstanceCoValuesMaybeLoaded<T>
    > = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<CoListInstanceCoValuesMaybeLoaded<T>, R>,
    listener: SubscribeCallback<
      Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>
    >,
  ): () => void;
  subscribe<
    const R extends RefsToResolve<CoListInstanceCoValuesMaybeLoaded<T>>,
  >(
    id: string,
    optionsOrListener:
      | SubscribeListenerOptions<CoListInstanceCoValuesMaybeLoaded<T>, R>
      | SubscribeCallback<Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>>,
    maybeListener?: SubscribeCallback<
      Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>
    >,
  ): () => void {
    if (typeof optionsOrListener === "function") {
      return this.coValueClass.subscribe(
        id,
        withSchemaResolveQuery({}, this.resolveQuery),
        optionsOrListener,
      );
    }
    return this.coValueClass.subscribe(
      id,
      withSchemaResolveQuery(optionsOrListener, this.resolveQuery),
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
      maybeListener,
    );
  }

  getCoValueClass(): typeof CoList {
    return this.coValueClass;
  }

  /** @deprecated Use `loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ): ID<CoListInstanceCoValuesMaybeLoaded<T>> {
    return this.coValueClass.findUnique(unique, ownerID, as);
  }

  /**
   * Get an existing unique CoList or create a new one if it doesn't exist.
   *
   * Unlike `upsertUnique`, this method does NOT update existing values with the provided value.
   * The provided value is only used when creating a new CoList.
   *
   * @example
   * ```ts
   * const items = await ItemList.getOrCreateUnique({
   *   value: [item1, item2, item3],
   *   unique: ["user-items", me.id],
   *   owner: me,
   * });
   * ```
   *
   * @param options The options for creating or loading the CoList.
   * @returns Either an existing CoList (unchanged), or a new initialised CoList if none exists.
   * @category Subscription & Loading
   */
  getOrCreateUnique<
    const R extends RefsToResolve<
      CoListInstanceCoValuesMaybeLoaded<T>
    > = DefaultResolveQuery,
  >(options: {
    value: CoListSchemaInit<T>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Account | Group;
    resolve?: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>;
  }): Promise<Settled<Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.getOrCreateUnique(
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  /**
   * @deprecated Use `getOrCreateUnique` instead. Note: getOrCreateUnique does not update existing values.
   * If you need to update, use getOrCreateUnique followed by `$jazz.applyDiff`.
   */
  upsertUnique<
    const R extends RefsToResolve<
      CoListInstanceCoValuesMaybeLoaded<T>
    > = DefaultResolveQuery,
  >(options: {
    value: CoListSchemaInit<T>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Account | Group;
    resolve?: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>;
  }): Promise<Settled<Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.upsertUnique(
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  loadUnique<
    const R extends RefsToResolve<
      CoListInstanceCoValuesMaybeLoaded<T>
    > = DefaultResolveQuery,
  >(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    options?: {
      resolve?: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Settled<Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.loadUnique(
      unique,
      ownerID,
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  /**
   * Adds a default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   */
  resolved<
    const R extends RefsToResolve<CoListInstanceCoValuesMaybeLoaded<T>> = true,
  >(
    resolveQuery: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>,
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

  private copy<ResolveQuery extends CoreResolveQuery = DefaultResolveQuery>({
    permissions,
    resolveQuery,
  }: {
    permissions?: SchemaPermissions;
    resolveQuery?: ResolveQuery;
  }): CoListSchema<T, ResolveQuery> {
    const coreSchema = createCoreCoListSchema(this.element);
    // @ts-expect-error
    const copy: CoListSchema<T, ResolveQuery> =
      hydrateCoreCoValueSchema(coreSchema);
    // @ts-expect-error TS cannot infer that the resolveQuery type is valid
    copy.resolveQuery = resolveQuery ?? this.resolveQuery;
    copy.#permissions = permissions ?? this.#permissions;
    return copy;
  }
}

export function createCoreCoListSchema<T extends AnyZodOrCoValueSchema>(
  element: T,
): CoreCoListSchema<T> {
  let descriptorsSchema: Schema | undefined;

  return {
    collaborative: true as const,
    builtin: "CoList" as const,
    element,
    getDescriptorsSchema: () => {
      if (descriptorsSchema) {
        return descriptorsSchema;
      }

      descriptorsSchema = resolveSchemaField(element as any);

      return descriptorsSchema;
    },
    resolveQuery: true as const,
    getValidationSchema: () => z.any(),
  };
}

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoListSchema<
  T extends AnyZodOrCoValueSchema = AnyZodOrCoValueSchema,
> extends CoreCoValueSchema {
  builtin: "CoList";
  element: T;
  getDescriptorsSchema: () => Schema;
}

export type CoListInstance<T extends AnyZodOrCoValueSchema> = CoList<
  InstanceOrPrimitiveOfSchema<T>
>;

export type CoListInstanceCoValuesMaybeLoaded<T extends AnyZodOrCoValueSchema> =
  CoList<InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<T>>;
