import { CoValueUniqueness } from "cojson";
import {
  Account,
  AnyZodOrCoValueSchema,
  BranchDefinition,
  CoFeed,
  Group,
  hydrateCoreCoValueSchema,
  Settled,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SubscribeCallback,
  SubscribeListenerOptions,
  coOptionalDefiner,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoFeedSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import { withSchemaResolveQuery } from "../../schemaUtils.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";
import { z } from "../zodReExport.js";
import { generateValidationSchemaFromItem } from "./schemaValidators.js";
import { type LocalValidationMode } from "../validationSettings.js";

export class CoFeedSchema<
  T extends AnyZodOrCoValueSchema,
  DefaultResolveQuery extends CoreResolveQuery = true,
> implements CoreCoFeedSchema<T>
{
  collaborative = true as const;
  builtin = "CoFeed" as const;

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

    this.#validationSchema = z
      .instanceof(CoFeed)
      .or(z.array(generateValidationSchemaFromItem(this.element)));
    return this.#validationSchema;
  };

  constructor(
    public element: T,
    private coValueClass: typeof CoFeed,
  ) {}

  create(
    init: CoFeedSchemaInit<T>,
    options?:
      | { owner: Group; unique?: CoValueUniqueness["uniqueness"] }
      | { owner?: Group; validation?: LocalValidationMode }
      | Group,
  ): CoFeedInstance<T>;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: CoFeedSchemaInit<T>,
    options?:
      | { owner: Account | Group; unique?: CoValueUniqueness["uniqueness"] }
      | { owner?: Account | Group; validation?: LocalValidationMode }
      | Account
      | Group,
  ): CoFeedInstance<T>;
  create(
    init: CoFeedSchemaInit<T>,
    options?:
      | { owner: Account | Group; unique?: CoValueUniqueness["uniqueness"] }
      | { owner?: Account | Group; validation?: LocalValidationMode }
      | Account
      | Group,
  ): CoFeedInstance<T> {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    return this.coValueClass.create(
      init as any,
      optionsWithPermissions,
    ) as CoFeedInstance<T>;
  }

  load<
    const R extends RefsToResolve<
      CoFeedInstanceCoValuesMaybeLoaded<T>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoFeedInstanceCoValuesMaybeLoaded<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.load(
      id,
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  unstable_merge<
    const R extends RefsToResolve<
      CoFeedInstanceCoValuesMaybeLoaded<T>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: string,
    options: {
      resolve?: RefsToResolveStrict<CoFeedInstanceCoValuesMaybeLoaded<T>, R>;
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
      CoFeedInstanceCoValuesMaybeLoaded<T>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: string,
    listener: SubscribeCallback<
      Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>
    >,
  ): () => void;
  subscribe<
    const R extends RefsToResolve<
      CoFeedInstanceCoValuesMaybeLoaded<T>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<CoFeedInstanceCoValuesMaybeLoaded<T>, R>,
    listener: SubscribeCallback<
      Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>
    >,
  ): () => void;
  subscribe<
    const R extends RefsToResolve<CoFeedInstanceCoValuesMaybeLoaded<T>>,
  >(
    id: string,
    optionsOrListener:
      | SubscribeListenerOptions<CoFeedInstanceCoValuesMaybeLoaded<T>, R>
      | SubscribeCallback<Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>>,
    maybeListener?: SubscribeCallback<
      Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>
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
      // @ts-expect-error
      maybeListener,
    );
  }

  getCoValueClass(): typeof CoFeed {
    return this.coValueClass;
  }

  /**
   * Get an existing unique CoFeed or create a new one if it doesn't exist.
   *
   * The provided value is only used when creating a new CoFeed.
   *
   * @example
   * ```ts
   * const feed = await MessageFeed.getOrCreateUnique({
   *   value: [],
   *   unique: ["messages", conversationId],
   *   owner: group,
   * });
   * ```
   *
   * @param options The options for creating or loading the CoFeed.
   * @returns Either an existing CoFeed (unchanged), or a new initialised CoFeed if none exists.
   * @category Subscription & Loading
   */
  getOrCreateUnique<
    const R extends RefsToResolve<
      CoFeedInstanceCoValuesMaybeLoaded<T>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(options: {
    value: CoFeedSchemaInit<T>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Account | Group;
    resolve?: RefsToResolveStrict<CoFeedInstanceCoValuesMaybeLoaded<T>, R>;
  }): Promise<Settled<Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.getOrCreateUnique(
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
    const R extends RefsToResolve<CoFeedInstanceCoValuesMaybeLoaded<T>> = true,
  >(
    resolveQuery: RefsToResolveStrict<CoFeedInstanceCoValuesMaybeLoaded<T>, R>,
  ): CoFeedSchema<T, R> {
    return this.copy({ resolveQuery: resolveQuery as R });
  }

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: SchemaPermissions,
  ): CoFeedSchema<T, DefaultResolveQuery> {
    return this.copy({ permissions });
  }

  private copy<ResolveQuery extends CoreResolveQuery = DefaultResolveQuery>({
    permissions,
    resolveQuery,
  }: {
    permissions?: SchemaPermissions;
    resolveQuery?: ResolveQuery;
  }): CoFeedSchema<T, ResolveQuery> {
    const coreSchema = createCoreCoFeedSchema(this.element);
    // @ts-expect-error
    const copy: CoFeedSchema<T, ResolveQuery> =
      hydrateCoreCoValueSchema(coreSchema);
    // @ts-expect-error TS cannot infer that the resolveQuery type is valid
    copy.resolveQuery = resolveQuery ?? this.resolveQuery;
    copy.#permissions = permissions ?? this.#permissions;
    return copy;
  }
}

export function createCoreCoFeedSchema<T extends AnyZodOrCoValueSchema>(
  element: T,
): CoreCoFeedSchema<T> {
  return {
    collaborative: true as const,
    builtin: "CoFeed" as const,
    element,
    resolveQuery: true as const,
    getValidationSchema: () => z.any(),
  };
}

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoFeedSchema<
  T extends AnyZodOrCoValueSchema = AnyZodOrCoValueSchema,
> extends CoreCoValueSchema {
  builtin: "CoFeed";
  element: T;
}

export type CoFeedInstance<T extends AnyZodOrCoValueSchema> = CoFeed<
  InstanceOrPrimitiveOfSchema<T>
>;

export type CoFeedInstanceCoValuesMaybeLoaded<T extends AnyZodOrCoValueSchema> =
  CoFeed<InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<T>>;
