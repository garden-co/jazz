import {
  Account,
  AnyZodOrCoValueSchema,
  BranchDefinition,
  CoFeed,
  Group,
  asConstructable,
  Settled,
  SubscribeListenerOptions,
  coOptionalDefiner,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
  parseCoValueCreateOptions,
  SchemaField,
  schemaFieldToFieldDescriptor,
  ResolveQuery,
  ResolveQueryStrict,
  Loaded,
  CoreAccountSchema,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoFeedSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import { withSchemaResolveQuery } from "../../schemaUtils.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";
import { RawCoStream } from "cojson";

export class CoFeedSchema<
  T extends AnyZodOrCoValueSchema,
  DefaultResolveQuery extends ResolveQuery<CoreCoFeedSchema<T>> = true,
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

  /**
   * Permissions to be used when creating or composing CoValues
   * @internal
   */
  permissions: SchemaPermissions = DEFAULT_SCHEMA_PERMISSIONS;

  constructor(
    public element: T,
    private coValueClass: typeof CoFeed,
  ) {}

  create(
    init: CoFeedSchemaInit<T>,
    options?: { owner: Group } | Group,
  ): Loaded<CoreCoFeedSchema<T>>;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: CoFeedSchemaInit<T>,
    options?:
      | { owner: Loaded<CoreAccountSchema, true> | Group }
      | Loaded<CoreAccountSchema, true>
      | Group,
  ): Loaded<CoreCoFeedSchema<T>>;
  create(
    init: CoFeedSchemaInit<T>,
    options?:
      | { owner: Loaded<CoreAccountSchema, true> | Group }
      | Loaded<CoreAccountSchema, true>
      | Group,
  ): Loaded<CoreCoFeedSchema<T>> {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    const { owner } = parseCoValueCreateOptions(options);

    const itemFieldDescriptor = schemaFieldToFieldDescriptor(
      this.element as SchemaField, // TODO we should enforce this at runtime
    );

    const raw = owner.$jazz.raw.createStream();
    const instance = new this.coValueClass(
      itemFieldDescriptor,
      raw,
      this as CoreCoFeedSchema<T>,
    );

    if (init) {
      instance.$jazz.push(...init);
    }
    return instance;
  }

  fromRaw(raw: RawCoStream): Loaded<CoreCoFeedSchema<T>> {
    const itemFieldDescriptor = schemaFieldToFieldDescriptor(
      this.element as SchemaField, // TODO we should enforce this at runtime
    );
    return new this.coValueClass(itemFieldDescriptor, raw, this);
  }

  load<const R extends ResolveQuery<CoreCoFeedSchema<T>> = DefaultResolveQuery>(
    id: string,
    options?: {
      resolve?: ResolveQueryStrict<CoreCoFeedSchema<T>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<CoreCoFeedSchema<T>, R>> {
    return loadCoValueWithoutMe(
      this,
      id,
      withSchemaResolveQuery(this, options),
    );
  }

  unstable_merge<
    const R extends ResolveQuery<CoreCoFeedSchema<T>> = DefaultResolveQuery,
  >(
    id: string,
    options: {
      resolve?: ResolveQueryStrict<CoreCoFeedSchema<T>, R>;
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

  subscribe(
    id: string,
    listener: (
      value: Loaded<CoreCoFeedSchema<T>, true>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe<
    const R extends ResolveQuery<CoreCoFeedSchema<T>> = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<CoreCoFeedSchema<T>, R>,
    listener: (
      value: Loaded<CoreCoFeedSchema<T>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe(id: string, ...args: any) {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe(
      this,
      id,
      withSchemaResolveQuery(this, options),
      listener,
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
    const R extends ResolveQuery<CoreCoFeedSchema<T>> = DefaultResolveQuery,
  >(
    resolveQuery: R & ResolveQueryStrict<CoreCoFeedSchema<T>, R>,
  ): CoFeedSchema<T, R> {
    return this.copy({ resolveQuery });
  }

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: SchemaPermissions,
  ): CoFeedSchema<T, DefaultResolveQuery> {
    return this.copy({ permissions });
  }

  private copy<
    R extends ResolveQuery<CoreCoFeedSchema<T>> = DefaultResolveQuery,
  >({
    permissions,
    resolveQuery,
  }: {
    permissions?: SchemaPermissions;
    resolveQuery?: R;
  }): CoFeedSchema<T, R> {
    const coreSchema = createCoreCoFeedSchema(this.element);
    // @ts-expect-error
    const copy: CoFeedSchema<T, R> = asConstructable(coreSchema);
    copy.resolveQuery = resolveQuery ?? (this.resolveQuery as unknown as R);
    copy.permissions = permissions ?? this.permissions;
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
  };
}

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoFeedSchema<
  T extends AnyZodOrCoValueSchema = AnyZodOrCoValueSchema,
> extends CoreCoValueSchema {
  builtin: "CoFeed";
  element: T;
}
