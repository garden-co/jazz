import {
  Account,
  BranchDefinition,
  CoMapSchemaDefinition,
  coOptionalDefiner,
  Group,
  Settled,
  RefsToResolveStrict,
  RefsToResolve,
  Resolved,
  Simplify,
  SubscribeCallback,
  SubscribeListenerOptions,
  unstable_mergeBranchWithResolve,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema, Loaded, ResolveQuery } from "../zodSchema.js";
import {
  CoMapSchema,
  CoMapDescriptorsSchema,
  CoreCoMapSchema,
  createCoreCoMapSchema,
} from "./CoMapSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreResolveQuery } from "./CoValueSchema.js";
import { withSchemaResolveQuery } from "../../schemaUtils.js";
import { AgentSecret } from "cojson";

export type BaseProfileShape = {
  name: z.core.$ZodString<string>;
  inbox?: z.core.$ZodOptional<z.core.$ZodString>;
  inboxInvite?: z.core.$ZodOptional<z.core.$ZodString>;
};

export type BaseAccountShape = {
  profile: CoreCoMapSchema<BaseProfileShape>;
  root: CoreCoMapSchema;
};

export type DefaultAccountShape = {
  profile: CoMapSchema<BaseProfileShape>;
  root: CoMapSchema<{}>;
};

export class AccountSchema<
  Shape extends BaseAccountShape = DefaultAccountShape,
  DefaultResolveQuery extends CoreResolveQuery = true,
> implements CoreAccountSchema<Shape>
{
  collaborative = true as const;
  builtin = "Account" as const;
  shape: Shape;
  getDescriptorsSchema: () => CoMapDescriptorsSchema;
  getDefinition: () => CoMapSchemaDefinition;

  #validationSchema: z.ZodType | undefined = undefined;

  getValidationSchema = () => {
    if (this.#validationSchema) {
      return this.#validationSchema;
    }

    this.#validationSchema = z.instanceof(Account).or(
      z.object({
        profile: this.shape.profile.getValidationSchema(),
        root: z.optional(this.shape.root.getValidationSchema()),
      }),
    );

    return this.#validationSchema;
  };

  /**
   * Default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   * @default true
   */
  resolveQuery: DefaultResolveQuery = true as DefaultResolveQuery;

  constructor(
    coreSchema: CoreAccountSchema<Shape>,
    private coValueClass: typeof Account,
  ) {
    this.shape = coreSchema.shape;
    this.getDescriptorsSchema = coreSchema.getDescriptorsSchema;
    this.getDefinition = coreSchema.getDefinition;
  }

  create(
    options: Simplify<Parameters<(typeof Account)["create"]>[0]>,
  ): Promise<AccountInstance<Shape>> {
    // @ts-expect-error
    return this.coValueClass.create(options);
  }

  load<
    // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    const R extends ResolveQuery<AccountSchema<Shape>> = DefaultResolveQuery,
  >(
    id: string,
    options?: {
      loadAs?: Account | AnonymousJazzAgent;
      resolve?: RefsToResolveStrict<AccountSchema<Shape>, R>;
    },
  ): Promise<Settled<Loaded<AccountSchema<Shape>, R>>> {
    // @ts-expect-error
    return this.coValueClass.load(
      id,
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    );
  }

  /**
   * Creates a new account as a worker account, useful for generating controlled accounts from a server environment.
   * This method initializes a new account, applies migrations, invokes the `onCreate` callback, and then shuts down the temporary node to avoid memory leaks.
   * Returns the created account (loaded on the worker) and its credentials.
   *
   * The method internally calls `waitForAllCoValuesSync` on the new account. If many CoValues are created during `onCreate`,
   * consider adjusting the timeout using the `waitForSyncTimeout` option.
   *
   * @param worker - The worker account to create the new account from
   * @param options.creationProps - The creation properties for the new account
   * @param options.onCreate - The callback to use to initialize the account after it is created
   * @param options.waitForSyncTimeout - The timeout for the sync to complete
   * @returns The credentials and the created account loaded by the worker account
   *
   *
   * @example
   * ```ts
   * const { credentials, account } = await AccountSchema.createAs(worker, {
   *   creationProps: { name: "My Account" },
   *   onCreate: async (account, worker, credentials) => {
   *     account.root.$jazz.owner.addMember(worker, "writer");
   *   },
   * });
   * ```
   */
  createAs(
    worker: Account,
    options: {
      creationProps: { name: string };
      onCreate?: (
        account: AccountInstance<Shape>,
        worker: Account,
        credentials: { accountID: string; accountSecret: AgentSecret },
      ) => Promise<void>;
      waitForSyncTimeout?: number;
    },
  ): Promise<{
    credentials: {
      accountID: string;
      accountSecret: AgentSecret;
    };
    // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    account: Loaded<AccountSchema<Shape>, DefaultResolveQuery>;
  }> {
    // @ts-expect-error
    return this.coValueClass.createAs(worker, options);
  }

  unstable_merge<
    // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    R extends ResolveQuery<AccountSchema<Shape>> = DefaultResolveQuery,
  >(
    id: string,
    options: {
      loadAs?: Account | AnonymousJazzAgent;
      resolve?: RefsToResolveStrict<AccountSchema<Shape>, R>;
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
      Simplify<AccountInstance<Shape>>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: string,
    listener: SubscribeCallback<Resolved<Simplify<AccountInstance<Shape>>, R>>,
  ): () => void;
  subscribe<
    const R extends RefsToResolve<
      Simplify<AccountInstance<Shape>>
      // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
    > = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<Simplify<AccountInstance<Shape>>, R>,
    listener: SubscribeCallback<Resolved<Simplify<AccountInstance<Shape>>, R>>,
  ): () => void;
  subscribe<const R extends RefsToResolve<Simplify<AccountInstance<Shape>>>>(
    id: string,
    optionsOrListener:
      | SubscribeListenerOptions<Simplify<AccountInstance<Shape>>, R>
      | SubscribeCallback<Resolved<Simplify<AccountInstance<Shape>>, R>>,
    maybeListener?: SubscribeCallback<
      Resolved<Simplify<AccountInstance<Shape>>, R>
    >,
  ): () => void {
    if (typeof optionsOrListener === "function") {
      return this.coValueClass.subscribe(
        id,
        withSchemaResolveQuery({}, this.resolveQuery),
        // @ts-expect-error
        optionsOrListener,
      );
    }
    return this.coValueClass.subscribe(
      id,
      // @ts-expect-error
      withSchemaResolveQuery(optionsOrListener, this.resolveQuery),
      maybeListener,
    );
  }

  getMe(): Loaded<this, true> {
    // @ts-expect-error
    return this.coValueClass.getMe();
  }

  withMigration(
    migration: (
      account: Loaded<AccountSchema<Shape>>,
      creationProps?: { name: string },
    ) => void,
  ): AccountSchema<Shape, DefaultResolveQuery> {
    (this.coValueClass.prototype as Account).migrate = async function (
      this,
      creationProps,
    ) {
      // @ts-expect-error
      await migration(this, creationProps);
    };

    return this;
  }

  getCoValueClass(): typeof Account {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  /**
   * Adds a default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   */
  resolved<R extends ResolveQuery<AccountSchema<Shape>>>(
    resolveQuery: RefsToResolveStrict<AccountSchema<Shape>, R>,
  ): AccountSchema<Shape, R> {
    const coreSchema: CoreAccountSchema<Shape> = createCoreAccountSchema(
      this.shape,
    );
    const copy = new AccountSchema<Shape, R>(coreSchema, this.coValueClass);
    copy.resolveQuery = resolveQuery as R;
    return copy;
  }
}

export function createCoreAccountSchema<Shape extends BaseAccountShape>(
  shape: Shape,
): CoreAccountSchema<Shape> {
  return {
    ...createCoreCoMapSchema(shape),
    builtin: "Account" as const,
  };
}

export type DefaultProfileShape = {
  name: z.core.$ZodString<string>;
  inbox: z.core.$ZodOptional<z.core.$ZodString>;
  inboxInvite: z.core.$ZodOptional<z.core.$ZodString>;
};

export type CoProfileSchema<
  Shape extends z.core.$ZodLooseShape = DefaultProfileShape,
  CatchAll extends AnyZodOrCoValueSchema | unknown = unknown,
> = CoMapSchema<Shape & DefaultProfileShape, CatchAll, Group>;

// less precise version to avoid circularity issues and allow matching against
export interface CoreAccountSchema<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
> extends Omit<CoreCoMapSchema<Shape>, "builtin"> {
  builtin: "Account";
}

export type AccountInstance<Shape extends z.core.$ZodLooseShape> = {
  readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
} & Account;
