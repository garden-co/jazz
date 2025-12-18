import {
  Account,
  AccountCreationProps,
  BranchDefinition,
  CoMapSchemaDefinition,
  coOptionalDefiner,
  Group,
  Settled,
  Simplify,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  unstable_mergeBranchWithResolve,
  Loaded,
  ResolveQuery,
  schemaFieldToFieldDescriptor,
  SchemaField,
  CoMapFieldSchema,
  ItemsMarker,
  RegisteredSchemas,
  CoMap,
  asConstructable,
  ResolveQueryStrict,
  activeAccountContext,
} from "../../../internal.js";
import {
  cojsonInternals,
  CryptoProvider,
  LocalNode,
  Peer,
  RawAccount,
} from "cojson";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema } from "../zodSchema.js";
import {
  CoMapSchema,
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
  DefaultResolveQuery extends ResolveQuery<CoreAccountSchema<Shape>> = true,
> implements CoreAccountSchema<Shape>
{
  collaborative = true as const;
  builtin = "Account" as const;
  shape: Shape;
  getDefinition: () => CoMapSchemaDefinition;

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
    this.getDefinition = coreSchema.getDefinition;
  }

  async create(options: {
    creationProps: { name: string };
    initialAgentSecret?: AgentSecret;
    peers?: Peer[];
    crypto: CryptoProvider;
  }): Promise<Loaded<CoreAccountSchema<Shape>>> {
    const def = this.getDefinition();
    const fields: CoMapFieldSchema = {};
    for (const [key, value] of Object.entries(def.shape)) {
      fields[key] = schemaFieldToFieldDescriptor(value as SchemaField);
    }

    const { node } = await LocalNode.withNewlyCreatedAccount({
      ...options,
      migration: async (rawAccount, _node, creationProps) => {
        const account = new this.coValueClass(fields, rawAccount, this);

        await account.applyMigration?.(creationProps);
      },
    });

    return this.fromNode(node);
  }

  load<
    const R extends ResolveQuery<
      CoreAccountSchema<Shape>
    > = DefaultResolveQuery,
  >(
    id: string,
    options?: {
      loadAs?: Loaded<CoreAccountSchema> | AnonymousJazzAgent;
      resolve?: ResolveQueryStrict<CoreAccountSchema<Shape>, R>;
    },
  ): Promise<Settled<CoreAccountSchema<Shape>, R>> {
    return loadCoValueWithoutMe<CoreAccountSchema<Shape>, R>(
      this,
      id,
      withSchemaResolveQuery(this, options),
    );
  }

  fromRaw(raw: RawAccount): Loaded<CoreAccountSchema<Shape>> {
    const def = this.getDefinition();
    const fields: CoMapFieldSchema = {};
    for (const [key, value] of Object.entries(def.shape)) {
      fields[key] = schemaFieldToFieldDescriptor(value as SchemaField);
    }
    return new this.coValueClass(fields, raw, this) as Loaded<
      CoreAccountSchema<Shape>
    >;
  }

  fromNode(node: LocalNode): Loaded<CoreAccountSchema<Shape>> {
    const def = this.getDefinition();
    const fields: CoMapFieldSchema = {};
    for (const [key, value] of Object.entries(def.shape)) {
      fields[key] = schemaFieldToFieldDescriptor(value as SchemaField);
    }
    return new this.coValueClass(
      fields,
      node.expectCurrentAccount("jazz-tools/Account.fromNode"),
      this,
    ) as Loaded<CoreAccountSchema<Shape>>;
  }

  // Create an account via worker, useful to generate controlled accounts from the server
  async createAs(
    worker: Loaded<CoreAccountSchema>,
    options: {
      creationProps: { name: string };
      onCreate?: (
        account: Loaded<CoreAccountSchema<Shape>>,
        worker: Loaded<CoreAccountSchema>,
      ) => Promise<void>;
    },
  ): Promise<{
    credentials: {
      accountID: string;
      accountSecret: AgentSecret;
    };
    account: Loaded<CoreAccountSchema<Shape>, DefaultResolveQuery>;
  }> {
    const crypto = worker.$jazz.localNode.crypto;

    const connectedPeers = cojsonInternals.connectedPeers(
      "creatingAccount",
      crypto.uniquenessForHeader(), // Use a unique id for the client peer, so we don't have clashes in the worker node
      { peer1role: "server", peer2role: "client" },
    );

    worker.$jazz.localNode.syncManager.addPeer(connectedPeers[1]);

    const account = await (this as unknown as AccountSchema).create({
      creationProps: options.creationProps,
      crypto,
      peers: [connectedPeers[0]],
    });

    const credentials = {
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
    };

    // Load the worker inside the account node
    const loadedWorker = await asConstructable(
      RegisteredSchemas["Account"],
    ).load(worker.$jazz.id, {
      loadAs: account,
    });

    // This should never happen, because the two accounts are linked
    if (!loadedWorker.$isLoaded)
      throw new Error("Unable to load the worker account");

    // The onCreate hook can be helpful to define inline logic, such as querying the DB
    if (options.onCreate)
      await options.onCreate(
        account as Loaded<CoreAccountSchema<Shape>>,
        loadedWorker as Loaded<CoreAccountSchema>,
      );

    await account.$jazz.waitForAllCoValuesSync();

    const createdAccount = await (this as AccountSchema<Shape>).load(
      account.$jazz.id,
      {
        loadAs: worker,
      },
    );

    if (!createdAccount.$isLoaded)
      throw new Error("Unable to load the created account");

    // Close the account node, to avoid leaking memory
    account.$jazz.localNode.gracefulShutdown();

    return {
      credentials,
      account: createdAccount as Loaded<
        CoreAccountSchema<Shape>,
        DefaultResolveQuery
      >,
    };
  }

  unstable_merge<
    R extends ResolveQuery<CoreAccountSchema<Shape>> = DefaultResolveQuery,
  >(
    id: string,
    options: {
      loadAs?: Loaded<CoreAccountSchema> | AnonymousJazzAgent;
      resolve?: ResolveQueryStrict<CoreAccountSchema<Shape>, R>;
      branch: BranchDefinition;
    },
  ): Promise<void> {
    return unstable_mergeBranchWithResolve(
      this as CoreAccountSchema<Shape>,
      id,
      withSchemaResolveQuery(this, options),
    );
  }

  subscribe<
    const R extends ResolveQuery<
      CoreAccountSchema<Shape>
    > = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<CoreAccountSchema<Shape>, R>,
    listener: (
      value: Loaded<CoreAccountSchema<Shape>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void {
    return subscribeToCoValueWithoutMe(
      this as CoreAccountSchema<Shape>,
      id,
      withSchemaResolveQuery(this, options),
      listener,
    );
  }

  getMe(): Loaded<this, true> {
    return activeAccountContext.get();
  }

  withMigration(
    migration: (
      account: Loaded<CoreAccountSchema<Shape>>,
      creationProps?: { name: string },
    ) => void,
  ): AccountSchema<Shape, DefaultResolveQuery> {
    (this.coValueClass.prototype as Account<CoreAccountSchema<Shape>>).migrate =
      async function (this, creationProps) {
        await migration(this, creationProps);
      };

    return this;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  /**
   * Adds a default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   */
  resolved<R extends ResolveQuery<AccountSchema<Shape>>>(
    resolveQuery: ResolveQueryStrict<AccountSchema<Shape>, R>,
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
> = CoMapSchema<Shape & DefaultProfileShape, CatchAll>;

// less precise version to avoid circularity issues and allow matching against
export interface CoreAccountSchema<
  Shape extends BaseAccountShape = BaseAccountShape,
> extends Omit<CoreCoMapSchema<Shape>, "builtin"> {
  builtin: "Account";
}

RegisteredSchemas["Account"] = new AccountSchema(
  createCoreAccountSchema({
    profile: new CoMapSchema(
      createCoreCoMapSchema({
        name: z.string(),
        inbox: z.optional(z.string()),
        inboxInvite: z.optional(z.string()),
      }),
      CoMap,
    ),
    root: new CoMapSchema(createCoreCoMapSchema({}), CoMap),
  }),
  Account,
);
