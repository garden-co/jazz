import {
  cojsonInternals,
  type CoValueUniqueness,
  type CojsonInternalTypes,
  type RawCoID,
  type RawCoValue,
} from "cojson";
import { AvailableCoValueCore } from "cojson";
import {
  Account,
  AnonymousJazzAgent,
  CoValueClassOrSchema,
  CoValueLoadingState,
  NotLoadedCoValueState,
  Group,
  Loaded,
  Inaccessible,
  MaybeLoaded,
  OnCreateCallback,
  Settled,
  RefsToResolve,
  RefsToResolveStrict,
  RegisteredSchemas,
  ResolveQuery,
  ResolveQueryStrict,
  Resolved,
  SubscriptionScope,
  TypeSym,
  NotLoaded,
  activeAccountContext,
  coValueClassFromCoValueClassOrSchema,
  inspect,
  LocalValidationMode,
} from "../internal.js";
import type {
  BranchDefinition,
  CoValueErrorState,
} from "../subscribe/types.js";
import { CoValueHeader } from "cojson";
import { JazzError } from "../subscribe/JazzError.js";
import { CoreCoValueSchema } from "../implementation/zodSchema/schemaTypes/CoValueSchema.js";

/** @category Abstract interfaces */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface CoValueClass<
  Value extends CoValue = CoValue,
  Schema extends CoreCoValueSchema = CoreCoValueSchema,
> {
  coValueSchema?: Schema;
  /** @ignore */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  new (...args: any[]): Value;
}

export interface CoValueFromRaw<V extends CoValue> {
  fromRaw(raw: V["$jazz"]["raw"]): V;
}

/** @category Abstract interfaces */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface CoValue {
  /** @category Type Helpers */
  [TypeSym]: string;

  $jazz: {
    /** @category Content */
    readonly id: ID<CoValue>;
    /** @category Content */
    loadingState: typeof CoValueLoadingState.LOADED;
    /** @category Collaboration */
    owner?: Group;
    /** @internal */
    readonly loadedAs: Account | AnonymousJazzAgent;
    /** @category Internals */
    raw: RawCoValue;
    /** @internal */
    _subscriptionScope?: SubscriptionScope<CoValue>;
    isBranched: boolean;
    branchName: string | undefined;
    unstable_merge: () => void;
  };
  /**
   * Whether the CoValue is loaded. Can be used to distinguish between loaded and {@link NotLoaded} CoValues.
   * For more information about the CoValue's loading state, use {@link $jazz.loadingState}.
   */
  $isLoaded: true;

  /** @category Stringifying & Inspection */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(key?: string, seenAbove?: ID<CoValue>[]): any[] | object | string;
  /** @category Stringifying & Inspection */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [inspect](): any;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isCoValue(value: any): value is CoValue {
  return value && value[TypeSym] !== undefined;
}

export function isCoValueClass<V extends CoValue>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  value: any,
): value is CoValueClass<V> & CoValueFromRaw<V> {
  return typeof value === "function" && value.fromRaw !== undefined;
}

/**
 * IDs are unique identifiers for `CoValue`s.
 * Can be used with a type argument to refer to a specific `CoValue` type.
 *
 * @example
 *
 * ```ts
 * type AccountID = ID<Account>;
 * ```
 *
 * @category CoValues
 */
export type ID<T> = string;

const unloadedCoValueStates = new Map<
  NotLoadedCoValueState,
  NotLoaded<CoValue>
>();

export function getUnloadedCoValueWithoutId<T extends CoValue>(
  loadingState: NotLoadedCoValueState,
): NotLoaded<T> {
  const value = unloadedCoValueStates.get(loadingState);
  if (value) {
    return value;
  }
  const newValue = createUnloadedCoValue("", loadingState);
  unloadedCoValueStates.set(loadingState, newValue);
  return newValue;
}

export function createSettledCoValue<T extends CoValue>(
  id: ID<T>,
  loadingState: CoValueErrorState,
): Settled<T> {
  return {
    $jazz: { id, loadingState },
    $isLoaded: false,
  };
}

export function createUnloadedCoValue<T extends CoValue>(
  id: ID<T>,
  loadingState: NotLoadedCoValueState,
): NotLoaded<T> {
  return {
    $jazz: { id, loadingState },
    $isLoaded: false,
  };
}

export function loadCoValueWithoutMe<
  V extends CoValue,
  const R extends RefsToResolve<V> = true,
>(
  cls: CoValueClass<V>,
  id: ID<CoValue>,
  options?: {
    resolve?: RefsToResolveStrict<V, R>;
    loadAs?: Account | AnonymousJazzAgent;
    skipRetry?: boolean;
    unstable_branch?: BranchDefinition;
  },
): Promise<Settled<Resolved<V, R>>> {
  return loadCoValue(cls, id, {
    ...options,
    loadAs: options?.loadAs ?? activeAccountContext.get(),
    unstable_branch: options?.unstable_branch,
  });
}

export function loadCoValue<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  id: ID<CoValue>,
  options: {
    resolve?: RefsToResolveStrict<V, R>;
    loadAs: Account | AnonymousJazzAgent;
    skipRetry?: boolean;
    unstable_branch?: BranchDefinition;
  },
): Promise<Settled<Resolved<V, R>>> {
  return new Promise((resolve) => {
    subscribeToCoValue<V, R>(
      cls,
      id,
      {
        resolve: options.resolve,
        loadAs: options.loadAs,
        syncResolution: true,
        skipRetry: options.skipRetry,
        onError: resolve,
        unstable_branch: options.unstable_branch,
      },
      (value, unsubscribe) => {
        resolve(value);
        unsubscribe();
      },
    );
  });
}

export async function ensureCoValueLoaded<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  existing: V,
  options?:
    | {
        resolve?: RefsToResolveStrict<V, R>;
        unstable_branch?: BranchDefinition;
      }
    | undefined,
): Promise<Resolved<V, R>> {
  const response = await loadCoValue(
    existing.constructor as CoValueClass<V>,
    existing.$jazz.id,
    {
      loadAs: existing.$jazz.loadedAs,
      resolve: options?.resolve,
      unstable_branch: options?.unstable_branch,
    },
  );

  if (!response.$isLoaded) {
    throw new Error("Failed to deeply load CoValue " + existing.$jazz.id);
  }

  return response;
}

type SubscribeListener<V extends CoValue, R extends RefsToResolve<V>> = (
  value: Resolved<V, R>,
  unsubscribe: () => void,
) => void;

export type SubscribeCallback<V> = (value: V, unsubscribe: () => void) => void;

export type SubscribeListenerOptions<
  V extends CoValue,
  R extends RefsToResolve<V>,
> = {
  resolve?: RefsToResolveStrict<V, R>;
  loadAs?: Account | AnonymousJazzAgent;
  onError?: (value: NotLoaded<V>) => void;
  /**
   * @deprecated Use `onError` instead. This callback will be removed in a future version.
   */
  onUnauthorized?: (value: NotLoaded<V>) => void;
  /**
   * @deprecated Use `onError` instead. This callback will be removed in a future version.
   */
  onUnavailable?: (value: NotLoaded<V>) => void;
  unstable_branch?: BranchDefinition;
};

export type SubscribeRestArgs<V extends CoValue, R extends RefsToResolve<V>> =
  | [options: SubscribeListenerOptions<V, R>, listener: SubscribeListener<V, R>]
  | [listener: SubscribeListener<V, R>];

export function parseSubscribeRestArgs<
  V extends CoValue,
  R extends RefsToResolve<V>,
>(
  args: SubscribeRestArgs<V, R>,
): {
  options: SubscribeListenerOptions<V, R>;
  listener: SubscribeListener<V, R>;
} {
  if (args.length === 2) {
    if (
      typeof args[0] === "object" &&
      args[0] &&
      typeof args[1] === "function"
    ) {
      return {
        options: {
          resolve: args[0].resolve,
          loadAs: args[0].loadAs,
          onError: args[0].onError,
          onUnauthorized: args[0].onUnauthorized,
          onUnavailable: args[0].onUnavailable,
          unstable_branch: args[0].unstable_branch,
        },
        listener: args[1],
      };
    } else {
      throw new Error("Invalid arguments");
    }
  } else {
    if (typeof args[0] === "function") {
      return { options: {}, listener: args[0] };
    } else {
      throw new Error("Invalid arguments");
    }
  }
}

export function subscribeToCoValueWithoutMe<
  V extends CoValue,
  const R extends RefsToResolve<V> = true,
>(
  cls: CoValueClass<V>,
  id: ID<CoValue>,
  options: SubscribeListenerOptions<V, R>,
  listener: SubscribeListener<V, R>,
) {
  return subscribeToCoValue(
    cls,
    id,
    {
      ...options,
      loadAs: options.loadAs ?? activeAccountContext.get(),
    },
    listener,
  );
}

export function subscribeToCoValue<
  V extends CoValue,
  const R extends RefsToResolve<V> = true,
>(
  cls: CoValueClass<V>,
  id: ID<CoValue>,
  options: {
    resolve?: RefsToResolveStrict<V, R>;
    loadAs: Account | AnonymousJazzAgent;
    onError?: (value: Inaccessible<V>) => void;
    /**
     * @deprecated Use `onError` instead. This callback will be removed in a future version.
     */
    onUnavailable?: (value: Inaccessible<V>) => void;
    /**
     * @deprecated Use `onError` instead. This callback will be removed in a future version.
     */
    onUnauthorized?: (value: Inaccessible<V>) => void;
    syncResolution?: boolean;
    skipRetry?: boolean;
    unstable_branch?: BranchDefinition;
  },
  listener: SubscribeListener<V, R>,
): () => void {
  const loadAs = options.loadAs ?? activeAccountContext.get();
  const node = "node" in loadAs ? loadAs.node : loadAs.$jazz.localNode;

  const resolve = options.resolve ?? true;

  let unsubscribed = false;

  const rootNode = new SubscriptionScope<V>(
    node,
    resolve,
    id as ID<V>,
    {
      ref: cls,
      optional: false,
    },
    options.skipRetry,
    false,
    options.unstable_branch,
  );

  // Track performance for API subscriptions
  rootNode.trackLoadingPerformance("subscribe");

  const handleUpdate = () => {
    if (unsubscribed) return;

    const value = rootNode.getCurrentValue();

    if (value.$isLoaded) {
      listener(value as Resolved<V, R>, unsubscribe);
      return;
    }

    options.onError?.(value as Inaccessible<V>);

    // Backward compatibility, going to remove this in the next minor release
    switch (value.$jazz.loadingState) {
      case CoValueLoadingState.UNAVAILABLE:
        options.onUnavailable?.(value as Inaccessible<V>);
        break;
      case CoValueLoadingState.UNAUTHORIZED:
        options.onUnauthorized?.(value as Inaccessible<V>);
        break;
    }
  };

  let shouldDefer = !options.syncResolution;

  rootNode.setListener(() => {
    if (shouldDefer) {
      shouldDefer = false;
      Promise.resolve().then(() => {
        handleUpdate();
      });
    } else {
      handleUpdate();
    }
  });

  function unsubscribe() {
    unsubscribed = true;
    rootNode.destroy();
  }

  return unsubscribe;
}

export function subscribeToExistingCoValue<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  existing: V,
  options:
    | {
        resolve?: RefsToResolveStrict<V, R>;
        onError?: (value: NotLoaded<V>) => void;
        /**
         * @deprecated Use `onError` instead. This callback will be removed in a future version.
         */
        onUnavailable?: (value: NotLoaded<V>) => void;
        /**
         * @deprecated Use `onError` instead. This callback will be removed in a future version.
         */
        onUnauthorized?: (value: NotLoaded<V>) => void;
        unstable_branch?: BranchDefinition;
      }
    | undefined,
  listener: SubscribeListener<V, R>,
): () => void {
  return subscribeToCoValue(
    existing.constructor as CoValueClass<V>,
    existing.$jazz.id,
    {
      loadAs: existing.$jazz.loadedAs,
      resolve: options?.resolve,
      onError: options?.onError,
      onUnavailable: options?.onUnavailable,
      onUnauthorized: options?.onUnauthorized,
      unstable_branch: options?.unstable_branch,
    },
    listener,
  );
}

export function isAccountInstance(instance: unknown): instance is Account {
  if (typeof instance !== "object" || instance === null) {
    return false;
  }

  return TypeSym in instance && instance[TypeSym] === "Account";
}

export function isAnonymousAgentInstance(
  instance: unknown,
): instance is AnonymousJazzAgent {
  if (typeof instance !== "object" || instance === null) {
    return false;
  }

  return TypeSym in instance && instance[TypeSym] === "Anonymous";
}

export type CoValueCreateOptions<
  MoreOptions extends object = {},
  Owner extends Group | Account = Group,
> =
  | undefined
  | Owner
  | ((
      | {
          owner: Owner;
          // we want to have explicit owner if unique is provided
          unique: CoValueUniqueness["uniqueness"];
          validation?: LocalValidationMode;
        }
      | {
          owner?: Owner;
          unique?: undefined;
          validation?: LocalValidationMode;
        }
    ) &
      MoreOptions);

export type CoValueCreateOptionsInternal = CoValueCreateOptions<
  {
    onCreate?: OnCreateCallback;
    firstComesWins?: boolean;
    restrictDeletion?: boolean;
  },
  Account | Group
>;

export function parseCoValueCreateOptions(
  options: CoValueCreateOptionsInternal,
): {
  owner: Group;
  uniqueness?: CoValueUniqueness;
  firstComesWins: boolean;
  restrictDeletion?: boolean;
} {
  const onCreate =
    options && "onCreate" in options ? options.onCreate : undefined;
  const Group = RegisteredSchemas["Group"];
  if (!options) {
    const owner = Group.create();
    onCreate?.(owner);
    return {
      owner,
      uniqueness: undefined,
      firstComesWins: false,
      restrictDeletion: undefined,
    };
  }

  if (TypeSym in options) {
    if (options[TypeSym] === "Account") {
      const owner = accountOrGroupToGroup(options);
      onCreate?.(owner);
      return {
        owner,
        uniqueness: undefined,
        firstComesWins: false,
        restrictDeletion: undefined,
      };
    } else if (options[TypeSym] === "Group") {
      onCreate?.(options);
      return {
        owner: options,
        uniqueness: undefined,
        firstComesWins: false,
        restrictDeletion: undefined,
      };
    }
  }

  const firstComesWins = options.firstComesWins ?? false;

  const uniqueness = options.unique
    ? { uniqueness: options.unique }
    : undefined;

  const owner = options.owner
    ? accountOrGroupToGroup(options.owner)
    : Group.create();

  onCreate?.(owner);

  const opts = {
    owner,
    uniqueness,
    firstComesWins,
    restrictDeletion: options.restrictDeletion,
  };
  return opts;
}

export function accountOrGroupToGroup(accountOrGroup: Account | Group): Group {
  if (accountOrGroup[TypeSym] === "Group") {
    return accountOrGroup;
  }
  return RegisteredSchemas["Group"].fromRaw(accountOrGroup.$jazz.raw);
}

export function parseGroupCreateOptions(
  options:
    | {
        owner?: Account;
        name?: string;
      }
    | Account
    | undefined,
): { owner: Account; name?: string } {
  if (!options) {
    return { owner: activeAccountContext.get() };
  }

  if (TypeSym in options && isAccountInstance(options)) {
    return { owner: options };
  }

  const owner = options.owner ?? activeAccountContext.get();
  return options.name !== undefined ? { owner, name: options.name } : { owner };
}

export function getIdFromHeader(
  header: CoValueHeader,
  loadAs?: Account | AnonymousJazzAgent | Group,
) {
  loadAs ||= activeAccountContext.get();

  const node =
    loadAs[TypeSym] === "Anonymous" ? loadAs.node : loadAs.$jazz.localNode;

  return cojsonInternals.idforHeader(header, node.crypto);
}

/**
 * Mapping from CoValue TypeSym to the CoValueHeaderType.
 */
const coValueTypeSymToHeaderType: Record<string, CoValueHeaderType | null> = {
  CoMap: "comap",
  Group: null,
  Account: null,
  CoList: "colist",
  CoStream: "costream",
  CoPlainText: "coplaintext",
  BinaryCoStream: null,
};

export async function unstable_loadUnique<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S>,
>(
  schema: S,
  options: {
    unique: CoValueUniqueness["uniqueness"];
    onCreateWhenMissing?: () => void;
    onUpdateWhenFound?: (value: Loaded<S, R>) => void;
    owner: Account | Group;
    resolve?: ResolveQueryStrict<S, R>;
  },
): Promise<MaybeLoaded<Loaded<S, R>>> {
  const cls = coValueClassFromCoValueClassOrSchema(schema);
  const headerType = getUniqueHeaderType(schema);

  // @ts-expect-error the CoValue class is too generic for TS to infer its instances are CoValues
  return internalLoadUnique(cls, {
    type: headerType,
    unique: options.unique,
    onCreateWhenMissing: options.onCreateWhenMissing,
    onUpdateWhenFound: options.onUpdateWhenFound,
    owner: options.owner,
    resolve: options.resolve,
  }) as unknown as MaybeLoaded<Loaded<S, R>>;
}

export type CoValueHeaderType = "comap" | "colist" | "costream" | "coplaintext";

/**
 * Get the CoValueHeaderType from a CoValue class.
 * Throws for unsupported types (Group, Account, BinaryCoStream).
 */
export function getUniqueHeaderType(
  schema: CoValueClassOrSchema,
): CoValueHeaderType {
  const cls = coValueClassFromCoValueClassOrSchema(schema);
  const typeSym = cls.prototype[TypeSym] as string | undefined;
  if (!typeSym) {
    throw new Error(`Cannot determine CoValue type from class: ${cls.name}`);
  }

  const headerType = coValueTypeSymToHeaderType[typeSym];
  if (!headerType) {
    throw new Error(
      `Unsupported CoValue type for unique headers: ${typeSym}. ` +
        `Only CoMap, CoList, CoFeed (CoStream), and CoPlainText are supported.`,
    );
  }

  return headerType;
}

/**
 * Generate a unique header for a CoValue class.
 * Throws for unsupported types (Group, Account, BinaryCoStream).
 */
export function getUniqueHeader(
  type: CoValueHeaderType,
  unique: CoValueUniqueness["uniqueness"],
  ownerID: ID<Account> | ID<Group>,
): CoValueHeader {
  return {
    type,
    ruleset: {
      type: "ownedByGroup" as const,
      group: ownerID as RawCoID,
    },
    meta: null,
    uniqueness: unique,
  };
}

export async function internalLoadUnique<
  V extends CoValue,
  R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  options: {
    unique: CoValueUniqueness["uniqueness"];
    type: CoValueHeaderType;
    onCreateWhenMissing?: () => void;
    onUpdateWhenFound?: (value: Resolved<V, R>) => void;
    owner: Account | Group;
    resolve?: RefsToResolveStrict<V, R>;
  },
): Promise<Settled<Resolved<V, R>>> {
  const loadAs = options.owner.$jazz.loadedAs;

  const node =
    loadAs[TypeSym] === "Anonymous" ? loadAs.node : loadAs.$jazz.localNode;

  const header = getUniqueHeader(
    options.type,
    options.unique,
    options.owner.$jazz.id,
  );
  const id = cojsonInternals.idforHeader(header, node.crypto);

  // We first try to load the unique value without using resolve and without
  // retrying failures
  // This way when we want to upsert we are sure that, if the load failed
  // it failed because the unique value was missing
  const maybeLoadedCoValue = await loadCoValueWithoutMe(cls, id, {
    skipRetry: true,
    loadAs,
  });

  const isAvailable = node.getCoValue(id).hasVerifiedContent();

  // if load returns unavailable, we check the state in localNode
  // to ward against race conditions that would happen when
  // running the same upsert unique concurrently
  if (options.onCreateWhenMissing && !isAvailable) {
    if (!loadAs.canWrite(options.owner)) {
      return createSettledCoValue<Resolved<V, R>>(
        id,
        CoValueLoadingState.UNAUTHORIZED,
      );
    }

    options.onCreateWhenMissing();

    return loadCoValueWithoutMe(cls, id, {
      loadAs,
      resolve: options.resolve,
    });
  }

  if (!isAvailable) {
    // @ts-expect-error the resolve query of the loaded values is not necessarily the same,
    // but we're only returning not-loaded values
    return maybeLoadedCoValue;
  }

  if (options.onUpdateWhenFound) {
    // we deeply load the value, retrying any failures
    const loaded = await loadCoValueWithoutMe(cls, id, {
      loadAs,
      resolve: options.resolve,
    });

    if (loaded.$isLoaded && loadAs.canWrite(options.owner)) {
      // we don't return the update result because
      // we want to run another load to backfill any possible partially loaded
      // values that have been set in the update
      options.onUpdateWhenFound(loaded);
    } else {
      return loaded;
    }
  }

  return loadCoValueWithoutMe(cls, id, {
    loadAs,
    resolve: options.resolve,
  });
}

/**
 * Deeply export a CoValue to a content piece.
 *
 * @param cls - The class of the CoValue to export.
 * @param id - The ID of the CoValue to export.
 * @param options - The options for the export.
 * @returns The content pieces that were exported.
 *
 * @example
 * ```ts
 * const Address = co.map({
 *   street: z.string(),
 *   city: z.string(),
 * });
 *
 * const Person = co.map({
 *   name: z.string(),
 *   address: Address,
 * });
 *
 * const group = Group.create();
 * const address = Address.create(
 *   { street: "123 Main St", city: "New York" },
 *   group,
 * );
 * const person = Person.create({ name: "John", address }, group);
 * group.addMember("everyone", "reader");
 *
 * // Export with nested references resolved, values can be serialized to JSON
 * const exportedWithResolve = await exportCoValue(Person, person.id, {
 *   resolve: { address: true },
 * });
 *
 * // In another client or session
 * // Load the exported content pieces into the node, they will be persisted
 * importContentPieces(exportedWithResolve);
 *
 * // Now the person can be loaded from the node, even offline
 * const person = await loadCoValue(Person, person.id, {
 *   resolve: { address: true },
 * });
 * ```
 */
export async function exportCoValue<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S>,
>(
  cls: S,
  id: ID<CoValue>,
  options: {
    resolve?: ResolveQueryStrict<S, R>;
    loadAs: Account | AnonymousJazzAgent;
    skipRetry?: boolean;
    bestEffortResolution?: boolean;
    unstable_branch?: BranchDefinition;
  },
) {
  const loadAs = options.loadAs ?? activeAccountContext.get();
  const node = "node" in loadAs ? loadAs.node : loadAs.$jazz.localNode;

  const resolve = options.resolve ?? true;

  const rootNode = new SubscriptionScope<CoValue>(
    node,
    resolve as any,
    id,
    {
      ref: coValueClassFromCoValueClassOrSchema(cls),
      optional: false,
    },
    options.skipRetry,
    options.bestEffortResolution,
    options.unstable_branch,
  );

  try {
    await rootNode.getPromise();
    rootNode.destroy();
  } catch (error) {
    rootNode.destroy();
    return null;
  }

  const valuesExported = new Set<string>();
  const contentPieces: CojsonInternalTypes.NewContentMessage[] = [];

  loadContentPiecesFromSubscription(rootNode, valuesExported, contentPieces);

  return contentPieces;
}

export function exportCoValueFromSubscription<V>(
  subscription: SubscriptionScope<CoValue>,
): ExportedCoValue<V> {
  const valuesExported = new Set<string>();
  const contentPieces: CojsonInternalTypes.NewContentMessage[] = [];

  loadContentPiecesFromSubscription(
    subscription,
    valuesExported,
    contentPieces,
  );

  return {
    id: subscription.id as ExportedID<V>,
    contentPieces,
  };
}

export type ExportedID<V> = string & { _exportedID: V };

export type ExportedCoValue<V> = {
  id: ExportedID<V>; // This is used for branding the export type
  contentPieces: CojsonInternalTypes.NewContentMessage[];
};

function loadContentPiecesFromSubscription(
  subscription: SubscriptionScope<any>,
  valuesExported: Set<string>,
  contentPieces: CojsonInternalTypes.NewContentMessage[],
) {
  if (valuesExported.has(subscription.id)) {
    return;
  }

  valuesExported.add(subscription.id);

  const currentValue = subscription.getCurrentValue();

  if (currentValue.$isLoaded) {
    const core = currentValue.$jazz.raw.core as AvailableCoValueCore;
    loadContentPiecesFromCoValue(core, valuesExported, contentPieces);
  }

  for (const child of subscription.childNodes.values()) {
    loadContentPiecesFromSubscription(child, valuesExported, contentPieces);
  }
}

function loadContentPiecesFromCoValue(
  core: AvailableCoValueCore,
  valuesExported: Set<string>,
  contentPieces: CojsonInternalTypes.NewContentMessage[],
) {
  for (const dependency of core.getDependedOnCoValues()) {
    if (valuesExported.has(dependency)) {
      continue;
    }

    const depCoValue = core.node.getCoValue(dependency);

    if (depCoValue.isAvailable()) {
      valuesExported.add(dependency);
      loadContentPiecesFromCoValue(depCoValue, valuesExported, contentPieces);
    }
  }

  const pieces = core.newContentSince() ?? [];

  for (const piece of pieces) {
    contentPieces.push(piece);
  }
}

/**
 * Import content pieces into the node.
 *
 * @param contentPieces - The content pieces to import.
 * @param loadAs - The account to load the content pieces as.
 */
export function importContentPieces(
  contentPieces: CojsonInternalTypes.NewContentMessage[],
  loadAs?: Account | AnonymousJazzAgent,
) {
  const account = loadAs ?? Account.getMe();
  const node = "node" in account ? account.node : account.$jazz.localNode;

  for (const piece of contentPieces) {
    node.syncManager.handleNewContent(piece, "import");
  }
}

export function unstable_mergeBranch(
  subscriptionScope: SubscriptionScope<CoValue>,
) {
  if (!subscriptionScope.unstable_branch) {
    return;
  }

  function handleMerge(subscriptionNode: SubscriptionScope<CoValue>) {
    if (subscriptionNode.value.type === CoValueLoadingState.LOADED) {
      subscriptionNode.value.value.$jazz.raw.core.mergeBranch();
    }

    for (const childNode of subscriptionNode.childNodes.values()) {
      handleMerge(childNode);
    }
  }

  handleMerge(subscriptionScope);
}

export async function unstable_mergeBranchWithResolve<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S>,
>(
  cls: S,
  id: ID<CoValue>,
  options: {
    resolve?: ResolveQueryStrict<S, R>;
    loadAs?: Account | AnonymousJazzAgent;
    branch: BranchDefinition;
  },
) {
  const loadAs = options.loadAs ?? activeAccountContext.get();
  const node = "node" in loadAs ? loadAs.node : loadAs.$jazz.localNode;

  const resolve = options.resolve ?? true;

  const rootNode = new SubscriptionScope<CoValue>(
    node,
    resolve as any,
    id,
    {
      ref: coValueClassFromCoValueClassOrSchema(cls),
      optional: false,
    },
    false,
    false,
    options.branch,
  );

  try {
    await rootNode.getPromise();
    rootNode.destroy();
  } catch (error) {
    rootNode.destroy();
    throw error;
  }

  unstable_mergeBranch(rootNode);
}

/**
 * Permanently delete a group of coValues
 *
 * This operation is irreversible and will permanently delete the coValues from the local machine and the sync servers.
 *
 */
export async function deleteCoValues<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S>,
>(
  cls: S,
  id: ID<CoValue>,
  options: {
    resolve?: ResolveQueryStrict<S, R>;
    loadAs?: Account | AnonymousJazzAgent;
  } = {},
) {
  const loadAs = options.loadAs ?? activeAccountContext.get();
  const node = "node" in loadAs ? loadAs.node : loadAs.$jazz.localNode;

  const resolve = options.resolve ?? true;

  const rootNode = new SubscriptionScope<CoValue>(
    node,
    resolve as any,
    id,
    {
      ref: coValueClassFromCoValueClassOrSchema(cls),
      optional: false,
    },
    false,
    false,
    undefined,
  );

  try {
    await rootNode.getPromise();
    rootNode.destroy();
  } catch (error) {
    rootNode.destroy();
    throw error;
  }

  // We validate permissions to fail early if one of the loaded coValues is not deletable
  const errors = validateDeletePermissions(rootNode);

  if (errors.length > 0) {
    const combined = new JazzError(
      id,
      CoValueLoadingState.DELETED,
      errors.flatMap((e) => e.issues),
    );
    throw new Error(combined.toString());
  }

  const deletedValues = deleteCoValueFromSubscription(rootNode);

  await Promise.allSettled(
    Array.from(deletedValues, (value) => value.waitForSync()),
  );
}

function validateDeletePermissions(
  rootNode: SubscriptionScope<CoValue>,
  path: string[] = [],
  errors: JazzError[] = [],
): JazzError[] {
  for (const [key, childNode] of rootNode.childNodes.entries()) {
    validateDeletePermissions(childNode, [...path, key], errors);
  }

  if (rootNode.value.type !== CoValueLoadingState.LOADED) {
    return errors;
  }

  const core = rootNode.value.value.$jazz.raw.core;

  // Account and Group coValues are not deletable, we skip them to make it easier to delete all coValues owned by an account
  if (core.isGroupOrAccount()) {
    return errors;
  }

  const result = core.validateDeletePermissions();
  if (!result.ok) {
    errors.push(
      new JazzError(core.id, CoValueLoadingState.DELETED, [
        {
          code: "deleteError",
          message: `Jazz Delete Error: ${result.message}`,
          params: {},
          path,
        },
      ]),
    );
  }

  return errors;
}

function deleteCoValueFromSubscription(
  rootNode: SubscriptionScope<CoValue>,
  values = new Set<AvailableCoValueCore>(),
) {
  for (const childNode of rootNode.childNodes.values()) {
    deleteCoValueFromSubscription(childNode, values);
  }

  if (rootNode.value.type !== CoValueLoadingState.LOADED) {
    return values;
  }

  const core = rootNode.value.value.$jazz.raw.core;

  // Account and Group coValues are not deletable, we skip them to make it easier to delete all coValues owned by an account
  if (core.isGroupOrAccount()) {
    return values;
  }

  try {
    core.deleteCoValue();
    values.add(core);
  } catch (error) {
    console.error("Failed to delete coValue", error);
  }

  return values;
}
