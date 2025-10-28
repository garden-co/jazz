import {
  cojsonInternals,
  type CoValueUniqueness,
  type CojsonInternalTypes,
  type RawCoValue,
} from "cojson";
import { AvailableCoValueCore } from "cojson/dist/coValueCore/coValueCore.js";
import {
  Account,
  AnonymousJazzAgent,
  CoValueClassOrSchema,
  CoValueLoadingState,
  NotLoadedCoValueState,
  type Group,
  Loaded,
  MaybeLoaded,
  RefsToResolve,
  RefsToResolveStrict,
  RegisteredSchemas,
  ResolveQuery,
  ResolveQueryStrict,
  Resolved,
  SubscriptionScope,
  type SubscriptionValue,
  TypeSym,
  NotLoaded,
  activeAccountContext,
  coValueClassFromCoValueClassOrSchema,
  getSubscriptionScope,
  inspect,
} from "../internal.js";
import type { BranchDefinition } from "../subscribe/types.js";
import { CoValueHeader } from "cojson/dist/coValueCore/verifiedState.js";

/** @category Abstract interfaces */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface CoValueClass<Value extends CoValue = CoValue> {
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
  const R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  id: ID<CoValue>,
  options?: {
    resolve?: RefsToResolveStrict<V, R>;
    loadAs?: Account | AnonymousJazzAgent;
    skipRetry?: boolean;
    unstable_branch?: BranchDefinition;
  },
): Promise<MaybeLoaded<Resolved<V, R>>> {
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
): Promise<MaybeLoaded<Resolved<V, R>>> {
  return new Promise((resolve) => {
    subscribeToCoValue<V, R>(
      cls,
      id,
      {
        resolve: options.resolve,
        loadAs: options.loadAs,
        syncResolution: true,
        skipRetry: options.skipRetry,
        onUnavailable: () => {
          resolve(createUnloadedCoValue(id, CoValueLoadingState.UNAVAILABLE));
        },
        onUnauthorized: () => {
          resolve(createUnloadedCoValue(id, CoValueLoadingState.UNAUTHORIZED));
        },
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

export type SubscribeListenerOptions<
  V extends CoValue,
  R extends RefsToResolve<V>,
> = {
  resolve?: RefsToResolveStrict<V, R>;
  loadAs?: Account | AnonymousJazzAgent;
  onUnauthorized?: () => void;
  onUnavailable?: () => void;
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
  const R extends RefsToResolve<V>,
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
  const R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  id: ID<CoValue>,
  options: {
    resolve?: RefsToResolveStrict<V, R>;
    loadAs: Account | AnonymousJazzAgent;
    onUnavailable?: () => void;
    onUnauthorized?: () => void;
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

  const handleUpdate = (value: SubscriptionValue<V, any>) => {
    if (unsubscribed) return;

    if (value.type === CoValueLoadingState.UNAVAILABLE) {
      options.onUnavailable?.();

      // Don't log unavailable errors when `loadUnique` or `upsertUnique` are used
      if (!options.skipRetry) {
        console.error(value.toString());
      }
    } else if (value.type === CoValueLoadingState.UNAUTHORIZED) {
      options.onUnauthorized?.();

      console.error(value.toString());
    } else if (value.type === CoValueLoadingState.LOADED) {
      listener(value.value as Resolved<V, R>, unsubscribe);
    }
  };

  let shouldDefer = !options.syncResolution;

  rootNode.setListener((value) => {
    if (shouldDefer) {
      shouldDefer = false;
      Promise.resolve().then(() => {
        handleUpdate(value);
      });
    } else {
      handleUpdate(value);
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
        onUnavailable?: () => void;
        onUnauthorized?: () => void;
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

export function parseCoValueCreateOptions(
  options:
    | {
        owner?: Account | Group;
        unique?: CoValueUniqueness["uniqueness"];
      }
    | Account
    | Group
    | undefined,
): {
  owner: Group;
  uniqueness?: CoValueUniqueness;
} {
  const Group = RegisteredSchemas["Group"];

  if (!options) {
    return { owner: Group.create(), uniqueness: undefined };
  }

  if (TypeSym in options) {
    if (options[TypeSym] === "Account") {
      return { owner: accountOrGroupToGroup(options), uniqueness: undefined };
    } else if (options[TypeSym] === "Group") {
      return { owner: options, uniqueness: undefined };
    }
  }

  const uniqueness = options.unique
    ? { uniqueness: options.unique }
    : undefined;

  const opts = {
    owner: options.owner
      ? accountOrGroupToGroup(options.owner)
      : Group.create(),
    uniqueness,
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
      }
    | Account
    | undefined,
) {
  if (!options) {
    return { owner: activeAccountContext.get() };
  }

  return TypeSym in options && isAccountInstance(options)
    ? { owner: options }
    : { owner: options.owner ?? activeAccountContext.get() };
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

  if (
    !("_getUniqueHeader" in cls) ||
    typeof cls._getUniqueHeader !== "function"
  ) {
    throw new Error("CoValue class does not support unique headers");
  }

  const header = cls._getUniqueHeader(options.unique, options.owner.$jazz.id);

  // @ts-expect-error the CoValue class is too generic for TS to infer its instances are CoValues
  return internalLoadUnique(cls, {
    header,
    onCreateWhenMissing: options.onCreateWhenMissing,
    onUpdateWhenFound: options.onUpdateWhenFound,
    owner: options.owner,
    resolve: options.resolve,
  }) as unknown as MaybeLoaded<Loaded<S, R>>;
}

export async function internalLoadUnique<
  V extends CoValue,
  R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  options: {
    header: CoValueHeader;
    onCreateWhenMissing?: () => void;
    onUpdateWhenFound?: (value: Resolved<V, R>) => void;
    owner: Account | Group;
    resolve?: RefsToResolveStrict<V, R>;
  },
): Promise<MaybeLoaded<Resolved<V, R>>> {
  const loadAs = options.owner.$jazz.loadedAs;

  const node =
    loadAs[TypeSym] === "Anonymous" ? loadAs.node : loadAs.$jazz.localNode;

  const id = cojsonInternals.idforHeader(options.header, node.crypto);

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

    if (loaded.$isLoaded) {
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

  const value = await new Promise<Loaded<S, R> | null>((resolve) => {
    rootNode.setListener((value) => {
      if (value.type === CoValueLoadingState.UNAVAILABLE) {
        resolve(null);
        console.error(value.toString());
      } else if (value.type === CoValueLoadingState.UNAUTHORIZED) {
        resolve(null);
        console.error(value.toString());
      } else if (value.type === CoValueLoadingState.LOADED) {
        resolve(value.value as Loaded<S, R>);
      }

      rootNode.destroy();
    });
  });

  if (!value) {
    return null;
  }

  const valuesExported = new Set<string>();
  const contentPieces: CojsonInternalTypes.NewContentMessage[] = [];

  loadContentPiecesFromSubscription(rootNode, valuesExported, contentPieces);

  return contentPieces;
}

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

  if (typeof currentValue !== "string") {
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

  const pieces = core.verified.newContentSince(undefined) ?? [];

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
    loadAs: Account | AnonymousJazzAgent;
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

  await new Promise<void>((resolve, reject) => {
    rootNode.setListener((value) => {
      if (value.type === CoValueLoadingState.UNAVAILABLE) {
        reject(new Error("Unable to load the branch. " + value.toString()));
      } else if (value.type === CoValueLoadingState.UNAUTHORIZED) {
        reject(new Error("Unable to load the branch. " + value.toString()));
      } else if (value.type === CoValueLoadingState.LOADED) {
        resolve();
      }

      rootNode.destroy();
    });
  });

  unstable_mergeBranch(rootNode);
}
