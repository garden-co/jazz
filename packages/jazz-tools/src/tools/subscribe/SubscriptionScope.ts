import type { LocalNode, RawCoValue } from "cojson";
import {
  CoFeed,
  CoList,
  CoMap,
  type CoValue,
  type ID,
  MaybeLoaded,
  type RefEncoded,
  type RefsToResolve,
  TypeSym,
  instantiateRefEncodedFromRaw,
  isRefEncoded,
} from "../internal.js";
import { applyCoValueMigrations } from "../lib/migration.js";
import { CoValueCoreSubscription } from "./CoValueCoreSubscription.js";
import { JazzError, type JazzErrorIssue } from "./JazzError.js";
import type {
  BranchDefinition,
  SubscriptionValue,
  SubscriptionValueLoading,
} from "./types.js";
import { CoValueLoadingState, NotLoadedCoValueState } from "./types.js";
import { createCoValue, myRoleForRawValue } from "./utils.js";

export class SubscriptionScope<D extends CoValue> {
  childNodes = new Map<string, SubscriptionScope<CoValue>>();
  childValues: Map<string, SubscriptionValue<any, any>> = new Map<
    string,
    SubscriptionValue<D, any>
  >();
  /**
   * Explicitly-loaded child ids that are unloaded
   */
  pendingLoadedChildren: Set<string> = new Set();
  /**
   * Autoloaded child ids that are unloaded
   */
  pendingAutoloadedChildren: Set<string> = new Set();
  value: SubscriptionValue<D, any> | SubscriptionValueLoading;
  childErrors: Map<string, JazzError> = new Map();
  validationErrors: Map<string, JazzError> = new Map();
  errorFromChildren: JazzError | undefined;
  subscription: CoValueCoreSubscription;
  dirty = false;
  resolve: RefsToResolve<any>;
  idsSubscribed = new Set<string>();
  autoloaded = new Set<string>();
  autoloadedKeys = new Set<string>();
  skipInvalidKeys = new Set<string>();
  totalValidTransactions = 0;
  migrated = false;
  migrating = false;
  closed = false;

  silenceUpdates = false;

  constructor(
    public node: LocalNode,
    resolve: RefsToResolve<D>,
    public id: ID<D>,
    public schema: RefEncoded<D>,
    public skipRetry = false,
    public bestEffortResolution = false,
    public unstable_branch?: BranchDefinition,
  ) {
    this.resolve = resolve;
    this.value = { type: CoValueLoadingState.LOADING, id };

    let lastUpdate:
      | RawCoValue
      | typeof CoValueLoadingState.UNAVAILABLE
      | undefined;
    this.subscription = new CoValueCoreSubscription(
      node,
      id,
      (value) => {
        lastUpdate = value;

        if (skipRetry && value === CoValueLoadingState.UNAVAILABLE) {
          this.handleUpdate(value);
          return;
        }

        // Need all these checks because the migration can trigger new syncronous updates
        //
        // We want to:
        // - Run the migration only once
        // - Skip all the updates until the migration is done
        // - Trigger handleUpdate only with the final value
        if (!this.migrated && value !== CoValueLoadingState.UNAVAILABLE) {
          if (this.migrating) {
            return;
          }

          this.migrating = true;
          applyCoValueMigrations(
            instantiateRefEncodedFromRaw(this.schema, value),
          );
          this.migrated = true;
          this.handleUpdate(lastUpdate);
          return;
        }

        this.handleUpdate(value);
      },
      skipRetry,
      this.unstable_branch,
    );
  }

  updateValue(value: SubscriptionValue<D, any>) {
    this.value = value;

    // Flags that the value has changed and we need to trigger an update
    this.dirty = true;
  }

  handleUpdate(update: RawCoValue | typeof CoValueLoadingState.UNAVAILABLE) {
    if (update === CoValueLoadingState.UNAVAILABLE) {
      if (this.value.type === CoValueLoadingState.LOADING) {
        this.updateValue(
          new JazzError(this.id, CoValueLoadingState.UNAVAILABLE, [
            {
              code: CoValueLoadingState.UNAVAILABLE,
              message: "The value is unavailable",
              params: {
                id: this.id,
              },
              path: [],
            },
          ]),
        );
      }
      this.triggerUpdate();
      return;
    }

    const ruleset = update.core.verified.header.ruleset;

    // Groups and accounts are accessible by everyone, for the other coValues we use the role to check access
    const hasAccess =
      ruleset.type !== "ownedByGroup" ||
      myRoleForRawValue(update) !== undefined;

    if (!hasAccess) {
      if (this.value.type !== CoValueLoadingState.UNAUTHORIZED) {
        this.updateValue(
          new JazzError(this.id, CoValueLoadingState.UNAUTHORIZED, [
            {
              code: CoValueLoadingState.UNAUTHORIZED,
              message: `The current user (${this.node.getCurrentAgent().id}) is not authorized to access this value`,
              params: {
                id: this.id,
              },
              path: [],
            },
          ]),
        );
        this.triggerUpdate();
      }
      return;
    }

    // When resolving a CoValue with available children, we want to trigger a single update
    // after loading all the children, not one per children
    this.silenceUpdates = true;

    if (this.value.type !== CoValueLoadingState.LOADED) {
      this.updateValue(createCoValue(this.schema, update, this));
      this.loadChildren();
    } else {
      const hasChanged =
        update.totalValidTransactions !== this.totalValidTransactions ||
        // Checking the identity of the raw value makes us cover the cases where the group
        // has been updated and the coValues that don't update the totalValidTransactions value (e.g. FileStream)
        this.value.value.$jazz.raw !== update;

      if (this.loadChildren()) {
        this.updateValue(createCoValue(this.schema, update, this));
      } else if (hasChanged) {
        this.updateValue(createCoValue(this.schema, update, this));
      }
    }

    this.totalValidTransactions = update.totalValidTransactions;

    this.silenceUpdates = false;
    this.triggerUpdate();
  }

  computeChildErrors() {
    let issues: JazzErrorIssue[] = [];
    let errorType: JazzError["type"] = CoValueLoadingState.UNAVAILABLE;

    if (this.childErrors.size === 0 && this.validationErrors.size === 0) {
      return undefined;
    }

    if (this.bestEffortResolution) {
      return undefined;
    }

    for (const [key, value] of this.childErrors.entries()) {
      // We don't want to block updates if the error is on an autoloaded value
      if (this.autoloaded.has(key)) {
        continue;
      }

      if (this.skipInvalidKeys.has(key)) {
        continue;
      }

      errorType = value.type;
      if (value.issues) {
        issues.push(...value.issues);
      }
    }

    for (const [key, value] of this.validationErrors.entries()) {
      if (this.skipInvalidKeys.has(key)) {
        continue;
      }

      errorType = value.type;
      if (value.issues) {
        issues.push(...value.issues);
      }
    }

    if (issues.length) {
      return new JazzError(this.id, errorType, issues);
    }

    return undefined;
  }

  handleChildUpdate = (
    id: string,
    value: SubscriptionValue<any, any> | SubscriptionValueLoading,
    key?: string,
  ) => {
    if (value.type === CoValueLoadingState.LOADING) {
      return;
    }

    this.pendingLoadedChildren.delete(id);
    this.pendingAutoloadedChildren.delete(id);
    this.childValues.set(id, value);

    if (
      value.type === CoValueLoadingState.UNAVAILABLE ||
      value.type === CoValueLoadingState.UNAUTHORIZED
    ) {
      this.childErrors.set(id, value.prependPath(key ?? id));

      this.errorFromChildren = this.computeChildErrors();
    } else if (this.errorFromChildren && this.childErrors.has(id)) {
      this.childErrors.delete(id);

      this.errorFromChildren = this.computeChildErrors();
    }

    if (this.shouldSendUpdates()) {
      if (this.value.type === CoValueLoadingState.LOADED) {
        // On child updates, we re-create the value instance to make the updates
        // seamless-immutable and so be compatible with React and the React compiler
        this.updateValue(
          createCoValue(this.schema, this.value.value.$jazz.raw, this),
        );
      }
    }

    this.triggerUpdate();
  };

  shouldSendUpdates() {
    if (this.value.type === CoValueLoadingState.LOADING) return false;

    // If the value is in error, we send the update regardless of the children statuses
    if (this.value.type !== CoValueLoadingState.LOADED) return true;

    return this.pendingLoadedChildren.size === 0;
  }

  getCurrentValue(): D | NotLoadedCoValueState {
    if (
      this.value.type === CoValueLoadingState.UNAUTHORIZED ||
      this.value.type === CoValueLoadingState.UNAVAILABLE
    ) {
      console.error(this.value.toString());
      return this.value.type;
    }

    if (!this.shouldSendUpdates()) {
      return CoValueLoadingState.LOADING;
    }

    if (this.errorFromChildren) {
      console.error(this.errorFromChildren.toString());
      return this.errorFromChildren.type;
    }

    if (this.value.type === CoValueLoadingState.LOADED) {
      return this.value.value;
    }

    return CoValueLoadingState.LOADING;
  }

  triggerUpdate() {
    if (!this.shouldSendUpdates()) return;
    if (!this.dirty) return;
    if (this.subscribers.size === 0) return;
    if (this.silenceUpdates) return;

    const error = this.errorFromChildren;
    const value = this.value;

    if (error) {
      this.subscribers.forEach((listener) => listener(error));
    } else if (value.type !== CoValueLoadingState.LOADING) {
      this.subscribers.forEach((listener) => listener(value));
    }

    this.dirty = false;
  }

  subscribers = new Set<(value: SubscriptionValue<D, any>) => void>();
  subscribe(listener: (value: SubscriptionValue<D, any>) => void) {
    this.subscribers.add(listener);

    return () => {
      this.subscribers.delete(listener);
    };
  }

  setListener(listener: (value: SubscriptionValue<D, any>) => void) {
    this.subscribers.add(listener);
    this.triggerUpdate();
  }

  subscribeToKey(key: string) {
    if (this.resolve === true || !this.resolve) {
      this.resolve = {};
    }

    const resolve: Record<string, any> = this.resolve;
    if (!resolve.$each && !(key in resolve)) {
      // Adding the key to the resolve object to resolve the key when calling loadChildren
      resolve[key] = true;
      // Track the keys that are autoloaded to flag any id on that key as autoloaded
      this.autoloadedKeys.add(key);
    }

    if (this.value.type !== CoValueLoadingState.LOADED) {
      return;
    }

    const value = this.value.value;

    // We don't want to trigger an update when autoloading available children
    // because on userland it looks like nothing has changed since the value
    // is available on the first access
    // This helps alot with correctness when triggering the autoloading while rendering components (on React and Svelte)
    this.silenceUpdates = true;

    if (value[TypeSym] === "CoMap" || value[TypeSym] === "Account") {
      const map = value as CoMap;

      this.loadCoMapKey(map, key, true);
    } else if (value[TypeSym] === "CoList") {
      const list = value as CoList;

      this.loadCoListKey(list, key, true);
    }

    this.silenceUpdates = false;
  }

  isSubscribedToId(id: string) {
    return (
      this.idsSubscribed.has(id) ||
      this.childValues.has(id) ||
      this.pendingAutoloadedChildren.has(id) ||
      this.pendingLoadedChildren.has(id)
    );
  }

  /**
   * Checks if the currently unloaded value has got some updates
   *
   * Used to make the autoload work on closed subscription scopes
   */
  pullValue(listener: (value: SubscriptionValue<D, any>) => void) {
    if (!this.closed) {
      throw new Error("Cannot pull a non-closed subscription scope");
    }

    if (this.value.type === CoValueLoadingState.LOADED) {
      return;
    }

    // Try to pull the value from the subscription
    // into the SubscriptionScope update flow
    this.subscription.pullValue();

    // Check if the value is now available
    const value = this.getCurrentValue();

    // If the value is available, trigger the listener
    if (typeof value !== "string") {
      listener({
        type: CoValueLoadingState.LOADED,
        value,
        id: this.id,
      });
    }
  }

  subscribeToId(id: string, descriptor: RefEncoded<any>) {
    if (this.isSubscribedToId(id)) {
      if (!this.closed) {
        return;
      }

      const child = this.childNodes.get(id);

      // If the subscription is closed, check if we missed the value
      // load event
      if (child) {
        child.pullValue((value) => this.handleChildUpdate(id, value));
      }

      return;
    }

    this.idsSubscribed.add(id);
    this.autoloaded.add(id);

    // We don't want to trigger an update when autoloading available children
    // because on userland it looks like nothing has changed since the value
    // is available on the first access
    // This helps alot with correctness when triggering the autoloading while rendering components (on React and Svelte)
    this.silenceUpdates = true;

    this.pendingAutoloadedChildren.add(id);

    const child = new SubscriptionScope(
      this.node,
      true,
      id as ID<any>,
      descriptor,
      this.skipRetry,
      this.bestEffortResolution,
      this.unstable_branch,
    );
    this.childNodes.set(id, child);
    child.setListener((value) => this.handleChildUpdate(id, value));

    /**
     * If the current subscription scope is closed, spawn
     * child nodes only to load in-memory values
     */
    if (this.closed) {
      child.destroy();
    }

    this.silenceUpdates = false;
  }

  loadChildren() {
    const { resolve } = this;

    if (this.value.type !== CoValueLoadingState.LOADED) {
      return false;
    }

    const value = this.value.value;

    const depth =
      typeof resolve !== "object" || resolve === null ? {} : (resolve as any);

    let hasChanged = false;

    const idsToLoad = new Set<string>(this.idsSubscribed);

    const coValueType = value[TypeSym];

    if (Object.keys(depth).length > 0) {
      if (
        coValueType === "CoMap" ||
        coValueType === "Account" ||
        coValueType === "Group"
      ) {
        const map = value as CoMap;
        const keys =
          "$each" in depth ? map.$jazz.raw.keys() : Object.keys(depth);

        for (const key of keys) {
          const id = this.loadCoMapKey(map, key, depth[key] ?? depth.$each);

          if (id) {
            idsToLoad.add(id);
          }
        }
      } else if (value[TypeSym] === "CoList") {
        const list = value as CoList;

        const descriptor = list.$jazz.getItemsDescriptor();

        if (descriptor && isRefEncoded(descriptor)) {
          list.$jazz.raw.processNewTransactions();
          const entries = list.$jazz.raw.entries();
          const keys =
            "$each" in depth ? Object.keys(entries) : Object.keys(depth);

          for (const key of keys) {
            const id = this.loadCoListKey(list, key, depth[key] ?? depth.$each);

            if (id) {
              idsToLoad.add(id);
            }
          }
        }
      } else if (value[TypeSym] === "CoStream") {
        const stream = value as CoFeed;
        const descriptor = stream.$jazz.getItemsDescriptor();

        if (descriptor && isRefEncoded(descriptor)) {
          for (const session of stream.$jazz.raw.sessions()) {
            const values = stream.$jazz.raw.items[session] ?? [];

            for (const [i, item] of values.entries()) {
              const key = `${session}/${i}`;

              if (!depth.$each && !depth[key]) {
                continue;
              }

              const id = item.value as string | undefined;

              if (id) {
                idsToLoad.add(id);
                this.loadChildNode(id, depth[key] ?? depth.$each, descriptor);
                this.validationErrors.delete(key);
              } else if (!descriptor.optional) {
                this.validationErrors.set(
                  key,
                  new JazzError(undefined, CoValueLoadingState.UNAVAILABLE, [
                    {
                      code: "validationError",
                      message: `The ref on position ${key} requested on ${stream.constructor.name} is missing`,
                      params: {},
                      path: [key],
                    },
                  ]),
                );
              }
            }
          }
        }
      }
    }

    this.errorFromChildren = this.computeChildErrors();

    // Collect all the deleted ids
    for (const id of this.childNodes.keys()) {
      if (!idsToLoad.has(id)) {
        hasChanged = true;
        const childNode = this.childNodes.get(id);

        if (childNode) {
          childNode.destroy();
        }

        this.pendingLoadedChildren.delete(id);
        this.pendingAutoloadedChildren.delete(id);
        this.childNodes.delete(id);
        this.childValues.delete(id);
      }
    }

    return hasChanged;
  }

  loadCoMapKey(map: CoMap, key: string, depth: Record<string, any> | true) {
    if (key === "$onError") {
      return undefined;
    }

    const id = map.$jazz.raw.get(key) as string | undefined;
    const descriptor = map.$jazz.getDescriptor(key);

    if (!descriptor) {
      this.childErrors.set(
        key,
        new JazzError(undefined, CoValueLoadingState.UNAVAILABLE, [
          {
            code: "validationError",
            message: `The ref ${key} requested on ${map.constructor.name} is not defined in the schema`,
            params: {},
            path: [key],
          },
        ]),
      );
      return undefined;
    }

    if (isRefEncoded(descriptor)) {
      if (id) {
        this.loadChildNode(id, depth, descriptor, key);
        this.validationErrors.delete(key);

        return id;
      } else if (!descriptor.optional) {
        this.validationErrors.set(
          key,
          new JazzError(undefined, CoValueLoadingState.UNAVAILABLE, [
            {
              code: "validationError",
              message: `The ref ${key} requested on ${map.constructor.name} is missing`,
              params: {},
              path: [key],
            },
          ]),
        );
      }
    }

    return undefined;
  }

  loadCoListKey(list: CoList, key: string, depth: Record<string, any> | true) {
    const descriptor = list.$jazz.getItemsDescriptor();

    if (!descriptor || !isRefEncoded(descriptor)) {
      return undefined;
    }

    const entries = list.$jazz.raw.entries();
    const entry = entries[Number(key)];

    if (!entry) {
      return undefined;
    }

    const id = entry.value as string | undefined;

    if (id) {
      this.loadChildNode(id, depth, descriptor, key);
      this.validationErrors.delete(key);

      return id;
    } else if (!descriptor.optional) {
      this.validationErrors.set(
        key,
        new JazzError(undefined, CoValueLoadingState.UNAVAILABLE, [
          {
            code: "validationError",
            message: `The ref on position ${key} requested on ${list.constructor.name} is missing`,
            params: {},
            path: [key],
          },
        ]),
      );
    }

    return undefined;
  }

  loadChildNode(
    id: string,
    query: RefsToResolve<any>,
    descriptor: RefEncoded<any>,
    key?: string,
  ) {
    if (this.isSubscribedToId(id)) {
      return;
    }

    const isAutoloaded = key && this.autoloadedKeys.has(key);
    if (isAutoloaded) {
      this.autoloaded.add(id);
    }

    const skipInvalid = typeof query === "object" && query.$onError === "catch";

    if (skipInvalid) {
      if (key) {
        this.skipInvalidKeys.add(key);
      }

      this.skipInvalidKeys.add(id);
    }

    // Cloning the resolve objects to avoid mutating the original object when tracking autoloaded values
    const resolve =
      typeof query === "object" && query !== null ? { ...query } : query;

    if (!isAutoloaded) {
      this.pendingLoadedChildren.add(id);
    } else {
      this.pendingAutoloadedChildren.add(id);
    }

    const child = new SubscriptionScope(
      this.node,
      resolve,
      id as ID<any>,
      descriptor,
      this.skipRetry,
      this.bestEffortResolution,
      this.unstable_branch,
    );
    this.childNodes.set(id, child);
    child.setListener((value) => this.handleChildUpdate(id, value, key));

    /**
     * If the current subscription scope is closed, spawn
     * child nodes only to load in-memory values
     */
    if (this.closed) {
      child.destroy();
    }
  }

  destroy() {
    this.closed = true;

    this.subscription.unsubscribe();
    this.subscribers.clear();
    this.childNodes.forEach((child) => child.destroy());
  }
}
