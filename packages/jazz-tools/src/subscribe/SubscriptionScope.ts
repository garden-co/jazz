import type { LocalNode, RawCoValue } from "cojson";
import {
  CoFeed,
  CoList,
  CoMap,
  type CoValue,
  type ID,
  type RefEncoded,
  type RefsToResolve,
  instantiateRefEncoded,
  isRefEncoded,
} from "../internal.js";
import { applyCoValueMigrations } from "../lib/migration.js";
import { CoValueCoreSubscription } from "./CoValueCoreSubscription.js";
import { JazzError, type JazzErrorIssue } from "./JazzError.js";
import type { SubscriptionValue, Unloaded } from "./types.js";
import { createCoValue, getOwnerFromRawValue } from "./utils.js";

export class SubscriptionScope<D extends CoValue> {
  childNodes = new Map<string, SubscriptionScope<CoValue>>();
  childValues: Map<string, SubscriptionValue<any, any> | Unloaded> = new Map<
    string,
    SubscriptionValue<D, any>
  >();
  value: SubscriptionValue<D, any> | Unloaded;
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

  silenceUpdates = false;

  constructor(
    public node: LocalNode,
    resolve: RefsToResolve<D>,
    public id: ID<D>,
    public schema: RefEncoded<D>,
  ) {
    this.resolve = resolve;
    this.value = { type: "unloaded", id };

    let lastUpdate: RawCoValue | "unavailable" | undefined;
    this.subscription = new CoValueCoreSubscription(node, id, (value) => {
      lastUpdate = value;

      // Need all these checks because the migration can trigger new syncronous updates
      //
      // We want to:
      // - Run the migration only once
      // - Skip all the updates until the migration is done
      // - Trigger handleUpdate only with the final value
      if (!this.migrated && value !== "unavailable") {
        if (this.migrating) {
          return;
        }

        this.migrating = true;
        applyCoValueMigrations(instantiateRefEncoded(this.schema, value));
        this.migrated = true;
        this.handleUpdate(lastUpdate);
        return;
      }

      this.handleUpdate(value);
    });
  }

  updateValue(value: SubscriptionValue<D, any>) {
    this.value = value;

    // Flags that the value has changed and we need to trigger an update
    this.dirty = true;
  }

  handleUpdate(update: RawCoValue | "unavailable") {
    if (update === "unavailable") {
      if (this.value.type === "unloaded") {
        this.updateValue(
          new JazzError(this.id, "unavailable", [
            {
              code: "unavailable",
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
      getOwnerFromRawValue(update).myRole() !== undefined;

    if (!hasAccess) {
      if (this.value.type !== "unauthorized") {
        this.updateValue(
          new JazzError(this.id, "unauthorized", [
            {
              code: "unauthorized",
              message:
                "The current user is not authorized to access this value",
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

    if (this.value.type !== "loaded") {
      this.updateValue(createCoValue(this.schema, update, this));
      this.loadChildren();
    } else {
      const hasChanged =
        update.totalValidTransactions !== this.totalValidTransactions ||
        // Checking the identity of the _raw value makes us cover the cases where the group
        // has been updated and the coValues that don't update the totalValidTransactions value (e.g. FileStream)
        this.value.value._raw !== update;

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
    let errorType: JazzError["type"] = "unavailable";

    if (this.childErrors.size === 0 && this.validationErrors.size === 0) {
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
    value: SubscriptionValue<any, any> | Unloaded,
    key?: string,
  ) => {
    if (value.type === "unloaded") {
      return;
    }

    this.childValues.set(id, value);

    if (value.type === "unavailable" || value.type === "unauthorized") {
      this.childErrors.set(id, value.prependPath(key ?? id));

      this.errorFromChildren = this.computeChildErrors();
    } else if (this.errorFromChildren && this.childErrors.has(id)) {
      this.childErrors.delete(id);

      this.errorFromChildren = this.computeChildErrors();
    }

    if (this.shouldSendUpdates()) {
      if (this.value.type === "loaded") {
        // On child updates, we re-create the value instance to make the updates
        // seamless-immutable and so be compatible with React and the React compiler
        this.updateValue(
          createCoValue(this.schema, this.value.value._raw, this),
        );
      }
    }

    this.triggerUpdate();
  };

  shouldSendUpdates() {
    if (this.value.type === "unloaded") return false;

    // If the value is in error, we send the update regardless of the children statuses
    if (this.value.type !== "loaded") return true;

    for (const value of this.childValues.values()) {
      // We don't wait for autoloaded values to be loaded, in order to stream updates
      // on autoloaded lists or records
      if (value.type === "unloaded" && !this.autoloaded.has(value.id)) {
        return false;
      }
    }

    return true;
  }

  getCurrentValue() {
    if (
      this.value.type === "unauthorized" ||
      this.value.type === "unavailable"
    ) {
      console.error(this.value.toString());
      return null;
    }

    if (!this.shouldSendUpdates()) {
      return undefined;
    }

    if (this.errorFromChildren) {
      console.error(this.errorFromChildren.toString());
      return null;
    }

    if (this.value.type === "loaded") {
      return this.value.value;
    }

    return undefined;
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
    } else if (value.type !== "unloaded") {
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

    if (this.resolve.$each || key in this.resolve) {
      return;
    }

    const resolve = this.resolve as Record<string, any>;

    // Adding the key to the resolve object to resolve the key when calling loadChildren
    resolve[key] = true;
    // Track the keys that are autoloaded to flag any id on that key as autoloaded
    this.autoloadedKeys.add(key);

    if (this.value.type !== "loaded") {
      return;
    }

    const value = this.value.value;

    // We don't want to trigger an update when autoloading available children
    // because on userland it looks like nothing has changed since the value
    // is available on the first access
    // This helps alot with correctness when triggering the autoloading while rendering components (on React and Svelte)
    this.silenceUpdates = true;

    if (value._type === "CoMap" || value._type === "Account") {
      const map = value as CoMap;

      this.loadCoMapKey(map, key, true);
    } else if (value._type === "CoList") {
      const list = value as CoList;

      this.loadCoListKey(list, key, true);
    }

    this.silenceUpdates = false;
  }

  subscribeToId(id: string, descriptor: RefEncoded<any>) {
    if (this.idsSubscribed.has(id) || this.childValues.has(id)) {
      return;
    }

    this.idsSubscribed.add(id);
    this.autoloaded.add(id);

    // We don't want to trigger an update when autoloading available children
    // because on userland it looks like nothing has changed since the value
    // is available on the first access
    // This helps alot with correctness when triggering the autoloading while rendering components (on React and Svelte)
    this.silenceUpdates = true;

    this.childValues.set(id, { type: "unloaded", id });
    const child = new SubscriptionScope(
      this.node,
      true,
      id as ID<any>,
      descriptor,
    );
    this.childNodes.set(id, child);
    child.setListener((value) => this.handleChildUpdate(id, value));

    this.silenceUpdates = false;
  }

  loadChildren() {
    const { resolve } = this;

    if (this.value.type !== "loaded") {
      return false;
    }

    const value = this.value.value;

    const depth =
      typeof resolve !== "object" || resolve === null ? {} : (resolve as any);

    let hasChanged = false;

    const idsToLoad = new Set<string>(this.idsSubscribed);

    const coValueType = value._type;

    if (Object.keys(depth).length > 0) {
      if (coValueType === "CoMap" || coValueType === "Account") {
        const map = value as CoMap;
        const keys = "$each" in depth ? map._raw.keys() : Object.keys(depth);

        for (const key of keys) {
          const id = this.loadCoMapKey(map, key, depth[key] ?? depth.$each);

          if (id) {
            idsToLoad.add(id);
          }
        }
      } else if (value._type === "CoList") {
        const list = value as CoList;

        const descriptor = list.getItemsDescriptor();

        if (descriptor && isRefEncoded(descriptor)) {
          list._raw.processNewTransactions();
          const entries = list._raw.entries();
          const keys =
            "$each" in depth ? Object.keys(entries) : Object.keys(depth);

          for (const key of keys) {
            const id = this.loadCoListKey(list, key, depth[key] ?? depth.$each);

            if (id) {
              idsToLoad.add(id);
            }
          }
        }
      } else if (value._type === "CoStream") {
        const stream = value as CoFeed;
        const descriptor = stream.getItemsDescriptor();

        if (descriptor && isRefEncoded(descriptor)) {
          for (const session of stream._raw.sessions()) {
            const values = stream._raw.items[session] ?? [];

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
                  new JazzError(undefined, "unavailable", [
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

    const id = map._raw.get(key) as string | undefined;
    const descriptor = map.getDescriptor(key);

    if (!descriptor) {
      this.childErrors.set(
        key,
        new JazzError(undefined, "unavailable", [
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
          new JazzError(undefined, "unavailable", [
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
    const descriptor = list.getItemsDescriptor();

    if (!descriptor || !isRefEncoded(descriptor)) {
      return undefined;
    }

    const entries = list._raw.entries();
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
        new JazzError(undefined, "unavailable", [
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
    if (this.childValues.has(id)) {
      return;
    }

    if (key && this.autoloadedKeys.has(key)) {
      this.autoloaded.add(id);
    }

    const skipInvalid = typeof query === "object" && query.$onError === null;

    if (skipInvalid) {
      if (key) {
        this.skipInvalidKeys.add(key);
      }

      this.skipInvalidKeys.add(id);
    }

    // Cloning the resolve objects to avoid mutating the original object when tracking autoloaded values
    const resolve =
      typeof query === "object" && query !== null ? { ...query } : query;

    this.childValues.set(id, { type: "unloaded", id });
    const child = new SubscriptionScope(
      this.node,
      resolve,
      id as ID<any>,
      descriptor,
    );
    this.childNodes.set(id, child);
    child.setListener((value) => this.handleChildUpdate(id, value, key));
  }

  destroy() {
    this.subscription.unsubscribe();
    this.subscribers.clear();
    this.childNodes.forEach((child) => child.destroy());
  }
}
