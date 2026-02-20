import { LocalNode, RawCoValue } from "cojson";
import {
  CoFeed,
  CoList,
  CoMap,
  type CoValue,
  MaybeLoaded,
  NotLoaded,
  type RefEncoded,
  type RefsToResolve,
  TypeSym,
  createUnloadedCoValue,
  instantiateRefEncodedFromRaw,
  isRefEncoded,
} from "../internal.js";
import { applyCoValueMigrations } from "../lib/migration.js";
import { CoValueCoreSubscription } from "./CoValueCoreSubscription.js";
import {
  JazzError,
  fillErrorWithJazzErrorInfo,
  type JazzErrorIssue,
} from "./JazzError.js";
import type {
  BranchDefinition,
  SubscriptionPerformanceDetail,
  SubscriptionValue,
  SubscriptionValueLoading,
} from "./types.js";
import { CoValueLoadingState, NotLoadedCoValueState } from "./types.js";
import {
  captureError,
  isCustomErrorReportingEnabled,
  isDev,
} from "./errorReporting.js";
import {
  createCoValue,
  myRoleForRawValue,
  PromiseWithStatus,
  rejectedPromise,
  resolvedPromise,
} from "./utils.js";

export class SubscriptionScope<D extends CoValue> {
  static isProfilingEnabled = isDev;

  static setProfilingEnabled(enabled: boolean) {
    this.isProfilingEnabled = enabled;
  }

  static enableProfiling() {
    this.isProfilingEnabled = true;
  }

  private performanceUuid: string | undefined;
  private performanceSource: string | undefined;

  childNodes = new Map<string, SubscriptionScope<CoValue>>();
  childValues: Map<string, SubscriptionValue<CoValue>> = new Map();
  /**
   * Explicitly-loaded child ids that are unloaded
   */
  pendingLoadedChildren: Set<string> = new Set();
  /**
   * Autoloaded child ids that are unloaded
   */
  private pendingAutoloadedChildren: Set<string> = new Set();
  value: SubscriptionValue<D> | SubscriptionValueLoading;
  private childErrors: Map<string, JazzError> = new Map();
  private validationErrors: Map<string, JazzError> = new Map();
  errorFromChildren: JazzError | undefined;
  private subscription: CoValueCoreSubscription;
  private dirty = false;
  private resolve: RefsToResolve<any>;
  private idsSubscribed = new Set<string>();
  private autoloaded = new Set<string>();
  private autoloadedKeys = new Set<string>();
  private skipInvalidKeys = new Set<string>();
  private totalValidTransactions = 0;
  private version = 0;
  private migrated = false;
  private migrating = false;
  private migrationFailed = false;
  closed = false;

  private silenceUpdates = false;

  /**
   * Stack trace captured at subscription creation time.
   * This helps identify which component/hook created the subscription
   * when debugging "value unavailable" errors.
   */
  callerStack: Error | undefined;

  constructor(
    public node: LocalNode,
    resolve: RefsToResolve<any>,
    public id: string,
    public schema: RefEncoded<CoValue>,
    public skipRetry = false,
    public bestEffortResolution = false,
    public unstable_branch?: BranchDefinition,
    callerStack?: Error | undefined,
  ) {
    // Use caller stack if provided, otherwise capture here (less useful but better than nothing)
    this.callerStack = callerStack;
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

        if (this.migrationFailed) {
          this.handleUpdate(CoValueLoadingState.UNAVAILABLE);
          return;
        }

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
        if (
          !this.migrated &&
          value !== CoValueLoadingState.UNAVAILABLE &&
          hasAccessToCoValue(value)
        ) {
          if (this.migrating) {
            return;
          }

          this.migrating = true;
          const instance = instantiateRefEncodedFromRaw(this.schema, value);
          try {
            applyCoValueMigrations(instance);
          } catch (error) {
            const reason =
              error instanceof Error ? error.message : String(error);
            this.migrationFailed = true;
            this.migrated = true;
            console.error(`Migration failed for ${this.id}: ${reason}`);
            this.handleUpdate(CoValueLoadingState.UNAVAILABLE);
            return;
          }
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

  trackLoadingPerformance(source: string) {
    if (!SubscriptionScope.isProfilingEnabled || !crypto.randomUUID) {
      return;
    }

    // Already tracking this subscription
    if (this.performanceUuid) {
      return;
    }

    const currentState = this.getCurrentRawValue();

    this.performanceUuid = crypto.randomUUID();
    this.performanceSource = source;

    const detail: SubscriptionPerformanceDetail = {
      type: "jazz-subscription",
      uuid: this.performanceUuid,
      id: this.id,
      source,
      resolve: this.resolve,
      status: "pending",
      startTime: performance.now(),
      callerStack: this.callerStack?.stack,
    };

    performance.mark(`jazz.subscription.start:${this.performanceUuid}`, {
      detail,
    });

    if (currentState !== CoValueLoadingState.LOADING) {
      this.emitLoadingComplete(currentState);
      return;
    }

    // Subscribe to get notified when loading completes
    const unsubscribe = this.subscribe(() => {
      const rawValue = this.getCurrentRawValue();

      if (rawValue === CoValueLoadingState.LOADING) {
        return;
      }

      this.emitLoadingComplete(rawValue);
      unsubscribe();
    });
  }

  private emitLoadingComplete(rawValue: D | NotLoadedCoValueState) {
    if (!this.performanceUuid) return;

    const isError = typeof rawValue === "string";
    const endTime = performance.now();

    let errorType: SubscriptionPerformanceDetail["errorType"];
    if (isError) {
      if (
        rawValue === CoValueLoadingState.UNAVAILABLE ||
        rawValue === CoValueLoadingState.UNAUTHORIZED ||
        rawValue === CoValueLoadingState.DELETED
      ) {
        errorType = rawValue;
      }
    }

    const detail: SubscriptionPerformanceDetail = {
      type: "jazz-subscription",
      uuid: this.performanceUuid,
      id: this.id,
      source: this.performanceSource ?? "unknown",
      resolve: this.resolve,
      status: isError ? "error" : "loaded",
      startTime: 0, // Will be calculated from measure
      endTime,
      errorType,
      devtools: {
        track: "Jazz 🎶",
        properties: [
          ["id", this.id],
          ["source", this.performanceSource ?? "unknown"],
        ],
        tooltipText: this.getCreationStackLines(false),
      },
    };

    performance.mark(`jazz.subscription.end:${this.performanceUuid}`, {
      detail,
    });

    try {
      performance.measure(
        `${detail.source}(${this.id}, ${JSON.stringify(this.resolve)})`,
        {
          start: `jazz.subscription.start:${this.performanceUuid}`,
          end: `jazz.subscription.end:${this.performanceUuid}`,
          detail,
        },
      );
    } catch {
      // Marks may have been cleared
    }
  }

  updateValue(value: SubscriptionValue<D>) {
    this.value = value;

    // Flags that the value has changed and we need to trigger an update
    this.dirty = true;
  }

  private handleUpdate(
    update: RawCoValue | typeof CoValueLoadingState.UNAVAILABLE,
  ) {
    if (update === CoValueLoadingState.UNAVAILABLE) {
      if (this.value.type === CoValueLoadingState.LOADING) {
        const error = new JazzError(this.id, CoValueLoadingState.UNAVAILABLE, [
          {
            code: CoValueLoadingState.UNAVAILABLE,
            message: `Jazz Unavailable Error: unable to load ${this.id}${this.node.syncWhen === "never" ? '. Sync is disabled (when: "never"), so this CoValue can only be loaded from local storage.' : this.node.syncWhen === "signedUp" ? ". Sync is set to when: \"signedUp\" — if the user hasn't signed up, the CoValue can't be loaded from the server." : ""}`,
            params: {
              id: this.id,
            },
            path: [],
          },
        ]);

        this.updateValue(error);
      }

      this.triggerUpdate();
      return;
    }

    if (update.core.isDeleted) {
      if (this.value.type !== CoValueLoadingState.DELETED) {
        const error = new JazzError(this.id, CoValueLoadingState.DELETED, [
          {
            code: CoValueLoadingState.DELETED,
            message: `Jazz Deleted Error: ${this.id} has been deleted`,
            params: {
              id: this.id,
            },
            path: [],
          },
        ]);

        this.updateValue(error);
        this.triggerUpdate();
      }
      return;
    }

    if (!hasAccessToCoValue(update)) {
      if (this.value.type !== CoValueLoadingState.UNAUTHORIZED) {
        const message = `Jazz Authorization Error: The current user (${this.node.getCurrentAgent().id}) is not authorized to access ${this.id}`;

        const error = new JazzError(this.id, CoValueLoadingState.UNAUTHORIZED, [
          {
            code: CoValueLoadingState.UNAUTHORIZED,
            message,
            params: {
              id: this.id,
            },
            path: [],
          },
        ]);

        this.updateValue(error);
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
        update.version !== this.version;

      if (this.loadChildren()) {
        this.updateValue(createCoValue(this.schema, update, this));
      } else if (hasChanged) {
        this.updateValue(createCoValue(this.schema, update, this));
      }
    }

    this.totalValidTransactions = update.totalValidTransactions;
    this.version = update.version;

    this.silenceUpdates = false;
    this.triggerUpdate();
  }

  private computeChildErrors() {
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

  handleChildUpdate(
    id: string,
    value: SubscriptionValue<CoValue> | SubscriptionValueLoading,
    key?: string,
  ) {
    if (value.type === CoValueLoadingState.LOADING) {
      return;
    }

    this.pendingLoadedChildren.delete(id);
    this.pendingAutoloadedChildren.delete(id);
    this.childValues.set(id, value);

    if (
      value.type === CoValueLoadingState.UNAVAILABLE ||
      value.type === CoValueLoadingState.DELETED ||
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
  }

  private shouldSendUpdates() {
    if (this.value.type === CoValueLoadingState.LOADING) return false;

    // If the value is in error, we send the update regardless of the children statuses
    if (this.value.type !== CoValueLoadingState.LOADED) return true;

    return this.pendingLoadedChildren.size === 0;
  }

  unloadedValue: NotLoaded<D> | undefined;

  private lastPromise: PromiseWithStatus<D> | undefined;

  private getErrorOpts() {
    return {
      cause: this.callerStack,
    };
  }

  getPromise() {
    const currentValue = this.getCurrentValue();

    if (currentValue.$isLoaded) {
      return resolvedPromise<D>(currentValue);
    }

    if (currentValue.$jazz.loadingState !== CoValueLoadingState.LOADING) {
      const error = this.getError();
      return rejectedPromise<D>(
        fillErrorWithJazzErrorInfo(
          new Error("Unknown error", this.getErrorOpts()),
          error,
        ),
      );
    }

    const promise = new Promise<D>((resolve, reject) => {
      const unsubscribe = this.subscribe(() => {
        const currentValue = this.getCurrentValue();

        if (currentValue.$jazz.loadingState === CoValueLoadingState.LOADING) {
          return;
        }

        if (currentValue.$isLoaded) {
          promise.status = "fulfilled";
          promise.value = currentValue;
          resolve(currentValue);
        } else {
          promise.status = "rejected";
          promise.reason = fillErrorWithJazzErrorInfo(
            new Error("Unknown error", this.getErrorOpts()),
            this.getError(),
          );
          reject(promise.reason);
        }

        unsubscribe();
      });
    }) as PromiseWithStatus<D>;

    promise.status = "pending";

    return promise;
  }

  getCachedPromise() {
    if (this.lastPromise) {
      const value = this.getCurrentValue();

      // if the value is loaded, we update the promise state
      // to ensure that the value provided is always up to date
      if (value.$isLoaded) {
        this.lastPromise.status = "fulfilled";
        this.lastPromise.value = value;
      } else if (value.$jazz.loadingState !== CoValueLoadingState.LOADING) {
        this.lastPromise.status = "rejected";
        this.lastPromise.reason = fillErrorWithJazzErrorInfo(
          new Error("Unknown error", this.getErrorOpts()),
          this.getError(),
        );
      } else if (this.lastPromise.status !== "pending") {
        // Value got into loading state, we need to suspend again
        this.lastPromise = this.getPromise();
      }
    } else {
      this.lastPromise = this.getPromise();
    }

    return this.lastPromise;
  }

  private getUnloadedValue(reason: NotLoadedCoValueState): NotLoaded<D> {
    if (this.unloadedValue?.$jazz.loadingState === reason) {
      return this.unloadedValue;
    }

    const unloadedValue: NotLoaded<D> = createUnloadedCoValue(this.id, reason);

    this.unloadedValue = unloadedValue;

    return unloadedValue;
  }

  private lastErrorLogged: JazzError | undefined;

  getCurrentValue(): MaybeLoaded<D> {
    const rawValue = this.getCurrentRawValue();

    if (
      rawValue === CoValueLoadingState.UNAUTHORIZED ||
      rawValue === CoValueLoadingState.DELETED ||
      rawValue === CoValueLoadingState.UNAVAILABLE ||
      rawValue === CoValueLoadingState.LOADING
    ) {
      this.logError();
      return this.getUnloadedValue(rawValue);
    }

    return rawValue;
  }

  private getCurrentRawValue(): D | NotLoadedCoValueState {
    if (
      this.value.type === CoValueLoadingState.UNAUTHORIZED ||
      this.value.type === CoValueLoadingState.DELETED ||
      this.value.type === CoValueLoadingState.UNAVAILABLE
    ) {
      return this.value.type;
    }

    if (!this.shouldSendUpdates()) {
      return CoValueLoadingState.LOADING;
    }

    if (this.errorFromChildren) {
      return this.errorFromChildren.type;
    }

    if (this.value.type === CoValueLoadingState.LOADED) {
      return this.value.value;
    }

    return CoValueLoadingState.LOADING;
  }

  private getCreationStackLines(fullFrame: boolean = true) {
    const stack = this.callerStack?.stack;

    if (!stack) {
      return "";
    }

    const creationStackLines = stack.split("\n").slice(2, 15);
    const creationAppFrame = creationStackLines.find(
      (line) =>
        !line.includes("node_modules") &&
        !line.includes("useCoValueSubscription") &&
        !line.includes("useCoState") &&
        !line.includes("useAccount") &&
        !line.includes("jazz-tools"),
    );

    let result = "\n\n";

    if (creationAppFrame) {
      (result += "Subscription created "), (result += creationAppFrame.trim());
    }

    if (!fullFrame) {
      return result;
    }

    result += "\nFull subscription creation stack:";
    for (const line of creationStackLines.slice(0, 8)) {
      result += "\n    " + line.trim();
    }

    return result;
  }

  private getError() {
    if (
      this.value.type === CoValueLoadingState.UNAUTHORIZED ||
      this.value.type === CoValueLoadingState.DELETED ||
      this.value.type === CoValueLoadingState.UNAVAILABLE
    ) {
      return this.value;
    }

    if (this.errorFromChildren) {
      return this.errorFromChildren;
    }
  }

  private logError() {
    const error = this.getError();

    if (!error || this.lastErrorLogged === error) {
      return;
    }

    if (error.type === CoValueLoadingState.UNAVAILABLE && this.skipRetry) {
      return;
    }

    this.lastErrorLogged = error;

    if (isCustomErrorReportingEnabled()) {
      captureError(new Error(error.toString(), { cause: this.callerStack }), {
        getPrettyStackTrace: () => this.getCreationStackLines(),
        jazzError: error,
      });
    } else {
      console.error(`${error.toString()}${this.getCreationStackLines()}`);
    }
  }

  private triggerUpdate() {
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

  subscribers = new Set<(value: SubscriptionValue<D>) => void>();
  subscriberChangeCallbacks = new Set<(count: number) => void>();

  /**
   * Subscribe to subscriber count changes
   * Callback receives the total number of subscribers
   * Returns an unsubscribe function
   */
  onSubscriberChange(callback: (count: number) => void): () => void {
    this.subscriberChangeCallbacks.add(callback);

    return () => {
      this.subscriberChangeCallbacks.delete(callback);
    };
  }

  private notifySubscriberChange() {
    const count = this.subscribers.size;
    this.subscriberChangeCallbacks.forEach((callback) => {
      callback(count);
    });
  }

  subscribe(listener: (value: SubscriptionValue<D>) => void) {
    this.subscribers.add(listener);
    this.notifySubscriberChange();

    return () => {
      this.subscribers.delete(listener);
      this.notifySubscriberChange();
    };
  }

  setListener(listener: (value: SubscriptionValue<D>) => void) {
    const hadListener = this.subscribers.has(listener);
    this.subscribers.add(listener);
    // Only notify if this is a new listener (count actually changed)
    if (!hadListener) {
      this.notifySubscriberChange();
    }
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
      const map = value as unknown as CoMap;

      this.loadCoMapKey(map, key, true);
    } else if (value[TypeSym] === "CoList") {
      const list = value as unknown as CoList;

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
  pullValue(listener: (value: SubscriptionValue<D>) => void) {
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
    const value = this.getCurrentRawValue();

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
      id,
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

  private loadChildren() {
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
        const map = value as unknown as CoMap;
        const keys =
          "$each" in depth ? map.$jazz.raw.keys() : Object.keys(depth);

        for (const key of keys) {
          const id = this.loadCoMapKey(map, key, depth[key] ?? depth.$each);

          if (id) {
            idsToLoad.add(id);
          }
        }
      } else if (value[TypeSym] === "CoList") {
        const list = value as unknown as CoList;

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
        const stream = value as unknown as CoFeed;
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
                      message: `Jazz Validation Error: The ref on position ${key} is missing`,
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

  private loadCoMapKey(
    map: CoMap,
    key: string,
    depth: Record<string, any> | true,
  ) {
    if (key === "$onError") {
      return undefined;
    }

    // Check if $onError: "catch" is specified for this key
    const skipInvalid = typeof depth === "object" && depth.$onError === "catch";
    if (skipInvalid) {
      this.skipInvalidKeys.add(key);
    }

    const id = map.$jazz.raw.get(key) as string | undefined;
    const descriptor = map.$jazz.getDescriptor(key);

    if (!descriptor) {
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
              message: `Jazz Validation Error: The ref ${key} is required but missing`,
              params: {},
              path: [key],
            },
          ]),
        );
      }
    }

    return undefined;
  }

  private loadCoListKey(
    list: CoList,
    key: string,
    depth: Record<string, any> | true,
  ) {
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
            message: `Jazz Validation Error: The ref on position ${key} is required but missing`,
            params: {},
            path: [key],
          },
        ]),
      );
    }

    return undefined;
  }

  private loadChildNode(
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
      id,
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
    const hadSubscribers = this.subscribers.size > 0;
    this.subscribers.clear();
    // Notify callbacks that subscriber count is now 0 if there were subscribers before
    if (hadSubscribers) {
      this.notifySubscriberChange();
    }
    // Clear subscriber change callbacks to prevent memory leaks
    this.subscriberChangeCallbacks.clear();
    this.childNodes.forEach((child) => child.destroy());
  }
}

function hasAccessToCoValue(rawCoValue: RawCoValue): boolean {
  const ruleset = rawCoValue.core.verified.header.ruleset;

  // Groups and accounts are accessible by everyone, for the other coValues we use the role to check access
  return (
    ruleset.type !== "ownedByGroup" ||
    myRoleForRawValue(rawCoValue) !== undefined
  );
}
