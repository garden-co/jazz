import type { CoValueCore, LocalNode, RawCoValue } from "cojson";
import { type Account } from "../coValues/account.js";
import type {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  CoValueFromRaw,
  ID,
} from "../internal.js";

export const subscriptionsScopes = new WeakMap<
  CoValue,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  SubscriptionScope<any>
>();

export class SubscriptionScope<Root extends CoValue> {
  scopeID: string = `scope-${Math.random().toString(36).slice(2)}`;
  subscriber: Account | AnonymousJazzAgent;
  entries = new Map<
    ID<CoValue>,
    | { state: "loading"; immediatelyUnsub?: boolean }
    | { state: "loaded"; rawUnsub: () => void }
  >();
  rootEntry: {
    state: "loaded";
    value: RawCoValue;
    rawUnsub: () => void;
  };
  scheduleUpdate: () => void;
  scheduledUpdate: boolean = false;
  cachedValues: { [id: ID<CoValue>]: CoValue } = {};
  parents: { [id: ID<CoValue>]: Set<ID<CoValue>> } = {};
  syncResolution: boolean = false;

  constructor(
    root: Root,
    rootSchema: CoValueClass<Root> & CoValueFromRaw<Root>,
    onUpdate: (newRoot: Root, scope: SubscriptionScope<Root>) => void,
  ) {
    this.rootEntry = {
      state: "loaded" as const,
      value: root._raw,
      rawUnsub: () => {}, // placeholder
    };
    this.entries.set(root.id, this.rootEntry);

    subscriptionsScopes.set(root, this);

    this.subscriber = root._loadedAs;

    this.scheduleUpdate = () => {
      const value = rootSchema.fromRaw(this.rootEntry.value) as Root;
      subscriptionsScopes.set(value, this);
      onUpdate(value, this);
    };

    this.rootEntry.rawUnsub = root._raw.core.subscribe(
      (rawUpdate: RawCoValue | undefined) => {
        if (!rawUpdate) return;
        this.rootEntry.value = rawUpdate;
        this.scheduleUpdate();
      },
    );
  }

  onRefAccessedOrSet(
    fromId: ID<CoValue>,
    accessedOrSetId: ID<CoValue> | undefined,
  ) {
    // console.log("onRefAccessedOrSet", this.scopeID, accessedOrSetId);
    if (!accessedOrSetId) {
      return;
    }

    this.parents[accessedOrSetId] = this.parents[accessedOrSetId] || new Set();
    this.parents[accessedOrSetId]!.add(fromId);

    if (!this.entries.has(accessedOrSetId)) {
      const loadingEntry = {
        state: "loading",
        immediatelyUnsub: false,
      } as const;
      this.entries.set(accessedOrSetId, loadingEntry);
      const node =
        this.subscriber._type === "Account"
          ? this.subscriber._raw.core.node
          : this.subscriber.node;

      loadCoValue(
        node,
        accessedOrSetId,
        (core) => {
          if (
            loadingEntry.state === "loading" &&
            loadingEntry.immediatelyUnsub
          ) {
            return;
          }
          if (core !== "unavailable") {
            const entry = {
              state: "loaded" as const,
              rawUnsub: () => {}, // placeholder
            };
            this.entries.set(accessedOrSetId, entry);

            const rawUnsub = core.subscribe((rawUpdate) => {
              if (!rawUpdate) return;

              this.invalidate(accessedOrSetId);
              this.scheduleUpdate();
            });

            entry.rawUnsub = rawUnsub;
          }
        },
        this.syncResolution,
      );
    }
  }

  invalidate(id: ID<CoValue>, seen: Set<ID<CoValue>> = new Set()) {
    if (seen.has(id)) return;

    delete this.cachedValues[id];
    seen.add(id);
    for (const parent of this.parents[id] || []) {
      this.invalidate(parent, seen);
    }
  }

  unsubscribeAll = () => {
    for (const entry of this.entries.values()) {
      if (entry.state === "loaded") {
        entry.rawUnsub();
      } else {
        entry.immediatelyUnsub = true;
      }
    }
    this.entries.clear();
  };
}

/**
 * Loads a CoValue from the node and calls the callback with the result.

 * If the CoValue is already loaded, the callback is called synchronously.
 * If the CoValue is not loaded, the callback is called asynchronously.
 */
function loadCoValue(
  node: LocalNode,
  id: ID<CoValue>,
  callback: (value: CoValueCore | "unavailable") => void,
  syncResolution: boolean,
) {
  const entry = node.coValuesStore.get(id);

  if (entry.isAvailable() && syncResolution) {
    callback(entry.core);
  } else {
    void node.loadCoValueCore(id).then((core) => {
      callback(core);
    });
  }
}
