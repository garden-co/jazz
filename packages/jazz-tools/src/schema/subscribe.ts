import { LocalNode, RawCoMap } from "cojson";
import { cojsonInternals } from "cojson";
import { Account } from "../exports.js";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import {
  AnonymousJazzAgent,
  ID,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SchemaInit,
  isRefEncoded,
} from "../internal.js";
import { CoMapInstance, CoMapSchema, CoMapSchemaClass } from "./coMap.js";

type SubscribeListener<S extends CoMapSchema, R extends RefsToResolve<S>> = (
  value: Resolved<S, R>,
  unsubscribe: () => void,
) => void;

function createResolvablePromise<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: any) => void;

  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}

type ResolvablePromise<T> = ReturnType<typeof createResolvablePromise<T>>;

class Subscription {
  _unsubscribe: () => void = () => {};
  unsubscribed = false;

  value: RawCoMap | undefined;
  status: "unknown" | "loading" | "loaded" | "unauthorized" | "unavailable" =
    "unknown";

  constructor(
    public node: LocalNode,
    public id: ID<CoMapSchema>,
    public listener: (value: RawCoMap) => void,
  ) {
    const value = this.node.coValuesStore.get(this.id as any);

    if (value.state.type === "available") {
      this.status = "loaded";
      this.subscribe(value.state.coValue.getCurrentContent() as RawCoMap);
    } else {
      this.status = "loading";
      this.node.load(this.id as any).then((value) => {
        if (this.unsubscribed) return;
        // TODO handle the error states which should be transitive
        if (value !== "unavailable") {
          this.status = "loaded";
          this.subscribe(value as RawCoMap);
        } else {
          this.status = "unavailable";
        }
      });
    }
  }

  subscribe(value: RawCoMap) {
    if (this.unsubscribed) return;

    this._unsubscribe = value.subscribe((value) => {
      this.listener(value);
    });
    this.listener(value);
  }

  unsubscribe() {
    if (this.unsubscribed) return;
    this.unsubscribed = true;
    this._unsubscribe();
  }
}

class CoValueResolutionNode<S extends CoMapSchema> {
  childNodes = new Map<string, CoValueResolutionNode<CoMapSchema>>();
  childValues = new Map<string, CoMapInstance<S> | undefined>();
  value: CoMapInstance<S> | undefined;
  status: "loading" | "loaded" | "unauthorized" | "unavailable" = "loading";
  promise: ResolvablePromise<void> | undefined;
  subscription: Subscription;

  constructor(
    public node: LocalNode,
    public resolve: RefsToResolve<S>,
    public id: ID<S>,
    public schema: S,
    public listener: (value: CoMapInstance<S>, isLoaded: boolean) => void,
  ) {
    this.subscription = new Subscription(node, id, (value) => {
      this.handleUpdate(value);
    });
  }

  handleUpdate(value: RawCoMap) {
    if (!this.value) {
      this.value = CoMapInstance.fromRaw(this.schema, value);
      this.loadChildren();
      this.listener(this.value, this.isLoaded());
    } else {
      this.value = this.value.$updated();
      this.listener(this.value, this.isLoaded());
    }
  }

  handleChildUpdate = (key: string, value: CoMapInstance<S>) => {
    this.childValues.set(key, value);

    this.listener(this.value!, this.isLoaded());
  };

  isLoaded() {
    if (!this.value) return false;

    for (const value of this.childValues.values()) {
      if (value === undefined) return false;
    }

    return true;
  }

  async loadChildren() {
    const { node, resolve, schema } = this;

    const raw = this.value?.$raw;

    if (raw === undefined) {
      throw new Error("RefNode is not initialized");
    }

    if (typeof resolve === "object" && resolve !== null) {
      for (const key of Object.keys(resolve)) {
        const value = raw.get(key);
        const refDescriptor = schema.getFieldDescriptor(key);

        if (value && isRefEncoded(refDescriptor)) {
          const childSchema = new (
            refDescriptor.ref as CoMapSchemaClass<any>
          )();

          this.childValues.set(key, undefined);
          const child = new CoValueResolutionNode(
            node,
            resolve[key],
            key as any,
            childSchema,
            (value) => this.handleChildUpdate(key, value),
          );
          this.childNodes.set(key, child);
        }
      }
    }
  }
}

export function subscribeToCoValue<
  S extends CoMapSchema,
  const R extends RefsToResolve<S>,
>(
  cls: CoMapSchemaClass<S>,
  id: ID<CoMapSchema>,
  options: {
    resolve?: RefsToResolveStrict<S, R>;
    loadAs?: Account | AnonymousJazzAgent;
    onUnavailable?: () => void;
    onUnauthorized?: () => void;
    syncResolution?: boolean;
  },
  listener: SubscribeListener<S, R>,
): () => void {
  const loadAs = options.loadAs ?? activeAccountContext.get();
  const node = "node" in loadAs ? loadAs.node : loadAs._raw.core.node;

  const resolve = options.resolve ?? true;

  let unsubscribed = false;
  let unsubscribe: (() => void) | undefined;

  const rootNode = new CoValueResolutionNode(node, resolve, id, new cls());

  if (rootNode.status === "loaded" && rootNode.value) {
    listener(cls.fromRaw(rootNode.value as RawCoMap), unsubscribe);
  } else {
    rootNode.promise?.promise.then(() => {
      listener(rootNode.value as Resolved<S, R>, unsubscribe);
    });
  }

  function subscribe() {
    const value = ref.getValueWithoutAccessCheck();

    if (!value) {
      options.onUnavailable?.();
      return;
    }

    if (unsubscribed) return;
    const subscription = new SubscriptionScope(
      value,
      cls as CoValueClass<V> & CoValueFromRaw<V>,
      (update, subscription) => {
        if (!ref.hasReadAccess()) {
          options.onUnauthorized?.();
          return;
        }

        let result;

        try {
          result = fulfillsDepth(options.resolve, update);
        } catch (e) {
          console.error(
            "Failed to load / subscribe to CoValue",
            e,
            e instanceof Error ? e.stack : undefined,
          );
          options.onUnavailable?.();
          return;
        }

        if (result === "unauthorized") {
          options.onUnauthorized?.();
          return;
        }

        if (result === "fulfilled") {
          listener(update as Resolved<V, R>, subscription.unsubscribeAll);
        }
      },
    );

    unsubscribe = subscription.unsubscribeAll;
  }

  const sync = options.syncResolution ? ref.syncLoad() : undefined;

  if (sync) {
    subscribe();
  } else {
    ref
      .load()
      .then(() => subscribe())
      .catch((e) => {
        console.error(
          "Failed to load / subscribe to CoValue",
          e,
          e instanceof Error ? e.stack : undefined,
        );
        options.onUnavailable?.();
      });
  }

  return function unsubscribeAtAnyPoint() {
    unsubscribed = true;
    unsubscribe && unsubscribe();
  };
}
