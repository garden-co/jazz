import { LocalNode, RawCoMap } from "cojson";
import { Account } from "../exports.js";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import { AnonymousJazzAgent, ID } from "../internal.js";
import { CoMapInstanceClass } from "./coMap.js";
import {
  CoMap,
  CoValueDefinition,
  Loaded,
  RelationsToResolve,
  RelationsToResolveStrict,
  isRelationRef,
} from "./schema.js";

type SubscribeListener<
  D extends CoValueDefinition<any>,
  R extends RelationsToResolve<D>,
> = (value: Loaded<D, R>, unsubscribe: () => void) => void;

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
    public id: ID<CoMap<any>>,
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

export class CoValueResolutionNode<
  D extends CoValueDefinition<any>,
  R extends RelationsToResolve<D>,
> {
  childNodes = new Map<string, CoValueResolutionNode<CoMap<any>, any>>();
  childValues = new Map<string, Loaded<any, any> | undefined>();
  value: Loaded<D, R> | undefined;
  status: "loading" | "loaded" | "unauthorized" | "unavailable" = "loading";
  promise: ResolvablePromise<void> | undefined;
  subscription: Subscription;
  listener: ((value: Loaded<D, R>) => void) | undefined;

  constructor(
    public node: LocalNode,
    public resolve: RelationsToResolve<D>,
    public id: ID<D>,
    public schema: D,
  ) {
    this.subscription = new Subscription(node, id, (value) => {
      this.handleUpdate(value);
    });
  }

  handleUpdate(value: RawCoMap) {
    if (!this.value) {
      this.value = CoMapInstanceClass.fromRaw<D, R>(
        this.schema,
        value,
        this.childValues,
        this,
      );
      this.loadChildren();
      if (this.isLoaded()) {
        this.listener?.(this.value);
      }
    } else if (this.isLoaded()) {
      this.value = this.value.$updated() as Loaded<D, R>;
      this.listener?.(this.value);
    }
  }

  handleChildUpdate = (key: string, value: Loaded<any, any>) => {
    this.childValues.set(key, value);

    if (this.value && this.isLoaded()) {
      this.value = this.value.$updated(this.childValues) as Loaded<D, R>;
      this.listener?.(this.value);
    }
  };

  isLoaded() {
    if (!this.value) return false;

    for (const value of this.childValues.values()) {
      if (value === undefined) return false;
    }

    return true;
  }

  request(resolve: RelationsToResolve<D>) {
    if (this.resolve === true || !this.resolve) {
      this.resolve = resolve;
    } else if (typeof this.resolve === "object") {
      // TODO: Better merge strategy
      // @ts-expect-error
      this.resolve = { ...this.resolve, ...resolve };
    }

    this.loadChildren();
  }

  setListener(listener: (value: Loaded<D, R>) => void) {
    this.listener = listener;
    if (this.value && this.isLoaded()) {
      this.listener(this.value);
    }
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
        const refDescriptor = schema.get(key);

        if (refDescriptor === undefined) {
          continue;
        }

        if (value && isRelationRef(refDescriptor) && resolve[key]) {
          const childSchema =
            refDescriptor === "self"
              ? this.schema
              : (refDescriptor as CoMap<any>);

          this.childValues.set(key, undefined);
          const child = new CoValueResolutionNode(
            node,
            resolve[key] as RelationsToResolve<any>,
            raw.get(key) as ID<any>,
            childSchema,
          );
          child.setListener((value) => this.handleChildUpdate(key, value));
          this.childNodes.set(key, child);
        }
      }
    }
  }
}

export function subscribeToCoValue<
  D extends CoValueDefinition<any>,
  R extends RelationsToResolve<D>,
>(
  schema: D,
  id: ID<D>,
  options: {
    resolve?: RelationsToResolveStrict<D, R>;
    loadAs?: Account | AnonymousJazzAgent;
    onUnavailable?: () => void;
    onUnauthorized?: () => void;
  },
  listener: SubscribeListener<D, R>,
) {
  const loadAs = options.loadAs ?? activeAccountContext.get();
  const node = "node" in loadAs ? loadAs.node : loadAs._raw.core.node;

  const resolve = options.resolve ?? true;

  let unsubscribed = false;

  const rootNode = new CoValueResolutionNode<D, R>(
    node,
    resolve,
    id as ID<D>,
    schema,
  );
  rootNode.setListener(handleUpdate);

  function unsubscribe() {
    unsubscribed = true;
    rootNode.subscription.unsubscribe();
  }

  function handleUpdate(value: Loaded<D, R>) {
    if (unsubscribed) return;

    listener(value, unsubscribe);
  }

  return unsubscribe;
}

export function loadCoValue<
  D extends CoValueDefinition<any>,
  R extends RelationsToResolve<D>,
>(
  schema: D,
  id: ID<D>,
  options?: {
    resolve?: RelationsToResolveStrict<D, R>;
    loadAs?: Account | AnonymousJazzAgent;
  },
) {
  return new Promise<Loaded<D, R> | undefined>((resolve) => {
    subscribeToCoValue<D, R>(
      schema,
      id,
      {
        resolve: options?.resolve,
        loadAs: options?.loadAs,
        onUnavailable: () => {
          resolve(undefined);
        },
        onUnauthorized: () => {
          resolve(undefined);
        },
      },
      (value, unsubscribe) => {
        resolve(value);
        unsubscribe();
      },
    );
  });
}

export async function ensureCoValueLoaded<
  D extends CoValueDefinition<any>,
  R extends RelationsToResolve<D>,
>(
  existing: CoMapInstanceClass<D, true>,
  options?: { resolve?: RelationsToResolveStrict<D, R> } | undefined,
) {
  const response = await loadCoValue<D, R>(existing.$schema, existing.$id, {
    loadAs: existing._loadedAs,
    resolve: options?.resolve,
  });

  if (!response) {
    throw new Error("Failed to deeply load CoValue " + existing.$id);
  }

  return response;
}
