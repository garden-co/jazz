import { LocalNode, RawCoMap } from "cojson";
import { Account } from "../exports.js";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import { AnonymousJazzAgent } from "../internal.js";
import { createCoMapFromRaw, isRelationRef } from "./coMap/instance.js";
import { CoValueSchema } from "./coMap/schema.js";
import { getOwnerFromRawValue } from "./coMap/utils.js";
import { isLazySchema } from "./coValue/lazy.js";
import { isOptional } from "./coValue/optional.js";
import {
  ID,
  Loaded,
  MaybeLoaded,
  ResolveQuery,
  ResolveQueryStrict,
  Unloaded,
} from "./coValue/types.js";
import {
  getUnauthorizedState,
  getUnavailableState,
  getUnloadedState,
} from "./coValue/unloaded.js";

type SubscribeListener<D extends CoValueSchema, R extends ResolveQuery<D>> = (
  value: Loaded<D, R>,
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
    public id: ID<CoValueSchema>,
    public listener: (value: RawCoMap | "unavailable") => void,
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
          this.listener("unavailable");
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
  D extends CoValueSchema,
  R extends ResolveQuery<D>,
> {
  childNodes = new Map<string, CoValueResolutionNode<CoValueSchema, any>>();
  childValues = new Map<string, Loaded<any, any> | Unloaded<any> | undefined>();
  value: Loaded<D, R> | undefined;
  error: undefined | "unauthorized" | "unavailable";
  promise: ResolvablePromise<void> | undefined;
  subscription: Subscription;
  listener:
    | ((value: Loaded<D, R> | "unavailable" | "unauthorized") => void)
    | undefined;

  constructor(
    public node: LocalNode,
    public resolve: ResolveQuery<D>,
    public id: ID<D>,
    public schema: D,
  ) {
    this.subscription = new Subscription(node, id, (value) => {
      this.handleUpdate(value);
    });
  }

  handleUpdate(value: RawCoMap | "unavailable") {
    if (value === "unavailable") {
      this.error = "unavailable";
      this.listener?.(value);
      return;
    }

    const owner = getOwnerFromRawValue(value);

    if (owner.myRole() === undefined) {
      this.error = "unauthorized";
      this.listener?.("unauthorized");
      return;
    }

    if (!this.value) {
      const instance = createCoMapFromRaw<D, R>(
        this.schema,
        value,
        this.childValues,
        this,
      );
      this.value = instance;
      this.loadChildren();
      if (this.isLoaded()) {
        this.listener?.(this.value);
      }
    } else if (this.isLoaded()) {
      const changesOnChildren = this.loadChildren();

      if (this.isLoaded()) {
        const value = this.value.$jazz.updated(
          changesOnChildren ? this.childValues : undefined,
        ) as Loaded<D, R>;

        if (value !== this.value) {
          this.value = value;
          this.listener?.(value);
        }
      }
    }
  }

  handleChildUpdate = (
    key: string,
    value: Loaded<any, any> | "unavailable" | "unauthorized",
  ) => {
    if (value === "unavailable" || value === "unauthorized") {
      const descriptor = this.schema.get(key);

      if (descriptor && isOptional(descriptor)) {
        this.childValues.set(key, undefined);
      } else {
        this.error = value;
        this.listener?.(value);
        return;
      }
    } else {
      this.childValues.set(key, value);
    }

    if (this.value && this.isLoaded()) {
      this.value = this.value.$jazz.updated(this.childValues) as Loaded<D, R>;

      this.listener?.(this.value);
    }
  };

  isLoaded() {
    if (!this.value) return false;

    for (const value of this.childValues.values()) {
      if (value?.$jazzState === "unloaded") {
        return false;
      }
    }

    return true;
  }

  setListener(
    listener: (value: Loaded<D, R> | "unavailable" | "unauthorized") => void,
  ) {
    this.listener = listener;
    if (this.value && this.isLoaded()) {
      this.listener(this.value);
    } else if (this.error) {
      this.listener(this.error);
    }
  }

  loadChildren() {
    const { node, resolve, schema } = this;

    const raw = this.value?.$jazz.raw;

    if (raw === undefined) {
      throw new Error("RefNode is not initialized");
    }

    if (typeof resolve !== "object" || resolve === null) {
      return false;
    }

    let hasChanged = false;

    const fieldsToLoad = resolve.$each
      ? raw
          .keys()
          .filter((key: string) => !(key in schema.shape))
          .map((key: string) => [key, resolve.$each])
      : Object.entries(resolve);

    const newNodes = new Map<string, CoValueResolutionNode<any, any>>();

    for (const [key, query] of fieldsToLoad) {
      const value = raw.get(key);
      const descriptor = schema.get(key);

      if (descriptor === undefined) {
        continue;
      }

      if (this.childNodes.has(key)) {
        const child = this.childNodes.get(key);

        if (child) {
          if (child.id !== value) {
            hasChanged = true;
            const childNode = this.childNodes.get(key);

            if (childNode) {
              childNode.destroy();
            }

            this.childNodes.delete(key);
            this.childValues.delete(key);
          } else {
            continue;
          }
        }
      }

      if (value && isRelationRef(descriptor) && query) {
        hasChanged = true;
        let childSchema = descriptor as CoValueSchema;

        if (isLazySchema<CoValueSchema>(childSchema)) {
          childSchema = childSchema.lazySchema();
        }

        this.childValues.set(key, getUnloadedState(childSchema, value));
        const child = new CoValueResolutionNode(
          node,
          query,
          value as ID<any>,
          childSchema,
        );
        this.childNodes.set(key, child);
        newNodes.set(key, child);
      }
    }

    // Adding listeners after that all the child nodes are created
    // to resolving the loaded state too early beacause setListener
    // may invoke syncrounously invoke the listener if the child is already loaded
    for (const [key, child] of newNodes) {
      child.setListener((value) => this.handleChildUpdate(key, value));
    }

    return hasChanged;
  }

  destroy() {
    this.subscription.unsubscribe();
    this.childNodes.forEach((child) => child.destroy());
  }
}

export function subscribeToCoValue<
  D extends CoValueSchema,
  R extends ResolveQuery<D>,
>(
  schema: D,
  id: ID<D>,
  options: {
    resolve?: ResolveQueryStrict<D, R>;
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

  function handleUpdate(value: Loaded<D, R> | "unavailable" | "unauthorized") {
    if (unsubscribed) return;

    if (value === "unavailable") {
      options.onUnavailable?.();
    } else if (value === "unauthorized") {
      options.onUnauthorized?.();
    } else {
      listener(value, unsubscribe);
    }
  }

  return unsubscribe;
}

export function loadCoValue<D extends CoValueSchema, R extends ResolveQuery<D>>(
  schema: D,
  id: ID<D>,
  options?: {
    resolve?: ResolveQueryStrict<D, R>;
    loadAs?: Account | AnonymousJazzAgent;
  },
) {
  return new Promise<MaybeLoaded<D, R>>((resolve) => {
    subscribeToCoValue<D, R>(
      schema,
      id,
      {
        resolve: options?.resolve,
        loadAs: options?.loadAs,
        onUnavailable: () => {
          resolve(getUnavailableState(schema, id));
        },
        onUnauthorized: () => {
          resolve(getUnauthorizedState(schema, id));
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
  D extends CoValueSchema,
  I extends ResolveQuery<D>,
  R extends ResolveQuery<D>,
>(
  existing: Loaded<D, I>,
  options?: { resolve?: ResolveQueryStrict<D, R> } | undefined,
) {
  const response = await loadCoValue<D, R>(
    existing.$jazz.schema as D,
    existing.$jazz.id as ID<D>,
    {
      loadAs: existing.$jazz._loadedAs,
      resolve: options?.resolve,
    },
  );

  if (response.$jazzState !== "loaded") {
    throw new Error(
      `Failed to deeply load CoValue ${existing.$jazz.id}: ${response.$jazzState}`,
    );
  }

  // TODO: Remove this cast
  return response as Loaded<D, R>;
}
