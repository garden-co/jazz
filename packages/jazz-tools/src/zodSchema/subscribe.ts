import { LocalNode, RawCoMap } from "cojson";
import { ZodError, ZodIssue } from "zod";
import { Account } from "../exports.js";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import { AnonymousJazzAgent } from "../internal.js";
import { createCoMapFromRaw, isRelationRef } from "./coMap/instance.js";
import { CoValueSchema } from "./coMap/schema.js";
import { getOwnerFromRawValue } from "./coMap/utils.js";
import { isLazySchema } from "./coValue/lazy.js";
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
  getUnloadedJazzAPI,
  getUnloadedState,
  getValidationErrorState,
} from "./coValue/unloaded.js";

type SubscribeListener<D extends CoValueSchema, R extends ResolveQuery<D>> = (
  value: Loaded<D, R> | Unloaded<D>,
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

export class CoValueResolutionNode<D extends CoValueSchema> {
  childNodes = new Map<string, CoValueResolutionNode<CoValueSchema>>();
  childValues: Map<string, Loaded<any, any> | Unloaded<any>> = new Map<
    string,
    Loaded<any, any> | Unloaded<any>
  >();
  value: Loaded<D, any> | Unloaded<D>;
  childErrors: Map<string, Unloaded<any>> = new Map<string, Unloaded<any>>();
  errorFromChildren: Unloaded<D> | undefined;
  promise: ResolvablePromise<void> | undefined;
  subscription: Subscription;
  listener: ((value: Loaded<D, any> | Unloaded<D>) => void) | undefined;

  constructor(
    public node: LocalNode,
    public resolve: ResolveQuery<D>,
    public id: ID<D>,
    public schema: D,
  ) {
    this.value = getUnloadedState(this.schema, this.id);
    this.subscription = new Subscription(node, id, (value) => {
      this.handleUpdate(value);
    });
  }

  handleUpdate(value: RawCoMap | "unavailable") {
    if (value === "unavailable") {
      // TODO: Gernerate a ZodError
      this.value = getUnavailableState(this.schema, this.id);
      this.triggerUpdate();
      return;
    }

    const owner = getOwnerFromRawValue(value);

    if (owner.myRole() === undefined) {
      // TODO: Gernerate a ZodError
      this.value = getUnauthorizedState(this.schema, this.id);
      this.triggerUpdate();
      return;
    }

    if (this.value.$jazzState !== "loaded") {
      try {
        const instance = createCoMapFromRaw<D, any>(
          this.schema,
          value,
          this.childValues,
          this,
        );
        this.value = instance;
        this.loadChildren();
        this.triggerUpdate();
      } catch (error) {
        if (error instanceof ZodError) {
          this.value = getValidationErrorState(this.schema, this.id, error);
          this.triggerUpdate();
        } else {
          throw error;
        }
      }
    } else {
      const changesOnChildren = this.loadChildren();

      if (this.shouldSendUpdates() && !this.errorFromChildren) {
        const value = this.value.$jazz.updated(
          changesOnChildren ? this.childValues : undefined,
        );

        if (value !== this.value) {
          this.value = value;
          this.triggerUpdate();
        }
      } else if (this.errorFromChildren) {
        this.triggerUpdate();
      }
    }
  }

  computeChildErrors() {
    let errors: ZodIssue[] = [];
    let errorType: Unloaded<D>["$jazzState"] = "unloaded";

    if (this.childErrors.size === 0) {
      return undefined;
    }

    for (const value of this.childErrors.values()) {
      errorType = value.$jazzState;
      if (value.$jazz.error) {
        errors.push(...value.$jazz.error.issues);
      }
    }

    if (errors.length > 0) {
      return getUnloadedJazzAPI(
        this.schema,
        this.id,
        errorType,
        new ZodError(errors),
      );
    }

    return getUnloadedJazzAPI(this.schema, this.id, errorType);
  }

  handleChildUpdate = (
    key: string,
    value: Loaded<any, any> | Unloaded<any>,
  ) => {
    this.childValues.set(key, value as Loaded<any, any>);

    if (
      value.$jazzState === "unavailable" ||
      value.$jazzState === "unauthorized" ||
      value.$jazzState === "validationError"
    ) {
      const error = value as Unloaded<D>;
      this.childErrors.set(key, error);

      if (error.$jazz.error) {
        error.$jazz.error.issues.forEach((issue) => {
          issue.path.unshift(key);
        });
      }

      this.errorFromChildren = this.computeChildErrors();
    } else if (this.errorFromChildren && this.childErrors.has(key)) {
      this.childErrors.delete(key);

      this.errorFromChildren = this.computeChildErrors();
    }

    if (this.shouldSendUpdates() && !this.errorFromChildren) {
      this.value = this.value.$jazz.updated(this.childValues) as Loaded<D, any>;
    }

    this.triggerUpdate();
  };

  shouldSendUpdates() {
    if (this.value.$jazzState === "unloaded") return false;
    if (this.value.$jazzState !== "loaded") return true;

    for (const value of this.childValues.values()) {
      if (value.$jazzState === "unloaded") {
        return false;
      }
    }

    return true;
  }

  triggerUpdate() {
    if (this.errorFromChildren) {
      this.listener?.(this.errorFromChildren);
    } else if (this.shouldSendUpdates()) {
      this.listener?.(this.value);
    }
  }

  setListener(listener: (value: Loaded<D, any> | Unloaded<D>) => void) {
    this.listener = listener;
    this.triggerUpdate();
  }

  loadChildren() {
    const { node, resolve, schema } = this;

    const raw = this.value.$jazz.raw;

    if (raw === undefined) {
      throw new Error("RefNode is not initialized");
    }

    if (typeof resolve !== "object" || resolve === null) {
      return false;
    }

    let hasChanged = false;

    const fieldsToLoad =
      "$each" in resolve && resolve.$each
        ? raw
            .keys()
            .filter((key: string) => !(key in schema.shape))
            .map((key: string) => [key, resolve.$each])
        : Object.entries(resolve);

    const newNodes = new Map<string, CoValueResolutionNode<any>>();

    for (const [key, query] of fieldsToLoad as [
      string,
      ResolveQuery<any, [0]>,
    ][]) {
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

        this.childValues.set(
          key,
          getUnloadedState(childSchema, value as ID<any>),
        );
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
  },
  listener: SubscribeListener<D, R>,
) {
  const loadAs = options.loadAs ?? activeAccountContext.get();
  const node = "node" in loadAs ? loadAs.node : loadAs._raw.core.node;

  const resolve = options.resolve ?? true;

  let unsubscribed = false;

  const rootNode = new CoValueResolutionNode<D>(
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

  function handleUpdate(value: Loaded<D, any> | Unloaded<D>) {
    if (unsubscribed) return;

    listener(value as Loaded<D, R> | Unloaded<D>, unsubscribe);
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

  return response;
}
