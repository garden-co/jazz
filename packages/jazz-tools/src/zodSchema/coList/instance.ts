import { CoValueUniqueness, JsonValue, RawAccount, RawCoList } from "cojson";
import { ZodError, ZodIssue, ZodTypeAny } from "zod";
import type { Account } from "../../coValues/account.js";
import type { Group } from "../../coValues/group.js";
import { RegisteredSchemas } from "../../coValues/registeredSchemas.js";
import { AnonymousJazzAgent } from "../../internal.js";
import { coValuesCache } from "../../lib/cache.js";
import { LazySchema, isLazySchema } from "../coValue/lazy.js";
import { isOptional } from "../coValue/optional.js";
import { extensibleResolveQuery } from "../coValue/typeUtils.js";
import {
  ID,
  Loaded,
  MaybeLoaded,
  ResolveQuery,
  ResolveQueryStrict,
  Unloaded,
} from "../coValue/types.js";
import { getUnloadedState } from "../coValue/unloaded.js";
import { CoValueResolutionNode, ensureCoValueLoaded } from "../subscribe.js";

import { AnyCoListSchema, CoListInit } from "./schema.js";
import { getOwnerFromRawValue } from "./utils.js";

export class LoadedCoListJazzProps<
  D extends AnyCoListSchema,
  V,
  R extends ResolveQuery<D> = true,
> extends Array<V> {
  declare $jazzState: "loaded";
  declare $jazz: CoListJazzApi<D, R>;
}

type ListContent = Array<unknown | undefined>;

export class CoListJazzApi<
  D extends AnyCoListSchema,
  R extends ResolveQuery<D> = true,
> {
  raw: RawCoList;
  schema: D;
  id: ID<D>;
  // TODO: Refs should be tracked by id
  _resolutionNode: CoValueResolutionNode<D> | undefined;
  protected lastUpdateTx: number;
  declare _instance: Loaded<D, any>;
  declare _resolveQuery: extensibleResolveQuery<R>;

  error: undefined;

  constructor(
    schema: D,
    raw: RawCoList,
    resolutionNode?: CoValueResolutionNode<D>,
  ) {
    this.schema = schema;
    this.raw = raw;
    // @ts-expect-error
    this.lastUpdateTx = raw.totalProcessedTransactions;
    this.id = raw.id as unknown as ID<D>;
    this._resolutionNode = resolutionNode;
  }

  _setInstance(instance: LoadedCoListJazzProps<D, any>) {
    this._instance = instance as unknown as Loaded<D, R>;
  }

  remove(index: number) {
    throw new Error("Not implemented");
  }

  retain(callback: (value: ListContent[number], index: number) => boolean) {
    throw new Error("Not implemented");
  }

  push(value: ListContent[number]) {
    throw new Error("Not implemented");
  }

  pop() {
    throw new Error("Not implemented");
  }

  shift() {
    throw new Error("Not implemented");
  }

  unshift(value: ListContent[number]) {
    throw new Error("Not implemented");
  }

  sort(compareFn?: (a: ListContent[number], b: ListContent[number]) => number) {
    throw new Error("Not implemented");
  }

  reverse() {
    throw new Error("Not implemented");
  }

  updated(): Loaded<D, R> {
    throw new Error("Not implemented");
  }

  /**
   * Given an already loaded `CoMapSchema`, ensure that the specified fields are loaded to the specified depth.
   *
   * Works like `CoMapSchema.load()`, but you don't need to pass the ID or the account to load as again.
   *
   * @category Subscription & Loading
   */
  ensureLoaded<O extends ResolveQuery<D>>(options: {
    resolve: ResolveQueryStrict<D, O>;
  }): Promise<Loaded<D, O>> {
    return ensureCoValueLoaded<D, R, O>(this._instance as Loaded<D, R>, {
      resolve: options.resolve,
    });
  }

  /**
   * Wait for the `CoMapSchema` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this.raw.core.waitForSync(options);
  }

  get _loadedAs(): Account | AnonymousJazzAgent {
    const rawAccount = this.raw.core.node.account;

    if (rawAccount instanceof RawAccount) {
      return coValuesCache.get(rawAccount, () =>
        RegisteredSchemas["Account"].fromRaw(rawAccount),
      );
    }

    return new AnonymousJazzAgent(this.raw.core.node);
  }

  get owner(): Account | Group {
    return getOwnerFromRawValue(this.raw);
  }

  values() {
    type KeysOf<T> = Exclude<keyof T, "$jazz" | "$jazzState"> & string;

    type ValuesOf<T> = T[KeysOf<T>];

    return Object.values(this._instance) as ValuesOf<Loaded<D, R>>[];
  }

  entries() {
    type KeysOf<T> = Exclude<keyof T, "$jazz" | "$jazzState"> & string;
    type EntriesOf<T> = [KeysOf<T>, T[KeysOf<T>]];

    return Object.entries(this._instance) as EntriesOf<Loaded<D, R>>[];
  }

  keys() {
    type KeysOf<T> = Exclude<keyof T, "$jazz" | "$jazzState"> & string;

    return Object.keys(this._instance) as KeysOf<Loaded<D, R>>[];
  }
}

export function createCoList<D extends AnyCoListSchema>(
  schema: D,
  init: CoListInit<D>,
  owner: Account | Group,
  uniqueness?: CoValueUniqueness,
) {
  const { raw, refs } = createCoListFromInit(
    init as any,
    owner,
    schema,
    uniqueness,
  );

  throw new Error("Not implemented");

  // return createCoListFromRaw<D, true>(schema, raw, refs);
}

export function createCoListFromRaw<
  D extends AnyCoListSchema,
  R extends ResolveQuery<D>,
>(
  schema: D,
  raw: RawCoList,
  refs?: ListContent,
  resolutionNode?: CoValueResolutionNode<D>,
) {
  // const instance = new Array(raw.length);

  // // TODO: Would it be better to use defineProperty instead of setting the prototype?
  // const prototype = [];

  // Object.setPrototypeOf(instance, prototype);

  // prototype.$jazz = new CoListJazzApi(schema, raw, resolutionNode);
  // prototype.$jazzState = "loaded";

  // instance.$jazz._setInstance(instance);

  throw new Error("Not implemented");
}

function createCoListFromInit<D extends AnyCoListSchema>(
  init: CoListInit<D> | undefined,
  owner: Account | Group,
  schema: D,
  uniqueness?: CoValueUniqueness,
) {
  const rawOwner = owner._raw;

  const rawInit = [];

  const refs = new Map<string, MaybeLoaded<any>>();

  throw new Error("Not implemented");

  const raw = rawOwner.createList(rawInit, null, "private", uniqueness);

  return { raw, refs };
}
