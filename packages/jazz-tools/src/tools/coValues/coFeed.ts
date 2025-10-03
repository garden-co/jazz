/* eslint-disable @typescript-eslint/ban-ts-comment */
import type {
  AgentID,
  BinaryStreamInfo,
  CojsonInternalTypes,
  JsonValue,
  RawAccountID,
  RawBinaryCoStream,
  RawCoStream,
  SessionID,
} from "cojson";
import { cojsonInternals } from "cojson";
import {
  AnonymousJazzAgent,
  CoFieldInit,
  CoValue,
  CoValueClass,
  CoValueLoadingState,
  getCoValueOwner,
  Group,
  ID,
  MaybeLoaded,
  LoadedAndRequired,
  unstable_mergeBranch,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Schema,
  SchemaFor,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
  BranchDefinition,
} from "../internal.js";
import {
  Account,
  CoValueBase,
  CoValueJazzApi,
  ItemsSym,
  Ref,
  SchemaInit,
  accessChildById,
  coField,
  ensureCoValueLoaded,
  inspect,
  instantiateRefEncodedWithInit,
  isRefEncoded,
  loadCoValueWithoutMe,
  parseCoValueCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
} from "../internal.js";

/** @deprecated Use CoFeedEntry instead */
export type CoStreamEntry<Item> = CoFeedEntry<Item>;

export type CoFeedEntry<Item> = SingleCoFeedEntry<Item> & {
  all: IterableIterator<SingleCoFeedEntry<Item>>;
};

/** @deprecated Use SingleCoFeedEntry instead */
export type SingleCoStreamEntry<Item> = SingleCoFeedEntry<Item>;

export type SingleCoFeedEntry<Item> = {
  value: LoadedAndRequired<Item> extends CoValue
    ? MaybeLoaded<LoadedAndRequired<Item>>
    : Item;
  ref: LoadedAndRequired<Item> extends CoValue
    ? Ref<LoadedAndRequired<Item>>
    : never;
  by: Account | null;
  madeAt: Date;
  tx: CojsonInternalTypes.TransactionID;
};

/** @deprecated Use CoFeed instead */
export { CoFeed as CoStream };

/**
 * CoFeeds are collaborative logs of data.
 *
 * @categoryDescription Content
 * They are similar to `CoList`s, but with a few key differences:
 * - They are append-only
 * - They consist of several internal append-only logs, one per account session (tab, device, app instance, etc.)
 * - They expose those as a per-account aggregated view (default) or a precise per-session view
 *
 * ```ts
 * favDog.push("Poodle");
 * favDog.push("Schnowzer");
 * ```
 *
 * @category CoValues
 */
export class CoFeed<out Item = any> extends CoValueBase implements CoValue {
  declare $jazz: CoFeedJazzApi<this>;

  /**
   * Declare a `CoFeed` by subclassing `CoFeed.Of(...)` and passing the item schema using a `co` primitive or a `coField.ref`.
   *
   * @example
   * ```ts
   * class ColorFeed extends CoFeed.Of(coField.string) {}
   * class AnimalFeed extends CoFeed.Of(coField.ref(Animal)) {}
   * ```
   *
   * @category Declaration
   */
  static Of<Item>(item: Item): typeof CoFeed<Item> {
    const cls = class CoFeedOf extends CoFeed<Item> {
      [coField.items] = item;
    };

    cls._schema ||= {};
    cls._schema[ItemsSym] = (item as any)[SchemaInit];

    return cls;
  }

  /** @category Type Helpers */
  declare [TypeSym]: "CoStream";
  static {
    this.prototype[TypeSym] = "CoStream";
  }

  /** @internal This is only a marker type and doesn't exist at runtime */
  [ItemsSym]!: Item;
  /** @internal */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static _schema: any;

  /**
   * The current account's view of this `CoFeed`
   * @category Content
   */
  get byMe(): CoFeedEntry<Item> | undefined {
    if (this.$jazz.loadedAs[TypeSym] === "Account") {
      return this.perAccount[this.$jazz.loadedAs.$jazz.id];
    } else {
      return undefined;
    }
  }

  /**
   * The per-account view of this `CoFeed`
   *
   * @example
   * ```ts
   * // Access entries directly by account ID
   * const aliceEntries = feed[aliceAccount.id];
   * console.log(aliceEntries.value); // Latest value from Alice
   *
   * // Iterate through all accounts' entries
   * for (const [accountId, entries] of Object.entries(feed)) {
   *   console.log(`Latest entry from ${accountId}:`, entries.value);
   *
   *   // Access all entries from this account
   *   for (const entry of entries.all) {
   *     console.log(`Entry made at ${entry.madeAt}:`, entry.value);
   *   }
   * }
   * ```
   *
   * @category Content
   */
  get perAccount(): {
    [key: ID<Account>]: CoFeedEntry<Item>;
  } {
    return new Proxy({}, CoStreamPerAccountProxyHandler(this)) as any;
  }

  /**
   * The per-session view of this `CoFeed`
   * @category Content
   */
  get perSession(): {
    [key: SessionID]: CoFeedEntry<Item>;
  } {
    return new Proxy(
      {},
      CoStreamPerSessionProxyHandler(this, this) as any,
    ) as any;
  }

  /**
   * The current session's view of this `CoFeed`
   *
   * This is a shortcut for `this.perSession` where the session ID is the current session ID.
   *
   * @category Content
   */
  get inCurrentSession(): CoFeedEntry<Item> | undefined {
    if (this.$jazz.loadedAs[TypeSym] === "Account") {
      return this.perSession[this.$jazz.loadedAs.$jazz.sessionID!];
    } else {
      return undefined;
    }
  }

  /** @internal */
  constructor(options: { fromRaw: RawCoStream }) {
    super();

    Object.defineProperties(this, {
      $jazz: {
        value: new CoFeedJazzApi(this, options.fromRaw),
        enumerable: false,
      },
    });

    return this;
  }

  /**
   * Create a new `CoFeed`
   * @category Creation
   * @deprecated Use `co.feed(...).create` instead.
   */
  static create<S extends CoFeed>(
    this: CoValueClass<S>,
    init: S extends CoFeed<infer Item> ? Item[] : never,
    options?: { owner: Account | Group } | Account | Group,
  ) {
    const { owner } = parseCoValueCreateOptions(options);
    const raw = owner.$jazz.raw.createStream();
    const instance = new this({ fromRaw: raw });

    if (init) {
      instance.$jazz.push(...init);
    }
    return instance;
  }

  /**
   * Get a JSON representation of the `CoFeed`
   * @category
   */
  toJSON(): {
    $jazz: { id: string };
    [key: string]: unknown;
    in: { [key: string]: unknown };
  } {
    const itemDescriptor = this.$jazz.schema[ItemsSym] as Schema;
    const mapper =
      itemDescriptor === "json"
        ? (v: unknown) => v
        : "encoded" in itemDescriptor
          ? itemDescriptor.encoded.encode
          : (v: unknown) => v && (v as CoValue).$jazz.id;

    return {
      $jazz: { id: this.$jazz.id },
      ...Object.fromEntries(
        Object.entries(this).map(([account, entry]) => [
          account,
          mapper(entry.value),
        ]),
      ),
      in: Object.fromEntries(
        Object.entries(this.perSession).map(([session, entry]) => [
          session,
          mapper(entry.value),
        ]),
      ),
    };
  }

  /** @internal */
  [inspect](): {
    $jazz: { id: string };
    [key: string]: unknown;
    in: { [key: string]: unknown };
  } {
    return this.toJSON();
  }

  /** @internal */
  static schema<V extends CoFeed>(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this: { new (...args: any): V } & typeof CoFeed,
    def: { [ItemsSym]: V["$jazz"]["schema"][ItemsSym] },
  ) {
    this._schema ||= {};
    Object.assign(this._schema, def);
  }

  /**
   * Load a `CoFeed`
   * @category Subscription & Loading
   * @deprecated Use `co.feed(...).load` instead.
   */
  static load<F extends CoFeed, const R extends RefsToResolve<F> = true>(
    this: CoValueClass<F>,
    id: ID<F>,
    options: {
      resolve?: RefsToResolveStrict<F, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<MaybeLoaded<Resolved<F, R>>> {
    return loadCoValueWithoutMe(this, id, options);
  }

  /**
   * Subscribe to a `CoFeed`, when you have an ID but don't have a `CoFeed` instance yet
   * @category Subscription & Loading
   * @deprecated Use `co.feed(...).subscribe` instead.
   */
  static subscribe<F extends CoFeed, const R extends RefsToResolve<F> = true>(
    this: CoValueClass<F>,
    id: ID<F>,
    listener: (value: Resolved<F, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<F extends CoFeed, const R extends RefsToResolve<F> = true>(
    this: CoValueClass<F>,
    id: ID<F>,
    options: SubscribeListenerOptions<F, R>,
    listener: (value: Resolved<F, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<F extends CoFeed, const R extends RefsToResolve<F>>(
    this: CoValueClass<F>,
    id: ID<F>,
    ...args: SubscribeRestArgs<F, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<F, R>(this, id, options, listener);
  }
}

/** @internal */
type CoFeedItem<L> = L extends CoFeed<infer Item> ? Item : never;

export class CoFeedJazzApi<F extends CoFeed> extends CoValueJazzApi<F> {
  constructor(
    private coFeed: F,
    public raw: RawCoStream,
  ) {
    super(coFeed);
  }

  get owner(): Group {
    return getCoValueOwner(this.coFeed);
  }

  /**
   * Push items to this `CoFeed`
   *
   * Items are appended to the current session's log. Each session (tab, device, app instance)
   * maintains its own append-only log, which is then aggregated into the per-account view.
   *
   * @example
   * ```ts
   * // Adds items to current session's log
   * feed.$jazz.push("item1", "item2");
   *
   * // View items from current session
   * console.log(feed.inCurrentSession);
   *
   * // View aggregated items from all sessions for current account
   * console.log(feed.byMe);
   * ```
   *
   * @category Content
   */
  push(...items: CoFieldInit<CoFeedItem<F>>[]): void {
    for (const item of items) {
      this.pushItem(item);
    }
  }

  private pushItem(item: CoFieldInit<CoFeedItem<F>>) {
    const itemDescriptor = this.schema[ItemsSym] as Schema;

    if (itemDescriptor === "json") {
      this.raw.push(item as JsonValue);
    } else if ("encoded" in itemDescriptor) {
      this.raw.push(itemDescriptor.encoded.encode(item));
    } else if (isRefEncoded(itemDescriptor)) {
      let refId = (item as unknown as CoValue).$jazz?.id;
      if (!refId) {
        const coValue = instantiateRefEncodedWithInit(
          itemDescriptor,
          item,
          this.owner,
        );
        refId = coValue.$jazz.id;
      }
      this.raw.push(refId);
    }
  }

  /**
   * Ensure a `CoFeed` is loaded to the specified depth
   *
   * @returns A new instance of the same CoFeed that's loaded to the specified depth
   * @category Subscription & Loading
   */
  ensureLoaded<F extends CoFeed, const R extends RefsToResolve<F>>(
    this: CoFeedJazzApi<F>,
    options?: {
      resolve?: RefsToResolveStrict<F, R>;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Resolved<F, R>> {
    return ensureCoValueLoaded(this.coFeed, options);
  }

  /**
   * An instance method to subscribe to an existing `CoFeed`
   *
   * No need to provide an ID or Account since they're already part of the instance.
   * @category Subscription & Loading
   */
  subscribe<F extends CoFeed, const R extends RefsToResolve<F>>(
    this: CoFeedJazzApi<F>,
    listener: (value: Resolved<F, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<F extends CoFeed, const R extends RefsToResolve<F>>(
    this: CoFeedJazzApi<F>,
    options: {
      resolve?: RefsToResolveStrict<F, R>;
      unstable_branch?: BranchDefinition;
    },
    listener: (value: Resolved<F, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<F extends CoFeed, const R extends RefsToResolve<F>>(
    this: CoFeedJazzApi<F>,
    ...args: SubscribeRestArgs<F, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToExistingCoValue(this.coFeed, options, listener);
  }

  /**
   * Wait for the `CoFeed` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this.raw.core.waitForSync(options);
  }

  /**
   * Get the descriptor for the items in the `CoFeed`
   * @internal
   */
  getItemsDescriptor(): Schema | undefined {
    return this.schema[ItemsSym];
  }

  /** @internal */
  get schema(): {
    [ItemsSym]: SchemaFor<CoFeedItem<F>> | any;
  } {
    return (this.coFeed.constructor as typeof CoFeed)._schema;
  }
}

/**
 * Converts a raw stream entry into a formatted CoFeed entry with proper typing and accessors.
 * @internal
 */
function entryFromRawEntry<Item>(
  accessFrom: CoValue,
  rawEntry: {
    by: RawAccountID | AgentID;
    tx: CojsonInternalTypes.TransactionID;
    at: Date;
    value: JsonValue;
  },
  loadedAs: Account | AnonymousJazzAgent,
  accountID: ID<Account> | undefined,
  itemField: Schema,
): Omit<CoFeedEntry<Item>, "all"> {
  return {
    get value(): LoadedAndRequired<Item> extends CoValue
      ? MaybeLoaded<CoValue & Item>
      : Item {
      if (itemField === "json") {
        return rawEntry.value as LoadedAndRequired<Item> extends CoValue
          ? MaybeLoaded<CoValue & Item>
          : Item;
      } else if ("encoded" in itemField) {
        return itemField.encoded.decode(rawEntry.value);
      } else if (isRefEncoded(itemField)) {
        return accessChildById(
          accessFrom,
          rawEntry.value as string,
          itemField,
        ) as LoadedAndRequired<Item> extends CoValue
          ? MaybeLoaded<CoValue & Item>
          : Item;
      } else {
        throw new Error("Invalid item field schema");
      }
    },
    get ref(): LoadedAndRequired<Item> extends CoValue
      ? Ref<LoadedAndRequired<Item>>
      : never {
      if (itemField !== "json" && isRefEncoded(itemField)) {
        const rawId = rawEntry.value;
        return new Ref(
          rawId as unknown as ID<CoValue>,
          loadedAs,
          itemField,
          accessFrom,
        ) as LoadedAndRequired<Item> extends CoValue
          ? Ref<LoadedAndRequired<Item>>
          : never;
      } else {
        return undefined as never;
      }
    },
    get by() {
      return (
        accountID &&
        accessChildById(accessFrom, accountID, {
          ref: Account,
          optional: false,
        })
      );
    },
    madeAt: rawEntry.at,
    tx: rawEntry.tx,
  };
}

/**
 * The proxy handler for `CoFeed` instances
 * @internal
 */
export const CoStreamPerAccountProxyHandler = (
  innerTarget: CoFeed,
): ProxyHandler<{}> => ({
  get(_target, key, receiver) {
    if (typeof key === "string" && key.startsWith("co_")) {
      const rawEntry = innerTarget.$jazz.raw.lastItemBy(key as RawAccountID);

      if (!rawEntry) return;
      const entry = entryFromRawEntry(
        receiver,
        rawEntry,
        innerTarget.$jazz.loadedAs,
        key as unknown as ID<Account>,
        innerTarget.$jazz.schema[ItemsSym],
      );

      Object.defineProperty(entry, "all", {
        get: () => {
          const allRawEntries = innerTarget.$jazz.raw.itemsBy(
            key as RawAccountID,
          );
          return (function* () {
            while (true) {
              const rawEntry = allRawEntries.next();
              if (rawEntry.done) return;
              yield entryFromRawEntry(
                receiver,
                rawEntry.value,
                innerTarget.$jazz.loadedAs,
                key as unknown as ID<Account>,
                innerTarget.$jazz.schema[ItemsSym],
              );
            }
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
          })() satisfies IterableIterator<SingleCoFeedEntry<any>>;
        },
      });

      return entry;
    } else {
      return Reflect.get(innerTarget, key, receiver);
    }
  },
  ownKeys(_target) {
    return Array.from(innerTarget.$jazz.raw.accounts());
  },
  getOwnPropertyDescriptor(_target, key) {
    if (typeof key === "string" && key.startsWith("co_")) {
      return {
        configurable: true,
        enumerable: true,
        writable: false,
      };
    } else {
      return Reflect.getOwnPropertyDescriptor(innerTarget, key);
    }
  },
});

/**
 * The proxy handler for the per-session view of a `CoFeed`
 * @internal
 */
const CoStreamPerSessionProxyHandler = (
  innerTarget: CoFeed,
  accessFrom: CoFeed,
): ProxyHandler<Record<string, never>> => ({
  get(_target, key, receiver) {
    if (typeof key === "string" && key.includes("session")) {
      const sessionID = key as SessionID;
      const rawEntry = innerTarget.$jazz.raw.lastItemIn(sessionID);

      if (!rawEntry) return;
      const by = cojsonInternals.accountOrAgentIDfromSessionID(sessionID);

      const entry = entryFromRawEntry(
        accessFrom,
        rawEntry,
        innerTarget.$jazz.loadedAs,
        cojsonInternals.isAccountID(by)
          ? (by as unknown as ID<Account>)
          : undefined,
        innerTarget.$jazz.schema[ItemsSym],
      );

      Object.defineProperty(entry, "all", {
        get: () => {
          const allRawEntries = innerTarget.$jazz.raw.itemsIn(sessionID);
          return (function* () {
            while (true) {
              const rawEntry = allRawEntries.next();
              if (rawEntry.done) return;
              yield entryFromRawEntry(
                accessFrom,
                rawEntry.value,
                innerTarget.$jazz.loadedAs,
                cojsonInternals.isAccountID(by)
                  ? (by as unknown as ID<Account>)
                  : undefined,
                innerTarget.$jazz.schema[ItemsSym],
              );
            }
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
          })() satisfies IterableIterator<SingleCoFeedEntry<any>>;
        },
      });

      return entry;
    } else {
      return Reflect.get(innerTarget, key, receiver);
    }
  },
  ownKeys() {
    return innerTarget.$jazz.raw.sessions();
  },
  getOwnPropertyDescriptor(target, key) {
    if (typeof key === "string" && key.startsWith("co_")) {
      return {
        configurable: true,
        enumerable: true,
        writable: false,
      };
    } else {
      return Reflect.getOwnPropertyDescriptor(target, key);
    }
  },
});

/** @deprecated Use FileStream instead */
export { FileStream as BinaryCoStream };

/**
 * FileStreams are `CoFeed`s that contain binary data, collaborative versions of `Blob`s.
 *
 * @categoryDescription Declaration
 * `FileStream` can be referenced in schemas.
 *
 * ```ts
 * import { coField, FileStream } from "jazz-tools";
 *
 * class MyCoMap extends CoMap {
 *   file = coField.ref(FileStream);
 * }
 * ```
 *
 * @category CoValues
 */
export class FileStream extends CoValueBase implements CoValue {
  declare $jazz: FileStreamJazzApi<this>;

  /** @category Type Helpers */
  declare [TypeSym]: "BinaryCoStream";

  constructor(
    options:
      | {
          owner: Account | Group;
        }
      | {
          fromRaw: RawBinaryCoStream;
        },
  ) {
    super();

    let raw: RawBinaryCoStream;

    if ("fromRaw" in options) {
      raw = options.fromRaw;
    } else {
      const rawOwner = options.owner.$jazz.raw;
      raw = rawOwner.createBinaryStream();
    }

    Object.defineProperties(this, {
      [TypeSym]: { value: "BinaryCoStream", enumerable: false },
      $jazz: {
        value: new FileStreamJazzApi(this, raw),
        enumerable: false,
      },
    });
  }

  /**
   * Create a new empty `FileStream` instance.
   *
   * @param options - Configuration options for the new FileStream
   * @param options.owner - The Account or Group that will own this FileStream and control access rights
   *
   * @example
   * ```typescript
   * // Create owned by an account
   * const stream = FileStream.create({ owner: myAccount });
   *
   * // Create owned by a group
   * const stream = FileStream.create({ owner: teamGroup });
   *
   * // Create with implicit owner
   * const stream = FileStream.create(myAccount);
   * ```
   *
   * @remarks
   * For uploading an existing file or blob, use {@link FileStream.createFromBlob} instead.
   *
   * @category Creation
   * @deprecated Use `co.fileStream(...).create` instead.
   */
  static create<S extends FileStream>(
    this: CoValueClass<S>,
    options?: { owner?: Account | Group } | Account | Group,
  ) {
    return new this(parseCoValueCreateOptions(options));
  }

  getMetadata(): BinaryStreamInfo | undefined {
    return this.$jazz.raw.getBinaryStreamInfo();
  }

  getChunks(options?: {
    allowUnfinished?: boolean;
  }):
    | (BinaryStreamInfo & { chunks: Uint8Array[]; finished: boolean })
    | undefined {
    return this.$jazz.raw.getBinaryChunks(options?.allowUnfinished);
  }

  isBinaryStreamEnded(): boolean {
    return this.$jazz.raw.isBinaryStreamEnded();
  }

  start(options: BinaryStreamInfo): void {
    this.$jazz.raw.startBinaryStream(options);
  }

  push(data: Uint8Array): void {
    this.$jazz.raw.pushBinaryStreamChunk(data);
  }

  end(): void {
    this.$jazz.raw.endBinaryStream();
  }

  toBlob(options?: { allowUnfinished?: boolean }): Blob | undefined {
    const chunks = this.getChunks({
      allowUnfinished: options?.allowUnfinished,
    });

    if (!chunks) {
      return undefined;
    }

    // @ts-ignore
    return new Blob(chunks.chunks, { type: chunks.mimeType });
  }

  /**
   * Load a `FileStream` as a `Blob`
   *
   * @category Content
   * @deprecated Use `co.fileStream(...).loadAsBlob` instead.
   */
  static async loadAsBlob(
    id: ID<FileStream>,
    options?: {
      allowUnfinished?: boolean;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Blob | undefined> {
    let stream = await this.load(id, options);

    if (stream.$jazzState !== CoValueLoadingState.LOADED) {
      return undefined;
    }

    return stream.toBlob({
      allowUnfinished: options?.allowUnfinished,
    });
  }

  static async loadAsBase64(
    id: ID<FileStream>,
    options?: {
      allowUnfinished?: boolean;
      loadAs?: Account | AnonymousJazzAgent;
      dataURL?: boolean;
    },
  ): Promise<string | undefined> {
    const stream = await this.load(id, options);

    if (stream.$jazzState !== CoValueLoadingState.LOADED) {
      return undefined;
    }

    return stream.asBase64(options);
  }

  asBase64(options?: {
    allowUnfinished?: boolean;
    dataURL?: boolean;
  }): string | undefined {
    const chunks = this.getChunks(options);

    if (!chunks) return undefined;

    const output = [];

    for (const chunk of chunks.chunks) {
      for (const byte of chunk) {
        output.push(String.fromCharCode(byte));
      }
    }

    const base64 = btoa(output.join(""));

    if (options?.dataURL) {
      return `data:${chunks.mimeType};base64,${base64}`;
    }

    return base64;
  }

  /**
   * Create a `FileStream` from a `Blob` or `File`
   *
   * @example
   * ```ts
   * import { coField, FileStream } from "jazz-tools";
   *
   * const fileStream = await FileStream.createFromBlob(file, {owner: group})
   * ```
   * @category Content
   * @deprecated Use `co.fileStream(...).createFromBlob` instead.
   */
  static async createFromBlob(
    blob: Blob | File,
    options?:
      | {
          owner?: Account | Group;
          onProgress?: (progress: number) => void;
        }
      | Account
      | Group,
  ): Promise<FileStream> {
    const arrayBuffer = await blob.arrayBuffer();
    return this.createFromArrayBuffer(
      arrayBuffer,
      blob.type,
      blob instanceof File ? blob.name : undefined,
      options,
    );
  }

  /**
   * Create a `FileStream` from a `Blob` or `File`
   *
   * @example
   * ```ts
   * import { coField, FileStream } from "jazz-tools";
   *
   * const fileStream = await FileStream.createFromBlob(file, {owner: group})
   * ```
   * @category Content
   * @deprecated Use `co.fileStream(...).createFromArrayBuffer` instead.
   */
  static async createFromArrayBuffer(
    arrayBuffer: ArrayBuffer,
    mimeType: string,
    fileName: string | undefined,
    options?:
      | {
          owner?: Account | Group;
          onProgress?: (progress: number) => void;
        }
      | Account
      | Group,
  ): Promise<FileStream> {
    const stream = this.create(options);
    const onProgress =
      options && "onProgress" in options ? options.onProgress : undefined;

    const start = Date.now();

    const data = new Uint8Array(arrayBuffer);
    stream.start({
      mimeType,
      totalSizeBytes: arrayBuffer.byteLength,
      fileName,
    });
    const chunkSize =
      cojsonInternals.TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE;

    let lastProgressUpdate = Date.now();

    for (let idx = 0; idx < data.length; idx += chunkSize) {
      stream.push(data.slice(idx, idx + chunkSize));

      if (Date.now() - lastProgressUpdate > 100) {
        onProgress?.(idx / data.length);
        lastProgressUpdate = Date.now();
      }

      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    stream.end();
    const end = Date.now();

    console.debug(
      "Finished creating binary stream in",
      (end - start) / 1000,
      "s - Throughput in MB/s",
      (1000 * (arrayBuffer.byteLength / (end - start))) / (1024 * 1024),
    );
    onProgress?.(1);

    return stream;
  }

  /**
   * Get a JSON representation of the `FileStream`
   * @category Content
   */
  toJSON(): {
    $jazz: { id: string };
    mimeType?: string;
    totalSizeBytes?: number;
    fileName?: string;
    chunks?: Uint8Array[];
    finished?: boolean;
  } {
    return {
      $jazz: { id: this.$jazz.id },
      ...this.getChunks(),
    };
  }

  /** @internal */
  [inspect]() {
    return this.toJSON();
  }

  /**
   * Load a `FileStream`
   * @category Subscription & Loading
   * @deprecated Use `co.fileStream(...).load` instead.
   */
  static async load<C extends FileStream>(
    this: CoValueClass<C>,
    id: ID<C>,
    options?: {
      loadAs?: Account | AnonymousJazzAgent;
      allowUnfinished?: boolean;
    },
  ): Promise<MaybeLoaded<FileStream>> {
    const stream = await loadCoValueWithoutMe(this, id, options);

    /**
     * If the user hasn't requested an incomplete blob and the
     * stream isn't complete wait for the stream download before progressing
     */
    if (
      !options?.allowUnfinished &&
      stream.$jazzState === CoValueLoadingState.LOADED &&
      !stream.isBinaryStreamEnded()
    ) {
      return new Promise<FileStream>((resolve) => {
        subscribeToCoValueWithoutMe(
          this,
          id,
          options || {},
          (value, unsubscribe) => {
            if (value.isBinaryStreamEnded()) {
              unsubscribe();
              resolve(value);
            }
          },
        );
      });
    }

    return stream;
  }

  /**
   * Subscribe to a `FileStream`, when you have an ID but don't have a `FileStream` instance yet
   * @category Subscription & Loading
   * @deprecated Use `co.fileStream(...).subscribe` instead.
   */
  static subscribe<F extends FileStream, const R extends RefsToResolve<F>>(
    this: CoValueClass<F>,
    id: ID<F>,
    listener: (value: Resolved<F, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<F extends FileStream, const R extends RefsToResolve<F>>(
    this: CoValueClass<F>,
    id: ID<F>,
    options: SubscribeListenerOptions<F, R>,
    listener: (value: Resolved<F, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<F extends FileStream, const R extends RefsToResolve<F>>(
    this: CoValueClass<F>,
    id: ID<F>,
    ...args: SubscribeRestArgs<F, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<F, R>(this, id, options, listener);
  }
}

export class FileStreamJazzApi<F extends FileStream> extends CoValueJazzApi<F> {
  constructor(
    private fileStream: F,
    public raw: RawBinaryCoStream,
  ) {
    super(fileStream);
  }

  get owner(): Group {
    return getCoValueOwner(this.fileStream);
  }

  /**
   * An instance method to subscribe to an existing `FileStream`
   * @category Subscription & Loading
   */
  subscribe<B extends FileStream>(
    this: FileStreamJazzApi<B>,
    listener: (value: Resolved<B, true>) => void,
  ): () => void {
    return subscribeToExistingCoValue(this.fileStream, {}, listener);
  }

  /**
   * Wait for the `FileStream` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this.raw.core.waitForSync(options);
  }
}
