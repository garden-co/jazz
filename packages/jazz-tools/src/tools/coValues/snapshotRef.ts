import { CoValueUniqueness, LocalNode, type RawCoMap } from "cojson";
import {
  CoValue,
  getCoValueOwner,
  Group,
  ID,
  RefEncoded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  TypeSym,
  CoValueBase,
  CoValueJazzApi,
  ensureCoValueLoaded,
  CoreSnapshotRefSchema,
  CoValueClass,
  parseCoValueCreateOptions,
  ResolveQuery,
  internalLoadUnique,
  Account,
  AnonymousJazzAgent,
  Settled,
  loadCoValueWithoutMe,
  ItemsSym,
  resolveCoSchemaField,
  CoValueCursor,
  instantiateRefEncodedFromRaw,
  coValueClassFromCoValueClassOrSchema,
  Loaded,
  inspect,
  isAccountOrGroup,
  CoValueCreateOptions,
  SchemaPermissions,
  withSchemaPermissions,
  accessChildByKey,
} from "../internal.js";
import { assertCoValueSchema } from "../implementation/zodSchema/schemaInvariant.js";
import { base58 } from "@scure/base";
import { CoreCoValueSchema } from "../implementation/zodSchema/schemaTypes/CoValueSchema.js";

type SnapshotRefInner<T extends SnapshotRef> = T extends SnapshotRef<
  infer Inner
>
  ? Inner
  : never;

const textEncoder = new TextEncoder();

/**
 * A SnapshotRef captures a point-in-time reference to a CoValue.
 *
 * It stores a target CoValue ID and a cursor that together identify
 * a specific snapshot of the referenced value. This is useful for
 * creating immutable references to collaborative data at a known state.
 *
 * @category CoValues
 */
export class SnapshotRef<out Inner = any>
  extends CoValueBase
  implements CoValue
{
  /** @category Type Helpers */
  declare [TypeSym]: "SnapshotRef";
  static {
    this.prototype[TypeSym] = "SnapshotRef";
  }

  /** @internal This is only a marker type and doesn't exist at runtime */
  [ItemsSym]!: Inner;

  /**
   * Jazz methods for SnapshotRefs are inside this property.
   *
   * This allows SnapshotRefs to be used as plain objects while still having
   * access to Jazz methods.
   */
  declare $jazz: SnapshotRefJazzApi<this>;

  static coValueSchema?: CoreSnapshotRefSchema;

  /** @internal */
  constructor(options: {
    fromRaw: RawCoMap;
    operation?: "create" | "load";
  }) {
    super();

    const snapshotRefSchema = assertCoValueSchema(
      this.constructor,
      "SnapshotRef",
      options.operation ?? "load",
    );

    Object.defineProperties(this, {
      $jazz: {
        value: new SnapshotRefJazzApi(
          this,
          () => options.fromRaw,
          snapshotRefSchema,
        ),
        enumerable: false,
        configurable: true,
      },
    });
  }

  /**
   * The referenced value this snapshot reference points to.
   *
   * @category Content
   */
  get ref(): Inner {
    return accessChildByKey(
      this,
      this.$jazz.raw.get("ref") as string,
      "ref",
    ) as Inner;
  }

  /**
   * The cursor identifying the specific point-in-time state of the target CoValue.
   *
   * @category Content
   */
  get cursor(): CoValueCursor {
    return this.$jazz.raw.get("cursor") as CoValueCursor;
  }

  static load<S extends SnapshotRef, const R extends RefsToResolve<S> = true>(
    this: CoValueClass<S>,
    id: ID<S>,
    options?: {
      resolve?: RefsToResolveStrict<S, R>;
      loadAs?: Account | AnonymousJazzAgent;
      skipRetry?: boolean;
    },
  ): Promise<Settled<Resolved<S, R>>> {
    return loadCoValueWithoutMe(this, id, options);
  }

  private static createSnapshotUniqueness({
    ref,
    cursor,
    node,
  }: {
    ref: string;
    cursor: string;
    node: LocalNode;
  }): CoValueUniqueness {
    const uniquenessObject = {
      version: 1,
      ref,
      cursor,
    };

    return {
      uniqueness: base58.encode(
        node.crypto.blake3HashOnce(
          textEncoder.encode(JSON.stringify(uniquenessObject)),
        ),
      ),
    };
  }

  private static createRawMap(options: {
    ref: string;
    cursor: string;
    owner: Group;
    uniqueness?: CoValueUniqueness;
    firstComesWins?: boolean;
  }) {
    return options.owner.$jazz.raw.createMap(
      {
        ref: options.ref,
        cursor: options.cursor,
      },
      null,
      "private",
      options.uniqueness,
      options.firstComesWins ? { fww: "init" } : undefined,
    );
  }

  /**
   * Create a new `SnapshotRef` pointing to the given CoValue.
   *
   * Captures the current state of the value by recording its ID and a cursor.
   * The SnapshotRef will immediately be persisted and synced to connected peers.
   *
   * @category Creation
   */
  static async create<
    S extends SnapshotRef,
    const R extends ResolveQuery<SnapshotRefInner<S>>,
  >(
    this: CoValueClass<S>,
    createInit: {
      value: Loaded<SnapshotRefInner<S>>;
      cursorResolve?: RefsToResolveStrict<SnapshotRefInner<S>, R>;
    },
    permissions: SchemaPermissions,
    options?: CoValueCreateOptions,
  ): Promise<Settled<S>> {
    const snapshotRefSchema = assertCoValueSchema(
      this,
      "SnapshotRef",
      "create",
    );

    const loadedCoValue = await ensureCoValueLoaded(
      createInit.value as CoValue,
      {
        // @ts-expect-error
        resolve: createInit.cursorResolve,
      },
    );

    const cursor = loadedCoValue.$jazz.createCursor();
    const createOptions = isAccountOrGroup(options)
      ? { owner: options }
      : { ...options };
    const me = loadedCoValue.$jazz.loadedAs;

    // if no owner was passed, pass the coValue owner if possible in order to deduplicate based on uniqueness
    if (
      !createOptions.owner &&
      loadedCoValue.$jazz.owner &&
      me.canWrite(loadedCoValue)
    ) {
      createOptions.owner = loadedCoValue.$jazz.owner;
    }

    const { owner } = parseCoValueCreateOptions(
      withSchemaPermissions(createOptions, permissions),
    );

    const loadAs = owner.$jazz.loadedAs;
    const node =
      loadAs[TypeSym] === "Anonymous" ? loadAs.node : loadAs.$jazz.localNode;

    const ref = loadedCoValue.$jazz.id;

    const uniqueness = SnapshotRef.createSnapshotUniqueness({
      ref,
      cursor,
      node,
    });

    const snapshotMap = await internalLoadUnique(
      snapshotRefSchema.snapshotRefMapSchema.getCoValueClass(),
      {
        type: "comap",
        unique: uniqueness.uniqueness,
        owner,
        onCreateWhenMissing: () => {
          SnapshotRef.createRawMap({
            ref,
            cursor,
            owner,
            uniqueness,
            firstComesWins: true,
          });
        },
      },
    );

    if (!snapshotMap.$isLoaded) {
      return snapshotMap;
    }

    return instantiateRefEncodedFromRaw(
      {
        ref: coValueClassFromCoValueClassOrSchema(this),
        optional: false,
      },
      snapshotMap.$jazz.raw,
    );
  }

  toJSON() {
    return {
      $jazz: { id: this.$jazz.id },
      ref: this.ref,
      cursor: this.cursor,
    };
  }

  [inspect]() {
    return this.toJSON();
  }
}

/**
 * Contains SnapshotRef Jazz methods that are part of the {@link SnapshotRef.$jazz`} property.
 */
class SnapshotRefJazzApi<M extends SnapshotRef> extends CoValueJazzApi<M> {
  private innerDescriptorCached: RefEncoded<CoValue> | undefined;

  constructor(
    private snapshotRef: M,
    private getRaw: () => RawCoMap,
    private coreSnapshotRefSchema: CoreSnapshotRefSchema,
  ) {
    super(snapshotRef);
  }

  /** The `Group` that owns this SnapshotRef and controls access. */
  get owner(): Group {
    return getCoValueOwner(this.snapshotRef);
  }

  /**
   * Given an already loaded `SnapshotRef`, ensure that the specified fields are loaded to the specified depth.
   *
   * Works like `SnapshotRef.load()`, but you don't need to pass the ID or the account to load as again.
   *
   * @category Subscription & Loading
   */
  ensureLoaded<S extends SnapshotRef, const R extends RefsToResolve<S>>(
    this: SnapshotRefJazzApi<S>,
    options: {
      resolve: RefsToResolveStrict<S, R>;
    },
  ): Promise<Resolved<S, R>> {
    return ensureCoValueLoaded(this.snapshotRef, options);
  }

  /**
   * Wait for the `SnapshotRef` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  async waitForSync(options?: { timeout?: number }): Promise<void> {
    await this.raw.core.waitForSync(options);
  }

  /** @internal */
  getDescriptor(key: string): RefEncoded<CoValue> | undefined {
    if (key !== "ref") {
      return undefined;
    }

    if (this.innerDescriptorCached) {
      return this.innerDescriptorCached;
    }

    const descriptor = {
      ...(resolveCoSchemaField(
        this.coreSnapshotRefSchema.innerSchema as CoreCoValueSchema & {
          getCoValueClass: () => CoValueClass;
        },
      ) as RefEncoded<CoValue>),
      isSnapshot: true,
    } satisfies RefEncoded<CoValue>;

    this.innerDescriptorCached = descriptor;
    return descriptor;
  }

  /** @internal */
  override get raw() {
    return this.getRaw();
  }
}
