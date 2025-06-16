import {
  ControlledAccount,
  type OpID,
  RawAccount,
  type RawCoPlainText,
  stringifyOpID,
} from "cojson";
import { calcPatch } from "fast-myers-diff";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  ID,
  Resolved,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  anySchemaToCoSchema,
  parseCoValueCreateOptions,
} from "../internal.js";
import {
  inspect,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
} from "../internal.js";
import { coValuesCache } from "../lib/cache.js";
import { Account } from "./account.js";
import { Group } from "./group.js";
import { RegisteredSchemas } from "./registeredSchemas.js";

export type TextPos = OpID;

export class CoPlainText extends String implements CoValue {
  declare id: ID<this>;
  declare _type: "CoPlainText";
  declare _raw: RawCoPlainText;

  get _owner(): Account | Group {
    return this._raw.group instanceof RawAccount
      ? Account.fromRaw(this._raw.group)
      : Group.fromRaw(this._raw.group);
  }

  get _loadedAs() {
    const agent = this._raw.core.node.getCurrentAgent();

    if (agent instanceof ControlledAccount) {
      return coValuesCache.get(agent.account, () =>
        anySchemaToCoSchema(RegisteredSchemas["Account"]).fromRaw(
          agent.account,
        ),
      );
    }

    return new AnonymousJazzAgent(this._raw.core.node);
  }

  /** @internal */
  constructor(
    options:
      | { fromRaw: RawCoPlainText }
      | { text: string; owner: Account | Group }
      | undefined,
  ) {
    if (!options) {
      super(""); // Intialise as empty string
      return;
    }

    if ("fromRaw" in options) {
      super(options.fromRaw.toString());
      const raw = options.fromRaw;
      Object.defineProperties(this, {
        id: { value: raw.id, enumerable: false },
        _type: { value: "CoPlainText", enumerable: false },
        _raw: { value: raw, enumerable: false },
      });
      return;
    }

    if ("text" in options && "owner" in options) {
      super(options.text);
      const raw = options.owner._raw.createPlainText(options.text);
      Object.defineProperties(this, {
        id: { value: raw.id, enumerable: false },
        _type: { value: "CoPlainText", enumerable: false },
        _raw: { value: raw, enumerable: false },
      });
      return;
    }

    throw new Error("Invalid constructor arguments");
  }

  /**
   * Create a new `CoPlainText` with the given text and owner.
   *
   * The owner (a Group or Account) determines access rights to the CoPlainText.
   *
   * The CoPlainText will immediately be persisted and synced to connected peers.
   *
   * @example
   * ```ts
   * const text = CoPlainText.create("Hello, world!", { owner: me });
   * ```
   *
   * @category Creation
   */
  static create<T extends CoPlainText>(
    this: CoValueClass<T>,
    text: string,
    options?: { owner: Account | Group } | Account | Group,
  ) {
    const { owner } = parseCoValueCreateOptions(options);
    return new this({ text, owner });
  }

  get length() {
    return this._raw.toString().length;
  }

  toString() {
    return this._raw.toString();
  }

  valueOf() {
    return this._raw.toString();
  }

  toJSON(): string {
    return this._raw.toString();
  }

  [inspect]() {
    return this.toJSON();
  }

  insertBefore(idx: number, text: string) {
    this._raw.insertBefore(idx, text);
  }

  insertAfter(idx: number, text: string) {
    this._raw.insertAfter(idx, text);
  }

  deleteRange(range: { from: number; to: number }) {
    this._raw.deleteRange(range);
  }

  posBefore(idx: number): TextPos | undefined {
    return this._raw.mapping.opIDbeforeIdx[idx];
  }

  posAfter(idx: number): TextPos | undefined {
    return this._raw.mapping.opIDafterIdx[idx];
  }

  idxBefore(pos: TextPos): number | undefined {
    return this._raw.mapping.idxBeforeOpID[stringifyOpID(pos)];
  }

  idxAfter(pos: TextPos): number | undefined {
    return this._raw.mapping.idxAfterOpID[stringifyOpID(pos)];
  }

  static fromRaw<V extends CoPlainText>(
    this: CoValueClass<V> & typeof CoPlainText,
    raw: RawCoPlainText,
  ) {
    return new this({ fromRaw: raw });
  }

  /**
   * Apply text, modifying the text in place. Calculates the diff and applies it to the CoValue.
   *
   * @category Mutation
   */
  applyDiff(other: string) {
    const current = this._raw.toString();

    // Split both strings into grapheme arrays for proper comparison
    const currentGraphemes = this._raw.toGraphemes(current);
    const otherGraphemes = this._raw.toGraphemes(other);

    // Calculate the diff on grapheme arrays
    const patches = [...calcPatch(currentGraphemes, otherGraphemes)];

    // Apply patches in reverse order to avoid index shifting issues
    for (const [from, to, insert] of patches.reverse()) {
      if (to > from) {
        this.deleteRange({ from, to });
      }
      if (insert.length > 0) {
        // Join the graphemes back into a string for insertion
        this.insertBefore(from, this._raw.fromGraphemes(insert));
      }
    }
  }

  /**
   * Load a `CoPlainText` with a given ID, as a given account.
   *
   * @category Subscription & Loading
   */
  static load<T extends CoPlainText>(
    this: CoValueClass<T>,
    id: ID<T>,
    options?: { loadAs?: Account | AnonymousJazzAgent },
  ): Promise<T | null> {
    return loadCoValueWithoutMe(this, id, options);
  }

  //   /**
  //    * Effectful version of `CoMap.load()`.
  //    *
  //    * Needs to be run inside an `AccountCtx` context.
  //    *
  //    * @category Subscription & Loading
  //    */
  //   static loadEf<T extends CoPlainText>(
  //     this: CoValueClass<T>,
  //     id: ID<T>,
  //   ): Effect.Effect<T, UnavailableError, AccountCtx> {
  //     return loadCoValueEf(this, id, []);
  //   }

  /**
   * Load and subscribe to a `CoPlainText` with a given ID, as a given account.
   *
   * Automatically also subscribes to updates to all referenced/nested CoValues as soon as they are accessed in the listener.
   *
   * Check out the `load` methods on `CoMap`/`CoList`/`CoStream`/`Group`/`Account` to see which depth structures are valid to nest.
   *
   * Returns an unsubscribe function that you should call when you no longer need updates.
   *
   * Also see the `useCoState` hook to reactively subscribe to a CoValue in a React component.
   *
   * @category Subscription & Loading
   */
  static subscribe<T extends CoPlainText>(
    this: CoValueClass<T>,
    id: ID<T>,
    listener: (value: Resolved<T, true>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<T extends CoPlainText>(
    this: CoValueClass<T>,
    id: ID<T>,
    options: Omit<SubscribeListenerOptions<T, true>, "resolve">,
    listener: (value: Resolved<T, true>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<T extends CoPlainText>(
    this: CoValueClass<T>,
    id: ID<T>,
    ...args: SubscribeRestArgs<T, true>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<T, true>(this, id, options, listener);
  }

  /**
   * Given an already loaded `CoPlainText`, subscribe to updates to the `CoPlainText` and ensure that the specified fields are loaded to the specified depth.
   *
   * Works like `CoPlainText.subscribe()`, but you don't need to pass the ID or the account to load as again.
   *
   * Returns an unsubscribe function that you should call when you no longer need updates.
   *
   * @category Subscription & Loading
   **/
  subscribe<T extends CoPlainText>(
    this: T,
    listener: (value: Resolved<T, true>, unsubscribe: () => void) => void,
  ): () => void {
    return subscribeToExistingCoValue(this, {}, listener);
  }

  /**
   * Allow CoPlainText to behave like a primitive string in most contexts (e.g.,
   * string concatenation, template literals, React rendering, etc.) by implementing
   * Symbol.toPrimitive. This eliminates the need to call .toString() explicitly.
   *
   * The 'hint' parameter indicates the preferred type of conversion:
   * - 'string': prefer string conversion
   * - 'number': prefer number conversion (attempt to parse the text as a number)
   * - 'default': usually treat as string
   */
  [Symbol.toPrimitive](hint: string) {
    if (hint === "number") {
      // Not meaningful for text, but required for completeness
      return Number(this._raw.toString());
    }
    // For 'string' and 'default', return the string representation
    return this._raw.toString();
  }
}
