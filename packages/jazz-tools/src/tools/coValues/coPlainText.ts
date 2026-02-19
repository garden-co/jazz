import { type OpID, type RawCoPlainText, stringifyOpID } from "cojson";
import { calcPatch } from "fast-myers-diff";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  CoValueJazzApi,
  CoValueLoadingState,
  ID,
  Settled,
  Resolved,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
  unstable_mergeBranch,
  parseCoValueCreateOptions,
  inspect,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
  CorePlainTextSchema,
  CoValueCreateOptionsInternal,
} from "../internal.js";
import { Account } from "./account.js";
import { getCoValueOwner, Group } from "./group.js";
import { type CoreRichTextSchema } from "../implementation/zodSchema/schemaTypes/RichTextSchema.js";

export type TextPos = OpID;

export class CoPlainText extends String implements CoValue {
  static coValueSchema?: CorePlainTextSchema | CoreRichTextSchema;
  declare [TypeSym]: "CoPlainText";

  declare $jazz: CoTextJazzApi<this>;
  declare $isLoaded: true;

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
        [TypeSym]: {
          value: "CoPlainText",
          enumerable: false,
          configurable: true,
        },
        $jazz: {
          value: new CoTextJazzApi(this, raw),
          enumerable: false,
          configurable: true,
        },
        $isLoaded: { value: true, enumerable: false, configurable: true },
      });
      return;
    }

    if ("text" in options && "owner" in options) {
      super(options.text);
      const raw = options.owner.$jazz.raw.createPlainText(options.text);
      Object.defineProperties(this, {
        [TypeSym]: {
          value: "CoPlainText",
          enumerable: false,
          configurable: true,
        },
        $jazz: {
          value: new CoTextJazzApi(this, raw),
          enumerable: false,
          configurable: true,
        },
        $isLoaded: { value: true, enumerable: false, configurable: true },
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
   * @deprecated Use `co.plainText(...).create` instead.
   */
  static create<T extends CoPlainText>(
    this: CoValueClass<T>,
    text: string,
    options?: CoValueCreateOptionsInternal,
  ) {
    const { owner } = parseCoValueCreateOptions(options);
    return new this({ text, owner });
  }

  get length() {
    return this.$jazz.raw.toString().length;
  }

  toString() {
    return this.$jazz.raw.toString();
  }

  valueOf() {
    return this.$jazz.raw.toString();
  }

  toJSON(): string {
    return this.$jazz.raw.toString();
  }

  [inspect]() {
    return this.toJSON();
  }

  insertBefore(idx: number, text: string) {
    this.$jazz.raw.insertBefore(idx, text);
  }

  insertAfter(idx: number, text: string) {
    this.$jazz.raw.insertAfter(idx, text);
  }

  deleteRange(range: { from: number; to: number }) {
    this.$jazz.raw.deleteRange(range);
  }

  posBefore(idx: number): TextPos | undefined {
    return this.$jazz.raw.mapping.opIDbeforeIdx[idx];
  }

  posAfter(idx: number): TextPos | undefined {
    return this.$jazz.raw.mapping.opIDafterIdx[idx];
  }

  idxBefore(pos: TextPos): number | undefined {
    return this.$jazz.raw.mapping.idxBeforeOpID[stringifyOpID(pos)];
  }

  idxAfter(pos: TextPos): number | undefined {
    return this.$jazz.raw.mapping.idxAfterOpID[stringifyOpID(pos)];
  }

  /** @category Internals */
  static fromRaw<V extends CoPlainText>(
    this: CoValueClass<V> & typeof CoPlainText,
    raw: RawCoPlainText,
  ) {
    return new this({ fromRaw: raw });
  }

  /**
   * Load a `CoPlainText` with a given ID, as a given account.
   *
   * @category Subscription & Loading
   * @deprecated Use `co.plainText(...).load` instead.
   */
  static load<T extends CoPlainText>(
    this: CoValueClass<T>,
    id: ID<T>,
    options?: { loadAs?: Account | AnonymousJazzAgent },
  ): Promise<Settled<T>> {
    return loadCoValueWithoutMe(this, id, options);
  }

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
   * @deprecated Use `co.plainText(...).subscribe` instead.
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
      return Number(this.$jazz.raw.toString());
    }
    // For 'string' and 'default', return the string representation
    return this.$jazz.raw.toString();
  }
}

export class CoTextJazzApi<T extends CoPlainText> extends CoValueJazzApi<T> {
  constructor(
    private coText: T,
    public raw: RawCoPlainText,
  ) {
    super(coText);
  }

  get owner(): Group {
    return getCoValueOwner(this.coText);
  }

  /**
   * Apply text, modifying the text in place. Calculates the diff and applies it to the CoValue.
   *
   * @category Mutation
   */
  applyDiff(other: string) {
    const current = this.raw.toString();

    // Split both strings into grapheme arrays for proper comparison
    const currentGraphemes = this.raw.toGraphemes(current);
    const otherGraphemes = this.raw.toGraphemes(other);

    // Calculate the diff on grapheme arrays
    const patches = [...calcPatch(currentGraphemes, otherGraphemes)];

    if (patches.length === 0) {
      return;
    }

    // Turns off updates in the middle of applyDiff to improve the performance
    this.raw.core.pauseNotifyUpdate();

    // Apply patches in reverse order to avoid index shifting issues
    for (const [from, to, insert] of patches.reverse()) {
      if (to > from) {
        this.coText.deleteRange({ from, to });
      }
      if (insert.length > 0) {
        // Join the graphemes back into a string for insertion
        this.coText.insertBefore(from, this.raw.fromGraphemes(insert));
      }
    }

    this.raw.core.resumeNotifyUpdate();
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
    this: CoTextJazzApi<T>,
    listener: (value: Resolved<T, true>, unsubscribe: () => void) => void,
  ): () => void {
    return subscribeToExistingCoValue(this.coText, {}, listener);
  }
}
