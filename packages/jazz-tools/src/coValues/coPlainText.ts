import {
  type OpID,
  RawAccount,
  type RawCoPlainText,
  stringifyOpID,
} from "cojson";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import type { CoValue, CoValueClass, ID } from "../internal.js";
import {
  inspect,
  isAccountInstance,
  loadCoValue,
  subscribeToCoValue,
  subscribeToExistingCoValue,
} from "../internal.js";
import { Account } from "./account.js";
import { Group } from "./group.js";

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
    return Account.fromNode(this._raw.core.node);
  }

  constructor(
    options:
      | { fromRaw: RawCoPlainText }
      | { text: string; owner: Account | Group },
  ) {
    super();

    let raw;

    if ("fromRaw" in options) {
      raw = options.fromRaw;
    } else {
      raw = options.owner._raw.createPlainText(options.text);
    }

    Object.defineProperties(this, {
      id: { value: raw.id, enumerable: false },
      _type: { value: "CoPlainText", enumerable: false },
      _raw: { value: raw, enumerable: false },
    });
  }

  static create<T extends CoPlainText>(
    this: CoValueClass<T>,
    text: string,
    options: { owner: Account | Group },
  ) {
    return new this({ text, owner: options.owner });
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
   * Load a `CoPlainText` with a given ID, as a given account.
   *
   * @category Subscription & Loading
   */
  static load<T extends CoPlainText>(
    this: CoValueClass<T>,
    id: ID<T>,
    as?: Account,
  ): Promise<T | undefined> {
    return loadCoValue(this, id, as ?? activeAccountContext.get(), []);
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
    listener: (value: T) => void,
  ): () => void;
  static subscribe<T extends CoPlainText>(
    this: CoValueClass<T>,
    id: ID<T>,
    as: Account,
    listener: (value: T) => void,
  ): () => void;
  static subscribe<T extends CoPlainText>(
    this: CoValueClass<T>,
    id: ID<T>,
    asOrListener: Account | ((value: T) => void),
    listener?: (value: T) => void,
  ): () => void {
    if (isAccountInstance(asOrListener)) {
      return subscribeToCoValue(this, id, asOrListener, [], listener!);
    }

    return subscribeToCoValue(
      this,
      id,
      activeAccountContext.get(),
      [],
      listener!,
    );
  }

  //   /**
  //    * Effectful version of `CoMap.subscribe()` that returns a stream of updates.
  //    *
  //    * Needs to be run inside an `AccountCtx` context.
  //    *
  //    * @category Subscription & Loading
  //    */
  //   static subscribeEf<T extends CoPlainText>(
  //     this: CoValueClass<T>,
  //     id: ID<T>,
  //   ): Stream.Stream<T, UnavailableError, AccountCtx> {
  //     return subscribeToCoValueEf(this, id, []);
  //   }

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
    listener: (value: T) => void,
  ): () => void {
    return subscribeToExistingCoValue(this, [], listener);
  }
}
