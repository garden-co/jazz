import type { Session } from "./context.js";
import type { QueryBuilder, QueryOptions } from "./db.js";
import type { RowDelta, SubscriptionDelta } from "./subscription-manager.js";

export type SubscriptionChannelCallback<T extends { id: string }> = (
  delta: SubscriptionDelta<T>,
) => void;

export interface EncodedSubscriptionRow {
  id: string;
  bytes: Uint8Array;
}

export type EncodedSubscriptionRowDelta =
  | { kind: 0 | 2; id: string; index: number; row: EncodedSubscriptionRow }
  | { kind: 1; id: string; index: number };

export interface EncodedSubscriptionDelta {
  all: EncodedSubscriptionRow[];
  delta: EncodedSubscriptionRowDelta[];
}

export interface SubscriptionRowCodec<T extends { id: string }> {
  decode(row: EncodedSubscriptionRow): T;
}

export interface SubscriptionChannel {
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: SubscriptionChannelCallback<T>,
    options?: QueryOptions,
    session?: Session,
  ): () => void;
  shutdown?(): Promise<void> | void;
}

export type SubscriptionChannelTarget = SubscriptionChannel;

export class InProcessSubscriptionChannel implements SubscriptionChannel {
  constructor(private readonly target: SubscriptionChannelTarget) {}

  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: SubscriptionChannelCallback<T>,
    options?: QueryOptions,
    session?: Session,
  ): () => void {
    return this.target.subscribeAll(query, callback, options, session);
  }

  shutdown(): Promise<void> | void {
    return this.target.shutdown?.();
  }
}

export function createInProcessSubscriptionChannel(
  target: SubscriptionChannelTarget,
): InProcessSubscriptionChannel {
  return new InProcessSubscriptionChannel(target);
}

/**
 * Build a row proxy over transferable encoded row bytes. The proxy decodes on
 * first field access, so worker transports can pass encoded records across the
 * API subscription channel without structured-cloning object trees.
 */
export function createLazySubscriptionRow<T extends { id: string }>(
  encoded: EncodedSubscriptionRow,
  codec: SubscriptionRowCodec<T>,
): T {
  let decoded: T | undefined;
  const materialize = () => {
    decoded ??= codec.decode(encoded);
    return decoded;
  };

  return new Proxy({ id: encoded.id } as T, {
    get(_target, property, receiver) {
      if (property === "id") {
        return encoded.id;
      }
      return Reflect.get(materialize(), property, receiver);
    },
    has(_target, property) {
      return property === "id" || property in materialize();
    },
    ownKeys() {
      return Reflect.ownKeys(materialize());
    },
    getOwnPropertyDescriptor(_target, property) {
      if (property === "id") {
        return { configurable: true, enumerable: true, value: encoded.id };
      }
      return Reflect.getOwnPropertyDescriptor(materialize(), property);
    },
  });
}

export function decodeEncodedSubscriptionDelta<T extends { id: string }>(
  encoded: EncodedSubscriptionDelta,
  codec: SubscriptionRowCodec<T>,
): SubscriptionDelta<T> {
  const decodeRow = (row: EncodedSubscriptionRow) => createLazySubscriptionRow(row, codec);
  return {
    all: encoded.all.map(decodeRow),
    delta: encoded.delta.map((change) => {
      if (change.kind === 1) {
        return change;
      }
      return {
        kind: change.kind,
        id: change.id,
        index: change.index,
        item: decodeRow(change.row),
      } as RowDelta<T>;
    }),
  };
}
