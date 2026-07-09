import type { Session } from "./context.js";
import type { DbConfig, QueryBuilder, QueryOptions, TableProxy } from "./db.js";
import type {
  BinaryLargeValueFileApp,
  BinaryLargeValueFileRow,
  FileReadOptions,
  FileWriteOptions,
} from "./file-storage.js";
import type { AuthState } from "./auth-state.js";
import type { RowDelta, SubscriptionDelta } from "./subscription-manager.js";
import type { CreateOptions, DeleteOptions, DurabilityTier, UpdateOptions } from "./client.js";

type MaybePromise<T> = T | Promise<T>;

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
  reset?: true;
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
  insert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options?: CreateOptions,
    session?: Session,
  ): MaybePromise<AsyncWriteResult<T>>;
  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
    session?: Session,
  ): MaybePromise<AsyncWriteHandle>;
  delete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    options?: DeleteOptions,
    session?: Session,
  ): MaybePromise<AsyncWriteHandle>;
  canInsert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    session?: Session,
  ): MaybePromise<boolean>;
  canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    session?: Session,
  ): MaybePromise<boolean>;
  canDelete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    session?: Session,
  ): MaybePromise<boolean>;
  getAuthState(): MaybePromise<AuthState>;
  onAuthChanged(listener: (state: AuthState) => void): () => void;
  updateAuthToken(token: string | null): MaybePromise<void>;
  getConfig(): MaybePromise<DbConfig>;
  createFileFromBlob<TApp extends BinaryLargeValueFileApp<any, any>>(
    app: TApp,
    blob: Blob,
    options?: FileWriteOptions,
  ): MaybePromise<BinaryLargeValueFileRow<TApp>>;
  loadFileAsBlob<TApp extends BinaryLargeValueFileApp<any, any>>(
    app: TApp,
    fileOrId: string | BinaryLargeValueFileRow<TApp>,
    options?: FileReadOptions,
  ): MaybePromise<Blob>;
  shutdown?(): Promise<void> | void;
}

export interface AsyncWriteHandle<T = void> {
  readonly transactionId: string;
  wait(options: { tier: DurabilityTier }): Promise<T>;
}

export interface AsyncWriteResult<T> extends AsyncWriteHandle<T> {
  readonly value: T;
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

  insert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options?: CreateOptions,
    session?: Session,
  ): MaybePromise<AsyncWriteResult<T>> {
    return this.target.insert(table, data, options, session);
  }

  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
    session?: Session,
  ): MaybePromise<AsyncWriteHandle> {
    return this.target.update(table, id, data, options, session);
  }

  delete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    options?: DeleteOptions,
    session?: Session,
  ): MaybePromise<AsyncWriteHandle> {
    return this.target.delete(table, id, options, session);
  }

  canInsert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    session?: Session,
  ): MaybePromise<boolean> {
    return this.target.canInsert(table, data, session);
  }

  canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    session?: Session,
  ): MaybePromise<boolean> {
    return this.target.canUpdate(table, id, data, session);
  }

  canDelete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    session?: Session,
  ): MaybePromise<boolean> {
    return this.target.canDelete(table, id, session);
  }

  getAuthState(): MaybePromise<AuthState> {
    return this.target.getAuthState();
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    return this.target.onAuthChanged(listener);
  }

  updateAuthToken(token: string | null): MaybePromise<void> {
    return this.target.updateAuthToken(token);
  }

  getConfig(): MaybePromise<DbConfig> {
    return this.target.getConfig();
  }

  createFileFromBlob<TApp extends BinaryLargeValueFileApp<any, any>>(
    app: TApp,
    blob: Blob,
    options?: FileWriteOptions,
  ): MaybePromise<BinaryLargeValueFileRow<TApp>> {
    return this.target.createFileFromBlob(app, blob, options);
  }

  loadFileAsBlob<TApp extends BinaryLargeValueFileApp<any, any>>(
    app: TApp,
    fileOrId: string | BinaryLargeValueFileRow<TApp>,
    options?: FileReadOptions,
  ): MaybePromise<Blob> {
    return this.target.loadFileAsBlob(app, fileOrId, options);
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
    reset: true,
  };
}
