export const REACT_NATIVE_SQLITE_STORAGE_UNIMPLEMENTED_ERROR =
  "React Native SQLite storage is not implemented in this alpha; the v2 runtime rejects sqliteStorage before opening a driver";

export const REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR =
  "ReactNativeDbConfig.sqliteStorage is proposal-only and cannot be used by the v2 runtime; remove sqliteStorage (memory mode remains unverified scaffolding)";

function throwUnimplemented(): never {
  throw new Error(REACT_NATIVE_SQLITE_STORAGE_UNIMPLEMENTED_ERROR);
}

export interface ReactNativeSqliteTransaction {
  execute(sql: string, params?: readonly unknown[]): Promise<void>;
  query<T = unknown>(sql: string, params?: readonly unknown[]): Promise<readonly T[]>;
}

export interface ReactNativeSqliteConnection extends ReactNativeSqliteTransaction {
  transaction<T>(
    callback: (transaction: ReactNativeSqliteTransaction) => Promise<T> | T,
  ): Promise<T>;
  close(): Promise<void>;
}

/**
 * Proposal-only storage ABI. The v2 runtime cannot consume this driver and
 * rejects persistent startup before calling it.
 *
 * @deprecated Do not implement or pass this interface until a native v2
 * ordered-KV runtime is available.
 */
export interface ReactNativeSqliteStorageDriver {
  readonly type: "react-native-sqlite";
  open(databaseName: string): Promise<ReactNativeSqliteConnection>;
  deleteDatabase(databaseName: string): Promise<void>;
}

export class UnimplementedSqliteStorageDriver implements ReactNativeSqliteStorageDriver {
  readonly type = "react-native-sqlite" as const;

  async open(_databaseName: string): Promise<ReactNativeSqliteConnection> {
    return throwUnimplemented();
  }

  async deleteDatabase(_databaseName: string): Promise<void> {
    return throwUnimplemented();
  }
}
