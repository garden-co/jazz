export const REACT_NATIVE_SQLITE_STORAGE_UNIMPLEMENTED_ERROR =
  "React Native SQLite storage driver is not yet implemented — see src/react-native/README.md";

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
