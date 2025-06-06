export interface SQLiteDatabaseDriverAsync {
  initialize(): Promise<void>;
  run(sql: string, params: unknown[]): Promise<void>;
  query<T>(sql: string, params: unknown[]): Promise<T[]>;
  get<T>(sql: string, params: unknown[]): Promise<T | undefined>;
  transaction(callback: () => unknown): Promise<unknown>;
  closeDb(): Promise<unknown>;
}
