export interface SQLiteDatabaseDriver {
  initialize(): void;
  run(sql: string, params: unknown[]): void;
  query<T>(sql: string, params: unknown[]): T[];
  transaction(callback: () => unknown): void;
}
