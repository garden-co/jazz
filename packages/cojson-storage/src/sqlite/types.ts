export interface SQLiteDatabaseDriver {
  run(sql: string, params: unknown[]): void;
  get<T>(sql: string, params: unknown[]): T | undefined;
  query<T>(sql: string, params: unknown[]): T[];
  transaction(callback: () => unknown): void;
  closeDb(): void;
}
