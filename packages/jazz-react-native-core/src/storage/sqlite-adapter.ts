export type SQLRow = Record<string, unknown>;
export type SQLResult = {
  rows: SQLRow[];
  insertId?: number;
  rowsAffected: number;
};

export interface SQLiteAdapter {
  /**
   * Initialize the database with required tables and indexes
   */
  initialize(): Promise<void>;

  /**
   * Execute an SQL statement asynchronously
   * @param sql The SQL query text
   * @param params The parameters to bind to the SQL statement
   * @returns A promise with rows and optional insertId
   */
  execute(sql: string, params?: unknown[]): Promise<SQLResult>;

  /**
   * Optionally run an SQL statement synchronously, if supported
   * @param sql The SQL query text
   * @param params The parameters to bind to the SQL statement
   * @returns The resulting rows
   */
  executeSync?(sql: string, params?: unknown[]): { rows: SQLRow[] };

  /**
   * Run a set of operations inside a transaction
   * @param callback A callback that returns a void promise
   */
  transaction(callback: () => Promise<void>): Promise<void>;
}
