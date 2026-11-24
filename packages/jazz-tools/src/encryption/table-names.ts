/** Prefix reserved for the encryption system tables. */
export const SYSTEM_PREFIX = "__e2ee_";

/** Prefix of the columns holding equality tokens. */
export const EQUALITY_PREFIX = "__e2ee_eq_";

export function hasSystemTables(tables: Record<string, unknown>): boolean {
  return Object.keys(tables).some((table) => table.startsWith(SYSTEM_PREFIX));
}
