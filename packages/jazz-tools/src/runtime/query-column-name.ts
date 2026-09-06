/** Remove a table qualifier without discarding a structured magic-column path. */
export function stripColumnQualifier(column: string): string {
  if (column.startsWith("$")) return column;
  const magic = column.indexOf(".$");
  if (magic >= 0) return column.slice(magic + 1);
  return column.split(".").at(-1) ?? column;
}
