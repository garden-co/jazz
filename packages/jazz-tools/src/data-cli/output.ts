import type { DataResult } from "./sql.js";

export type DataFormat = "table" | "json" | "jsonl";
function json(value: unknown, pretty = false): string {
  return JSON.stringify(
    value,
    (_key, item) =>
      typeof item === "bigint"
        ? item.toString()
        : item instanceof Uint8Array
          ? Array.from(item)
          : item,
    pretty ? 2 : undefined,
  );
}

export function formatData(result: DataResult, format: DataFormat): string {
  if (format === "json") return `${json(result.rows, true)}\n`;
  if (format === "jsonl") return result.rows.map((row) => `${json(row)}\n`).join("");
  // Escape terminal controls and line breaks, including in column names. Table
  // output is a bounded preview; JSON/JSONL retains complete values.
  const cell = (value: unknown) => {
    const text = typeof value === "string" ? value : (json(value) ?? "NULL");
    const safe = text.replace(
      // eslint-disable-next-line no-control-regex -- Escape terminal control sequences in table cells.
      /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/g,
      (char) => `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`,
    );
    return safe.length > 80 ? `${safe.slice(0, 77)}...` : safe;
  };
  const rows = [
    result.columns.map(cell),
    ...result.rows.map((row) => result.columns.map((name) => cell(row[name]))),
  ];
  const widths = result.columns.map((_name, index) =>
    rows.reduce((width, row) => Math.max(width, row[index]!.length), 0),
  );
  const line = (row: string[]) =>
    row
      .map((value, index) => value.padEnd(widths[index]!))
      .join(" | ")
      .trimEnd();
  return [
    line(rows[0]!),
    widths.map((width) => "-".repeat(width)).join("-+-"),
    ...rows.slice(1).map(line),
    `(${result.rows.length} row${result.rows.length === 1 ? "" : "s"})`,
    "",
  ].join("\n");
}
