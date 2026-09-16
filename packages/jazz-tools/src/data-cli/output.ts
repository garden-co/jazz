import type { DataError } from "./errors.js";
import type { DataResult } from "./sql.js";

export type DataFormat = "table" | "json" | "jsonl";

export const DATA_FORMATS: readonly DataFormat[] = ["table", "json", "jsonl"];

export function isDataFormat(value: string): value is DataFormat {
  return (DATA_FORMATS as readonly string[]).includes(value);
}

/**
 * Machine-readable output is the default whenever stdout is not a terminal, so
 * piping or capturing a command never yields a lossy, truncated table.
 */
export function resolveFormat(requested: DataFormat | undefined, isTty: boolean): DataFormat {
  return requested ?? (isTty ? "table" : "json");
}

function json(value: unknown, pretty: boolean): string {
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

export interface FormatOptions {
  /** Table-only cell budget. 0 disables truncation. */
  maxCellWidth?: number;
}

export function formatData(
  result: DataResult,
  format: DataFormat,
  options: FormatOptions = {},
): string {
  if (format === "json")
    return `${json(result.kind === "object" ? result.value : result.rows, true)}\n`;
  if (format === "jsonl") {
    const rows = result.kind === "object" ? [result.value] : result.rows;
    return rows.map((row) => `${json(row, false)}\n`).join("");
  }
  return formatTable(result, options.maxCellWidth ?? 80);
}

function formatTable(result: DataResult, maxCellWidth: number): string {
  // Structured results (--explain, --capabilities) have no column layout.
  if (result.kind === "object") return `${json(result.value, true)}\n`;
  // Escape terminal controls and line breaks, including in column names. The
  // table view is a bounded preview; JSON/JSONL retains complete values.
  const cell = (value: unknown) => {
    const text = typeof value === "string" ? value : (json(value, false) ?? "NULL");
    const safe = text.replace(
      // eslint-disable-next-line no-control-regex -- Escape terminal control sequences in table cells.
      /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/g,
      (char) => `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`,
    );
    return maxCellWidth > 0 && safe.length > maxCellWidth
      ? `${safe.slice(0, Math.max(0, maxCellWidth - 3))}...`
      : safe;
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

export function formatError(error: DataError, format: DataFormat): string {
  if (format === "table") {
    const lines = [`${error.code}: ${error.message}`];
    if (error.hint) lines.push(`hint: ${error.hint}`);
    return `${lines.join("\n")}\n`;
  }
  return `${json(
    {
      error: {
        code: error.code,
        message: error.message,
        ...(error.hint ? { hint: error.hint } : {}),
        exitCode: error.exitCode,
      },
    },
    false,
  )}\n`;
}
