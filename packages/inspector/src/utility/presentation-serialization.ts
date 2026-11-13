import { isLargeValueSummary } from "jazz-tools/inspect";

type PresentationSerializationOptions = {
  bigint?: "string" | "raw";
};

function stringifyRawBigInts(value: unknown, space: number | undefined, level = 0): string {
  if (typeof value === "bigint") return value.toString();
  if (value === null) return "null";
  if (typeof value !== "object") return JSON.stringify(value) ?? "null";

  if (value instanceof Date) {
    return JSON.stringify(value.toJSON());
  }

  const indentation = typeof space === "number" && space > 0 ? " ".repeat(space) : "";
  const childIndentation = indentation.repeat(level + 1);
  const closingIndentation = indentation.repeat(level);

  if (Array.isArray(value)) {
    const entries = value.map((entry) => {
      if (entry === undefined || typeof entry === "function" || typeof entry === "symbol") {
        return "null";
      }
      return stringifyRawBigInts(entry, space, level + 1);
    });
    if (entries.length === 0) return "[]";
    return indentation
      ? `[\n${entries.map((entry) => `${childIndentation}${entry}`).join(",\n")}\n${closingIndentation}]`
      : `[${entries.join(",")}]`;
  }

  const entries: string[] = [];
  for (const key of Object.keys(value)) {
    const entry = (value as Record<string, unknown>)[key];
    if (entry === undefined || typeof entry === "function" || typeof entry === "symbol") {
      continue;
    }
    entries.push(
      `${JSON.stringify(key)}${indentation ? ": " : ":"}${stringifyRawBigInts(entry, space, level + 1)}`,
    );
  }
  if (entries.length === 0) return "{}";
  return indentation
    ? `{\n${entries.map((entry) => `${childIndentation}${entry}`).join(",\n")}\n${closingIndentation}}`
    : `{${entries.join(",")}}`;
}

/** A size as a person reads it: 5.2 MB, 812 KB, 90 bytes. */
function formatSize(size: number, unit: string): string {
  if (unit !== "bytes") return `${size.toLocaleString("en-US")} ${unit}`;
  if (size >= 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(1)} MB`;
  if (size >= 1024) return `${Math.round(size / 1024)} KB`;
  return `${size} bytes`;
}

export function stringifyForPresentation(
  value: unknown,
  space?: number,
  options?: PresentationSerializationOptions,
): string {
  // A value too large for the backend to list: say what it is.
  if (isLargeValueSummary(value)) {
    const { kind, size } = value.$large;
    const unit = kind === "bytes" ? "bytes" : "characters";
    return `‹large ${kind} value, ${formatSize(size, unit)}›`;
  }
  if (options?.bigint === "raw") {
    return stringifyRawBigInts(value, space);
  }

  return (
    JSON.stringify(
      value,
      (_key, candidate: unknown) =>
        typeof candidate === "bigint" ? candidate.toString() : candidate,
      space,
    ) ?? ""
  );
}
