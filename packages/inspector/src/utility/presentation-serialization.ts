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

export function stringifyForPresentation(
  value: unknown,
  space?: number,
  options?: PresentationSerializationOptions,
): string {
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
