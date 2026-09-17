import type { ColumnType } from "../drivers/types.js";

/** Normalize only JSON-schema object key order; semantic arrays remain ordered. */
export function columnTypeSignature(columnType: ColumnType): string {
  return JSON.stringify(columnType, function (key, value) {
    return key === "schema" && this.type === "Json" && value !== undefined
      ? canonicalJsonSchema(value)
      : value;
  });
}

// Rust's UTF-8 string order follows Unicode scalar order, unlike UTF-16 sort.
function compareUnicodeScalars(left: string, right: string): number {
  const leftPoints = Array.from(left, (character) => character.codePointAt(0)!);
  const rightPoints = Array.from(right, (character) => character.codePointAt(0)!);
  for (let index = 0; index < Math.min(leftPoints.length, rightPoints.length); index++) {
    const difference = leftPoints[index]! - rightPoints[index]!;
    if (difference !== 0) return difference;
  }
  return leftPoints.length - rightPoints.length;
}

// Rust's serde_json::Value uses sorted object keys for JSON-schema metadata.
// This does not normalize stored JSON default text, whose bytes are identity.
export function canonicalJsonSchema(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJsonSchema).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object)
      .sort(compareUnicodeScalars)
      .map((key) => `${JSON.stringify(key)}:${canonicalJsonSchema(object[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}
