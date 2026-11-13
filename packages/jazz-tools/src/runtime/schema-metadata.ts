import type { ColumnType } from "../drivers/types.js";

/** Normalize only JSON-schema object key order; semantic arrays remain ordered. */
export function columnTypeSignature(columnType: ColumnType): string {
  return JSON.stringify(columnType, function (key, value) {
    return key === "schema" && this.type === "Json" && value !== undefined
      ? canonicalJsonSchema(value)
      : value;
  });
}

/**
 * How an enum column's variants change between two column types that are
 * otherwise the same (an enum, or an array of enums at any depth), or null
 * when they differ in anything else. Order is ignored: a migration may add
 * variants anywhere in the list.
 */
export function enumVariantChange(
  source: ColumnType,
  target: ColumnType,
): { added: string[]; dropped: string[] } | null {
  if (source.type === "Enum" && target.type === "Enum") {
    return {
      added: target.variants.filter((variant) => !source.variants.includes(variant)),
      dropped: source.variants.filter((variant) => !target.variants.includes(variant)),
    };
  }
  if (source.type === "Array" && target.type === "Array") {
    return enumVariantChange(source.element, target.element);
  }
  return null;
}

/**
 * Whether `target` is `source` with enum variants added (and possibly
 * reordered) but none dropped: rows written under `source` pass unchanged.
 */
export function onlyAddsEnumVariants(source: ColumnType, target: ColumnType): boolean {
  const change = enumVariantChange(source, target);
  return change !== null && change.dropped.length === 0;
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
