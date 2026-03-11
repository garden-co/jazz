import type { ColumnDescriptor, ColumnType } from "jazz-tools";

export type MutationFormMode = "edit" | "insert";

export type MutationFieldReadOnlyReason = "binary" | "foreign-key" | null;

export interface MutationFormField {
  column: ColumnDescriptor;
  readOnlyReason: MutationFieldReadOnlyReason;
}

function parseBoolean(value: string): boolean | null {
  const normalized = value.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") return true;
  if (normalized === "false" || normalized === "0") return false;
  return null;
}

function isBinaryColumnType(columnType: ColumnType): boolean {
  if (columnType.type === "Bytea") return true;
  if (columnType.type === "Array") return isBinaryColumnType(columnType.element);
  return false;
}

export function getFieldReadOnlyReason(column: ColumnDescriptor): MutationFieldReadOnlyReason {
  if (column.references) return "foreign-key";
  if (isBinaryColumnType(column.column_type)) return "binary";
  return null;
}

export function buildMutationFormFields(columns: ColumnDescriptor[]): MutationFormField[] {
  return columns.map((column) => ({
    column,
    readOnlyReason: getFieldReadOnlyReason(column),
  }));
}

export function parseMutationFieldValue(columnType: ColumnType, valueText: string): unknown {
  const trimmed = valueText.trim();

  switch (columnType.type) {
    case "Boolean": {
      const parsed = parseBoolean(trimmed);
      if (parsed === null) {
        throw new Error('Boolean values must be "true" or "false".');
      }
      return parsed;
    }
    case "Integer":
    case "BigInt": {
      const parsed = Number(trimmed);
      if (!Number.isInteger(parsed)) {
        throw new Error("Value must be an integer.");
      }
      return parsed;
    }
    case "Double": {
      const parsed = Number(trimmed);
      if (!Number.isFinite(parsed)) {
        throw new Error("Value must be a finite number.");
      }
      return parsed;
    }
    case "Timestamp": {
      if (trimmed.length === 0) {
        throw new Error("Timestamp is required.");
      }
      const asNumber = Number(trimmed);
      if (Number.isFinite(asNumber)) return asNumber;
      const asDate = Date.parse(trimmed);
      if (Number.isFinite(asDate)) return asDate;
      throw new Error("Timestamp must be milliseconds or an ISO date string.");
    }
    case "Json": {
      if (trimmed.length === 0) {
        throw new Error("JSON value is required.");
      }
      try {
        return JSON.parse(trimmed) as unknown;
      } catch {
        throw new Error("JSON value is invalid.");
      }
    }
    case "Bytea":
      throw new Error("Binary fields are read-only in the inspector.");
    case "Array": {
      try {
        const parsed = JSON.parse(trimmed) as unknown;
        if (!Array.isArray(parsed)) {
          throw new Error();
        }
        return parsed;
      } catch {
        throw new Error("Array must be valid JSON array.");
      }
    }
    case "Row": {
      try {
        const parsed = JSON.parse(trimmed) as unknown;
        if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
          throw new Error();
        }
        return parsed;
      } catch {
        throw new Error("Row value must be valid JSON object.");
      }
    }
    case "Enum":
      if (!columnType.variants.includes(valueText)) {
        throw new Error(`Expected one of: ${columnType.variants.join(", ")}`);
      }
      return valueText;
    case "Text":
    case "Uuid":
    default:
      return valueText;
  }
}

export function formatMutationFieldValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Uint8Array) return `(${value.length} bytes)`;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
