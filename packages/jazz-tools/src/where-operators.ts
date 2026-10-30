import type { ColumnDescriptor, ColumnType } from "./drivers/types.js";

export const WHERE_OPERATORS = [
  "eq",
  "ne",
  "gt",
  "gte",
  "lt",
  "lte",
  "contains",
  "in",
  "notIn",
  "isNull",
] as const;

export type WhereOperator = (typeof WHERE_OPERATORS)[number];

export interface WhereOperatorColumn {
  name: string;
  columnType: ColumnType;
  nullable: boolean;
  references?: string;
  implicitId?: boolean;
}

function operatorsForColumn(
  columnType: ColumnType,
  nullable: boolean,
  references?: string,
): WhereOperator[] {
  const operators: WhereOperator[] = [];
  if (references) {
    operators.push("eq", "ne", "in", "notIn");
  } else {
    switch (columnType.type) {
      case "Text":
        operators.push("eq", "ne", "contains", "in", "notIn");
        break;
      case "Boolean":
        operators.push("eq", "ne", "in", "notIn");
        break;
      case "Integer":
      case "BigInt":
      case "Double":
      case "Timestamp":
        operators.push("eq", "ne", "gt", "gte", "lt", "lte", "in", "notIn");
        break;
      case "Uuid":
      case "Bytea":
      case "Json":
      case "Enum":
        operators.push("eq", "ne", "in", "notIn");
        break;
      case "EnumPayload":
        // Payload enums deliberately use their dedicated `match` operator rather
        // than pretending that a whole discriminated record is comparable.
        break;
      case "Array":
        operators.push("eq", "contains", "in", "notIn");
        break;
      case "Row":
        break;
    }
  }

  if (nullable && operators.length > 0) {
    operators.push("isNull");
  }
  return operators;
}

export function getSupportedWhereOperatorsForColumn(column: WhereOperatorColumn): WhereOperator[] {
  if (column.implicitId || column.name === "id") {
    return ["eq", "ne", "in", "notIn"];
  }

  return operatorsForColumn(column.columnType, column.nullable, column.references);
}

export function getSupportedWhereOperatorsForSchemaColumn(
  fieldName: string,
  column: ColumnDescriptor | undefined,
): WhereOperator[] | undefined {
  if (fieldName === "id") {
    return ["eq", "ne", "in", "notIn"];
  }

  if (!column) {
    return undefined;
  }

  const operators = operatorsForColumn(column.column_type, column.nullable, column.references);
  return operators.length > 0 ? operators : undefined;
}
