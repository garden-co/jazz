import type { ColumnDescriptor, ColumnType } from "./drivers/types.js";

export type WhereOperator =
  | "eq"
  | "ne"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "contains"
  | "in"
  | "notIn"
  | "isNull";

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
  if (references) {
    return nullable ? ["eq", "ne", "in", "notIn", "isNull"] : ["eq", "ne", "in", "notIn"];
  }

  switch (columnType.type) {
    case "Text":
      return ["eq", "ne", "contains", "in", "notIn"];
    case "Boolean":
      return ["eq", "ne", "in", "notIn"];
    case "Integer":
    case "BigInt":
    case "Double":
      return ["eq", "ne", "gt", "gte", "lt", "lte", "in", "notIn"];
    case "Timestamp":
      return ["eq", "ne", "gt", "gte", "lt", "lte", "in", "notIn"];
    case "Uuid":
      return ["eq", "ne", "in", "notIn"];
    case "Bytea":
      return ["eq", "ne", "in", "notIn"];
    case "Json":
      return ["eq", "ne", "in", "notIn"];
    case "Enum":
      return ["eq", "ne", "in", "notIn"];
    case "Array":
      return ["eq", "contains", "in", "notIn"];
    case "Row":
      return [];
  }
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
