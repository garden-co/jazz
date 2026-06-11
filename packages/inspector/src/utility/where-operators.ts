import type { ColumnType } from "jazz-tools";

export type WhereOperator =
  | "eq"
  | "ne"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "contains"
  | "in"
  | "isNull";

export interface WhereOperatorColumn {
  name: string;
  columnType: ColumnType;
  nullable: boolean;
  references?: string;
  implicitId?: boolean;
}

export function getSupportedWhereOperatorsForColumn(column: WhereOperatorColumn): WhereOperator[] {
  if (column.implicitId || column.name === "id") {
    return ["eq", "ne", "in"];
  }

  switch (column.columnType.type) {
    case "Text":
      return ["eq", "ne", "contains", "in"];
    case "Boolean":
      return ["eq", "ne", "in"];
    case "Integer":
    case "BigInt":
    case "Double":
      return ["eq", "ne", "gt", "gte", "lt", "lte", "in"];
    case "Timestamp":
      return ["eq", "ne", "gt", "gte", "lt", "lte", "in"];
    case "Uuid":
      if (column.references) {
        return column.nullable ? ["eq", "ne", "in", "isNull"] : ["eq", "ne", "in"];
      }
      return ["eq", "ne", "in"];
    case "Bytea":
      return ["eq", "ne", "in"];
    case "Json":
      return ["eq", "ne", "in"];
    case "Enum":
      return ["eq", "ne", "in"];
    case "Array":
      return ["eq", "contains", "in"];
    default:
      return [];
  }
}
