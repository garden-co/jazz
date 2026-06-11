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
      return ["eq"];
    case "Integer":
    case "BigInt":
    case "Double":
      return ["eq", "ne", "gt", "gte", "lt", "lte"];
    case "Timestamp":
      return ["eq", "gt", "gte", "lt", "lte"];
    case "Uuid":
      if (column.references) {
        return column.nullable ? ["eq", "ne", "in", "isNull"] : ["eq", "ne", "in"];
      }
      return ["eq", "ne", "in"];
    case "Bytea":
      return ["eq", "ne"];
    case "Json":
      return ["eq", "ne", "in"];
    case "Enum":
      return ["eq", "ne", "in"];
    case "Array":
      return ["eq", "contains"];
    default:
      return [];
  }
}
