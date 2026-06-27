import type {
  ColumnDescriptor,
  ColumnType,
  PolicyExpr,
  PolicyValue,
  TablePolicies,
  Value,
  WasmSchema,
} from "../../drivers/types.js";
import { PostcardWriter, writeValueType, type ValueType } from "./core-codec.js";

export function encodeSchema(schema: WasmSchema): Uint8Array {
  const tables = Object.entries(schema);
  const writer = new PostcardWriter();
  writer.vec((table, index) => {
    const [tableName, definition] = tables[index]!;
    table.string(tableName);
    table.vec((column, columnIndex) => {
      const columnSpec = definition.columns[columnIndex]!;
      column.string(columnSpec.name);
      writeValueType(column, columnValueType(columnSpec));
      column.none();
    }, definition.columns.length);
    table.map(definition.columns.filter((column) => column.references).length);
    for (const column of definition.columns) {
      if (column.references) {
        table.string(column.name);
        table.string(column.references);
      }
    }
    writePolicy(table, tableName, definition.policies?.select?.using);
    writePolicy(table, tableName, writePolicyExpr(definition.policies));
    table.set(0);
    table.map(0);
  }, tables.length);
  writer.none();
  writer.none();
  return writer.finish();
}

export function columnValueType(column: ColumnDescriptor): ValueType {
  const valueType = columnTypeToValueType(column.column_type);
  return column.nullable ? { tag: 12, inner: valueType } : valueType;
}

export function columnTypeToValueType(type: ColumnType): ValueType {
  switch (type.type) {
    case "Boolean":
      return { tag: 5 };
    case "Integer":
      return { tag: 2 };
    case "BigInt":
    case "Timestamp":
      return { tag: 3 };
    case "Double":
      return { tag: 4 };
    case "Text":
    case "Json":
    case "Enum":
      return { tag: 6 };
    case "Bytea":
      return { tag: 7 };
    case "Uuid":
      return { tag: 8 };
    case "Array":
      return { tag: 11, inner: columnTypeToValueType(type.element) };
    case "Row":
      throw new Error("Core runtime does not encode nested row columns yet");
  }
}

function writePolicy(writer: PostcardWriter, table: string, expr: PolicyExpr | undefined): void {
  if (!expr) {
    writer.none();
    return;
  }

  writer.some((query) => {
    writePolicyQuery(query, table, expr);
  });
}

function writePolicyQuery(writer: PostcardWriter, table: string, expr: PolicyExpr): void {
  writer.string(table);
  writer.vec((filter) => writePolicyPredicate(filter, expr), 1);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec(() => undefined, 0);
  writer.none();
  writer.none();
  writer.u64(0);
}

function writePolicyPredicate(writer: PostcardWriter, expr: PolicyExpr): void {
  switch (expr.type) {
    case "True":
      writer.u64(0); // Predicate::All
      writer.vec(() => undefined, 0);
      return;
    case "False":
      writer.u64(1); // Predicate::Any
      writer.vec(() => undefined, 0);
      return;
    case "And":
      writer.u64(0); // Predicate::All
      writer.vec(
        (child, index) => writePolicyPredicate(child, expr.exprs[index]!),
        expr.exprs.length,
      );
      return;
    case "Or":
      writer.u64(1); // Predicate::Any
      writer.vec(
        (child, index) => writePolicyPredicate(child, expr.exprs[index]!),
        expr.exprs.length,
      );
      return;
    case "Not":
      writer.u64(2); // Predicate::Not
      writePolicyPredicate(writer, expr.expr);
      return;
    case "Cmp":
      writer.u64(policyPredicateOpTag(expr.op));
      writer.u64(0); // Operand::Column
      writer.string(expr.column);
      writePolicyOperand(writer, expr.value);
      return;
    case "IsNull":
      writer.u64(11); // Predicate::IsNull
      writer.u64(0); // Operand::Column
      writer.string(expr.column);
      return;
    case "IsNotNull":
      writer.u64(2); // Predicate::Not
      writer.u64(11); // Predicate::IsNull
      writer.u64(0); // Operand::Column
      writer.string(expr.column);
      return;
    default:
      throw new Error(`Core runtime schema policies do not support ${expr.type} yet.`);
  }
}

function writePolicyOperand(writer: PostcardWriter, value: PolicyValue): void {
  if (value.type === "SessionRef") {
    if (value.path.length !== 1 || value.path[0] !== "user_id") {
      throw new Error(
        `Core runtime schema policies only support session.user_id references, got ${value.path.join(".")}.`,
      );
    }
    writer.u64(2); // Operand::Claim
    writer.string("user_id");
    return;
  }

  writer.u64(3); // Operand::Literal
  writePolicyLiteral(writer, value.value);
}

function writePolicyLiteral(writer: PostcardWriter, value: Value): void {
  switch (value.type) {
    case "Null":
      writer.u64(12); // groove::records::Value::Nullable
      writer.none();
      return;
    case "Boolean":
      writer.u64(5); // groove::records::Value::Bool
      writer.bool(value.value);
      return;
    case "Text":
      writer.u64(6); // groove::records::Value::String
      writer.string(value.value);
      return;
    case "Uuid":
      writer.u64(8); // groove::records::Value::Uuid
      writer.bytes(uuidBytes(value.value));
      return;
    default:
      throw new Error(`Core runtime schema policies do not support ${value.type} literals yet.`);
  }
}

function policyPredicateOpTag(op: "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge"): number {
  switch (op) {
    case "Eq":
      return 3;
    case "Ne":
      return 4;
    case "Gt":
      return 6;
    case "Ge":
      return 7;
    case "Lt":
      return 8;
    case "Le":
      return 9;
  }
}

function writePolicyExpr(policies: TablePolicies | undefined): PolicyExpr | undefined {
  return (
    policies?.insert?.with_check ??
    policies?.update?.with_check ??
    policies?.update?.using ??
    policies?.delete?.using
  );
}

function uuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let index = 0; index < 16; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}
