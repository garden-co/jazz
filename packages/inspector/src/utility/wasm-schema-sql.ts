import type {
  Column,
  OperationPolicy,
  PolicyCmpOp,
  PolicyExpr,
  PolicyOperation,
  PolicyValue,
  Schema,
  SqlType,
  Table,
  TablePolicies,
  RelExpr,
} from "jazz-tools";
interface WasmColumnTypeInteger {
  type: "Integer";
}

interface WasmColumnTypeBigInt {
  type: "BigInt";
}

interface WasmColumnTypeBoolean {
  type: "Boolean";
}

interface WasmColumnTypeText {
  type: "Text";
}

interface WasmColumnTypeEnum {
  type: "Enum";
  variants: string[];
}

interface WasmColumnTypeTimestamp {
  type: "Timestamp";
}

interface WasmColumnTypeUuid {
  type: "Uuid";
}

interface WasmColumnTypeArray {
  type: "Array";
  element: WasmColumnType;
}

interface WasmColumnTypeRow {
  type: "Row";
  columns: WasmColumnDescriptor[];
}

type WasmColumnType =
  | WasmColumnTypeInteger
  | WasmColumnTypeBigInt
  | WasmColumnTypeBoolean
  | WasmColumnTypeText
  | WasmColumnTypeEnum
  | WasmColumnTypeTimestamp
  | WasmColumnTypeUuid
  | WasmColumnTypeArray
  | WasmColumnTypeRow;

interface WasmColumnDescriptor {
  name: string;
  column_type: WasmColumnType;
  nullable: boolean;
  references?: string | null;
}

interface WasmValueInteger {
  type: "Integer";
  value: number;
}

interface WasmValueBigInt {
  type: "BigInt";
  value: number;
}

interface WasmValueBoolean {
  type: "Boolean";
  value: boolean;
}

interface WasmValueText {
  type: "Text";
  value: string;
}

interface WasmValueTimestamp {
  type: "Timestamp";
  value: number;
}

interface WasmValueUuid {
  type: "Uuid";
  value: string;
}

interface WasmValueArray {
  type: "Array";
  value: WasmValue[];
}

interface WasmValueRow {
  type: "Row";
  value: WasmValue[];
}

interface WasmValueNull {
  type: "Null";
}

type WasmValue =
  | WasmValueInteger
  | WasmValueBigInt
  | WasmValueBoolean
  | WasmValueText
  | WasmValueTimestamp
  | WasmValueUuid
  | WasmValueArray
  | WasmValueRow
  | WasmValueNull;

interface WasmPolicyValueLiteral {
  type: "Literal";
  value: WasmValue;
}

interface WasmPolicyValueSessionRef {
  type: "SessionRef";
  path: string[];
}

type WasmPolicyValue = WasmPolicyValueLiteral | WasmPolicyValueSessionRef;

type WasmCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";

type WasmPolicyOperation = "Select" | "Insert" | "Update" | "Delete";

interface WasmPolicyExprCmp {
  type: "Cmp";
  column: string;
  op: WasmCmpOp;
  value: WasmPolicyValue;
}

interface WasmPolicyExprIsNull {
  type: "IsNull";
  column: string;
}

interface WasmPolicyExprIsNotNull {
  type: "IsNotNull";
  column: string;
}

interface WasmPolicyExprIn {
  type: "In";
  column: string;
  session_path: string[];
}

interface WasmPolicyExprExists {
  type: "Exists";
  table: string;
  condition: WasmPolicyExpr;
}

interface WasmPolicyExprExistsRel {
  type: "ExistsRel";
  // Relation IR; opaque for SQL pretty-printing.
  rel: unknown;
}

interface WasmPolicyExprInherits {
  type: "Inherits";
  operation: WasmPolicyOperation;
  via_column: string;
  max_depth?: number;
}

interface WasmPolicyExprAnd {
  type: "And";
  exprs: WasmPolicyExpr[];
}

interface WasmPolicyExprOr {
  type: "Or";
  exprs: WasmPolicyExpr[];
}

interface WasmPolicyExprNot {
  type: "Not";
  expr: WasmPolicyExpr;
}

interface WasmPolicyExprTrue {
  type: "True";
}

interface WasmPolicyExprFalse {
  type: "False";
}

type WasmPolicyExpr =
  | WasmPolicyExprCmp
  | WasmPolicyExprIsNull
  | WasmPolicyExprIsNotNull
  | WasmPolicyExprIn
  | WasmPolicyExprExists
  | WasmPolicyExprExistsRel
  | WasmPolicyExprInherits
  | WasmPolicyExprAnd
  | WasmPolicyExprOr
  | WasmPolicyExprNot
  | WasmPolicyExprTrue
  | WasmPolicyExprFalse;

interface WasmOperationPolicy {
  using?: WasmPolicyExpr;
  with_check?: WasmPolicyExpr;
}

interface WasmTablePolicies {
  select?: WasmOperationPolicy;
  insert?: WasmOperationPolicy;
  update?: WasmOperationPolicy;
  delete?: WasmOperationPolicy;
}

export interface WasmTableSchemaLike {
  columns: WasmColumnDescriptor[];
  policies?: WasmTablePolicies;
}

function wasmColumnTypeToSqlType(columnType: WasmColumnType): SqlType {
  switch (columnType.type) {
    case "Integer":
      return "INTEGER";
    case "BigInt":
      return "BIGINT" as unknown as SqlType;
    case "Boolean":
      return "BOOLEAN";
    case "Text":
      return "TEXT";
    case "Timestamp":
      return "TIMESTAMP" as unknown as SqlType;
    case "Uuid":
      return "UUID";
    case "Enum": {
      return {
        kind: "ENUM",
        variants: [...columnType.variants],
      } as unknown as SqlType;
    }
    case "Array":
      return {
        kind: "ARRAY",
        element: wasmColumnTypeToSqlType(columnType.element),
      } as unknown as SqlType;
    case "Row":
      throw new Error("Row-typed columns are not supported in SQL rendering yet.");
  }
}

function wasmValueToPrimitive(value: WasmValue): unknown {
  switch (value.type) {
    case "Integer":
    case "BigInt":
    case "Timestamp":
      return value.value;
    case "Boolean":
      return value.value;
    case "Text":
    case "Uuid":
      return value.value;
    case "Array":
      return value.value.map(wasmValueToPrimitive);
    case "Row":
      throw new Error("Row default values are not supported in SQL rendering yet.");
    case "Null":
      return null;
  }
}

function wasmPolicyValueToPolicyValue(value: WasmPolicyValue): PolicyValue {
  if (value.type === "SessionRef") {
    return {
      type: "SessionRef",
      path: [...value.path],
    };
  }

  return {
    type: "Literal",
    value: wasmValueToPrimitive(value.value),
  };
}

function wasmCmpOpToPolicyCmpOp(op: WasmCmpOp): PolicyCmpOp {
  switch (op) {
    case "Eq":
      return "Eq";
    case "Ne":
      return "Ne";
    case "Lt":
      return "Lt";
    case "Le":
      return "Le";
    case "Gt":
      return "Gt";
    case "Ge":
      return "Ge";
  }
}

function wasmPolicyOperationToPolicyOperation(operation: WasmPolicyOperation): PolicyOperation {
  switch (operation) {
    case "Select":
      return "Select";
    case "Insert":
      return "Insert";
    case "Update":
      return "Update";
    case "Delete":
      return "Delete";
  }
}

function wasmPolicyExprToPolicyExpr(expr: WasmPolicyExpr): PolicyExpr {
  switch (expr.type) {
    case "Cmp":
      return {
        type: "Cmp",
        column: expr.column,
        op: wasmCmpOpToPolicyCmpOp(expr.op),
        value: wasmPolicyValueToPolicyValue(expr.value),
      };
    case "IsNull":
      return {
        type: "IsNull",
        column: expr.column,
      };
    case "IsNotNull":
      return {
        type: "IsNotNull",
        column: expr.column,
      };
    case "In":
      return {
        type: "In",
        column: expr.column,
        session_path: [...expr.session_path],
      };
    case "Exists":
      return {
        type: "Exists",
        table: expr.table,
        condition: wasmPolicyExprToPolicyExpr(expr.condition),
      };
    case "ExistsRel":
      return {
        type: "ExistsRel",
        rel: expr.rel as RelExpr,
      };
    case "Inherits": {
      return {
        type: "Inherits",
        operation: wasmPolicyOperationToPolicyOperation(expr.operation),
        via_column: expr.via_column,
        max_depth: expr.max_depth,
      };
    }
    case "And":
      return {
        type: "And",
        exprs: expr.exprs.map(wasmPolicyExprToPolicyExpr),
      };
    case "Or":
      return {
        type: "Or",
        exprs: expr.exprs.map(wasmPolicyExprToPolicyExpr),
      };
    case "Not":
      return {
        type: "Not",
        expr: wasmPolicyExprToPolicyExpr(expr.expr),
      };
    case "True":
      return { type: "True" };
    case "False":
      return { type: "False" };
  }
}

function wasmOperationPolicyToOperationPolicy(
  policy: WasmOperationPolicy | undefined,
): OperationPolicy | undefined {
  if (!policy) {
    return undefined;
  }

  const using = policy.using ? wasmPolicyExprToPolicyExpr(policy.using) : undefined;
  const with_check = policy.with_check ? wasmPolicyExprToPolicyExpr(policy.with_check) : undefined;

  if (!using && !with_check) {
    return {};
  }

  return {
    using,
    with_check,
  };
}

function wasmTablePoliciesToJazzTablePolicies(
  policies: WasmTablePolicies | undefined,
): TablePolicies | undefined {
  if (!policies) {
    return undefined;
  }

  const select = wasmOperationPolicyToOperationPolicy(policies.select);
  const insert = wasmOperationPolicyToOperationPolicy(policies.insert);
  const update = wasmOperationPolicyToOperationPolicy(policies.update);
  const deletePolicy = wasmOperationPolicyToOperationPolicy(policies.delete);

  if (!select && !insert && !update && !deletePolicy) {
    return undefined;
  }

  const result: TablePolicies = {};
  if (select) {
    result.select = select;
  }
  if (insert) {
    result.insert = insert;
  }
  if (update) {
    result.update = update;
  }
  if (deletePolicy) {
    result.delete = deletePolicy;
  }

  return result;
}

export function wasmTableToJazzSchema(tableName: string, table: WasmTableSchemaLike): Schema {
  const columns: Column[] = table.columns.map((column) => {
    const sqlType = wasmColumnTypeToSqlType(column.column_type);
    const columnDescriptor: Column = {
      name: column.name,
      sqlType,
      nullable: column.nullable,
    };

    if (column.references) {
      columnDescriptor.references = column.references;
    }

    return columnDescriptor;
  });

  const policies = wasmTablePoliciesToJazzTablePolicies(table.policies);

  const jazzTable: Table = {
    name: tableName,
    columns,
  };

  if (policies) {
    jazzTable.policies = policies;
  }

  const schema: Schema = {
    tables: [jazzTable],
  };

  return schema;
}
