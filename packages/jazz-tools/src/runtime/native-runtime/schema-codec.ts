import type {
  ColumnDescriptor,
  ColumnType,
  PolicyExpr,
  PolicyValue,
  TablePolicies,
  Value,
  WasmSchema,
} from "../../drivers/types.js";
import { PostcardWriter, writeValueType, type ValueType } from "./native-codec.js";

const OUTER_ROW_SESSION_PREFIX = "__jazz_outer_row";

type PolicyOperandValue = PolicyValue | { type: "OuterRowRef"; column: string };

type PolicyQueryShape = {
  filters: PolicyExpr[];
  joins: PolicyJoin[];
};

type PolicyJoin = {
  table: string;
  onColumn: string;
  target: "Column" | "RowId";
  sourceColumn?: string;
  sourceLookup?: {
    table: string;
    rowIdSourceColumn: string;
    valueColumn: string;
  };
  filters: PolicyExpr[];
  nestedJoins?: PolicyJoin[];
};

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
    writePolicy(table, schema, tableName, definition.policies?.select?.using);
    writePolicy(table, schema, tableName, writePolicyExpr(definition.policies));
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

function writePolicy(
  writer: PostcardWriter,
  schema: WasmSchema,
  table: string,
  expr: PolicyExpr | undefined,
): void {
  if (!expr) {
    writer.none();
    return;
  }

  writer.some((query) => {
    writePolicyQuery(query, schema, table, expr);
  });
}

function writePolicyQuery(
  writer: PostcardWriter,
  schema: WasmSchema,
  table: string,
  expr: PolicyExpr,
): void {
  const alternatives = policyExprToAlternatives(schema, table, expr);
  const query =
    alternatives.length === 1
      ? alternatives[0]!
      : policyExprToQueryShape(schema, table, { type: "False" });
  writer.string(table);
  writer.vec(
    (filter, index) => writePolicyPredicate(filter, query.filters[index]!),
    query.filters.length,
  );
  writer.vec((join, index) => writePolicyJoin(join, query.joins[index]!), query.joins.length);
  writer.vec(
    (branch, index) => writePolicyBranch(branch, alternatives[index]!),
    alternatives.length === 1 ? 0 : alternatives.length,
  );
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
      writePolicyOperand(writer, policyOperandValue(expr.value));
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
    case "Contains":
      writer.u64(10); // Predicate::Contains
      writer.u64(0); // Operand::Column
      writer.string(expr.column);
      writePolicyOperand(writer, policyOperandValue(expr.value));
      return;
    case "InList":
      writer.u64(5); // Predicate::In
      writer.u64(0); // Operand::Column
      writer.string(expr.column);
      writer.vec(
        (operand, index) => writePolicyOperand(operand, policyOperandValue(expr.values[index]!)),
        expr.values.length,
      );
      return;
    default:
      throw new Error(`Core runtime schema policies do not support ${expr.type} yet.`);
  }
}

function writePolicyOperand(writer: PostcardWriter, value: PolicyOperandValue): void {
  if (value.type === "OuterRowRef") {
    writer.u64(0); // Operand::Column
    writer.string(value.column);
    return;
  }

  if (value.type === "SessionRef") {
    const claim = sessionRefClaimName(value.path);
    writer.u64(2); // Operand::Claim
    writer.string(claim);
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

function writePolicyJoin(writer: PostcardWriter, join: PolicyJoin): void {
  writer.string(join.table);
  writer.string(join.onColumn);
  writer.u64(join.target === "Column" ? 0 : 1);
  if (join.sourceColumn == null) {
    writer.none();
  } else {
    writer.some((sourceColumn) => sourceColumn.string(join.sourceColumn!));
  }
  if (join.sourceLookup == null) {
    writer.none();
  } else {
    writer.some((lookup) => {
      lookup.string(join.sourceLookup!.table);
      lookup.string(join.sourceLookup!.rowIdSourceColumn);
      lookup.string(join.sourceLookup!.valueColumn);
    });
  }
  writer.vec(
    (filter, index) => writePolicyPredicate(filter, join.filters[index]!),
    join.filters.length,
  );
  writer.vec(
    (nestedJoin, index) => writePolicyJoin(nestedJoin, join.nestedJoins![index]!),
    join.nestedJoins?.length ?? 0,
  );
}

function writePolicyBranch(writer: PostcardWriter, branch: PolicyQueryShape): void {
  writer.vec(
    (filter, index) => writePolicyPredicate(filter, branch.filters[index]!),
    branch.filters.length,
  );
  writer.vec((join, index) => writePolicyJoin(join, branch.joins[index]!), branch.joins.length);
  writer.vec(() => undefined, 0);
}

function policyExprToAlternatives(
  schema: WasmSchema,
  table: string,
  expr: PolicyExpr,
): PolicyQueryShape[] {
  if (expr.type === "Inherits" && expr.operation === "Select" && expr.max_depth == null) {
    return inheritedSelectPolicyToQueryShapes(schema, table, expr.via_column);
  }
  if (expr.type === "InheritsReferencing" && expr.max_depth == null) {
    return inheritedReferencingPolicyToQueryShapes(
      schema,
      expr.operation,
      expr.source_table,
      expr.via_column,
    );
  }
  if (expr.type === "Or") {
    return expr.exprs.flatMap((child) => policyExprToAlternatives(schema, table, child));
  }
  if (expr.type !== "And") {
    return [policyExprToQueryShape(schema, table, expr)];
  }
  return expr.exprs.reduce<PolicyQueryShape[]>(
    (alternatives, child) => {
      const childAlternatives = policyExprToAlternatives(schema, table, child);
      return alternatives.flatMap((left) =>
        childAlternatives.map((right) => ({
          filters: [...left.filters, ...right.filters],
          joins: [...left.joins, ...right.joins],
        })),
      );
    },
    [{ filters: [], joins: [] }],
  );
}

function policyExprToQueryShape(
  schema: WasmSchema,
  table: string,
  expr: PolicyExpr,
): PolicyQueryShape {
  if (expr.type === "True") return { filters: [], joins: [] };
  if (expr.type === "False") return { filters: [expr], joins: [] };
  if (expr.type === "And") {
    return expr.exprs.reduce<PolicyQueryShape>(
      (shape, child) => {
        const childShape = policyExprToQueryShape(schema, table, child);
        shape.filters.push(...childShape.filters);
        shape.joins.push(...childShape.joins);
        return shape;
      },
      { filters: [], joins: [] },
    );
  }
  if (expr.type === "Exists") {
    return { filters: [], joins: [policyExistsToJoin(schema, expr)] };
  }
  if (expr.type === "Inherits" && expr.operation === "Select" && expr.max_depth == null) {
    const alternatives = inheritedSelectPolicyToQueryShapes(schema, table, expr.via_column);
    if (alternatives.length !== 1) {
      throw new Error("Core runtime schema Inherits policy alternatives must be branch-lowered.");
    }
    return alternatives[0]!;
  }
  if (expr.type === "InheritsReferencing" && expr.max_depth == null) {
    const alternatives = inheritedReferencingPolicyToQueryShapes(
      schema,
      expr.operation,
      expr.source_table,
      expr.via_column,
    );
    if (alternatives.length !== 1) {
      throw new Error(
        "Core runtime schema InheritsReferencing policy alternatives must be branch-lowered.",
      );
    }
    return alternatives[0]!;
  }
  return { filters: [expr], joins: [] };
}

function policyExistsToJoin(
  schema: WasmSchema,
  expr: Extract<PolicyExpr, { type: "Exists" }>,
): PolicyJoin {
  const condition = policyExprToQueryShape(schema, expr.table, expr.condition);
  if (condition.joins.length > 0) {
    throw new Error("Core runtime schema policies do not support nested Exists policies yet.");
  }

  const filters = [...condition.filters];
  const correlationIndex = filters.findIndex(isOuterRowEquality);
  if (correlationIndex === -1) {
    throw new Error("Core runtime schema Exists policies must include an outer row equality.");
  }
  const [correlation] = filters.splice(correlationIndex, 1);
  if (!correlation || correlation.type !== "Cmp" || correlation.op !== "Eq") {
    throw new Error(
      "Core runtime schema Exists policies must use equality for outer row correlation.",
    );
  }
  const outer = policyOperandValue(correlation.value);
  if (outer.type !== "OuterRowRef") {
    throw new Error(
      "Core runtime schema Exists policies must correlate to an outer row reference.",
    );
  }

  return {
    table: expr.table,
    onColumn: correlation.column,
    target: correlation.column === "id" && outer.column !== "id" ? "RowId" : "Column",
    sourceColumn: outer.column === "id" ? undefined : outer.column,
    filters,
  };
}

function inheritedSelectPolicyToQueryShapes(
  schema: WasmSchema,
  table: string,
  viaColumn: string,
): PolicyQueryShape[] {
  const parentTable = schema[table]?.columns.find(
    (column) => column.name === viaColumn,
  )?.references;
  if (!parentTable) {
    throw new Error(
      `Core runtime schema Inherits policy ${table}.${viaColumn} is not a reference.`,
    );
  }
  const parentPolicy = schema[parentTable]?.policies?.select?.using;
  if (!parentPolicy) {
    throw new Error(
      `Core runtime schema Inherits policy ${table}.${viaColumn} references ${parentTable} without a select policy.`,
    );
  }

  const parentAlternatives = policyExprToAlternatives(schema, parentTable, parentPolicy);
  return parentAlternatives.map((branch) =>
    inheritedParentBranchToChildQuery(parentTable, viaColumn, branch),
  );
}

function inheritedParentBranchToChildQuery(
  parentTable: string,
  viaColumn: string,
  branch: PolicyQueryShape,
): PolicyQueryShape {
  const joins: PolicyJoin[] = [];
  if (branch.filters.length > 0 && !isFalseFilterSet(branch.filters)) {
    joins.push({
      table: parentTable,
      onColumn: "id",
      target: "RowId",
      sourceColumn: viaColumn,
      filters: branch.filters,
    });
  }
  for (const join of branch.joins) {
    joins.push({
      ...join,
      sourceColumn: join.sourceColumn,
      sourceLookup:
        join.sourceColumn == null
          ? undefined
          : {
              table: parentTable,
              rowIdSourceColumn: viaColumn,
              valueColumn: join.sourceColumn,
            },
    });
  }
  return { filters: [], joins };
}

function inheritedReferencingPolicyToQueryShapes(
  schema: WasmSchema,
  operation: "Select" | "Insert" | "Update" | "Delete",
  sourceTable: string,
  viaColumn: string,
): PolicyQueryShape[] {
  const sourceColumn = schema[sourceTable]?.columns.find((column) => column.name === viaColumn);
  if (!sourceColumn?.references) {
    throw new Error(
      `Core runtime schema InheritsReferencing policy ${sourceTable}.${viaColumn} is not a reference.`,
    );
  }
  const sourcePolicy = sourceOperationPolicy(schema[sourceTable]?.policies, operation) ?? {
    type: "True" as const,
  };
  return policyExprToAlternatives(schema, sourceTable, sourcePolicy).map((branch) => ({
    filters: [],
    joins: [
      {
        table: sourceTable,
        onColumn: viaColumn,
        target: "Column",
        filters: branch.filters,
        nestedJoins: branch.joins,
      },
    ],
  }));
}

function sourceOperationPolicy(
  policies: TablePolicies | undefined,
  operation: "Select" | "Insert" | "Update" | "Delete",
): PolicyExpr | undefined {
  switch (operation) {
    case "Select":
      return policies?.select?.using;
    case "Insert":
      return policies?.insert?.with_check;
    case "Update":
      return policies?.update?.using ?? policies?.update?.with_check;
    case "Delete":
      return policies?.delete?.using;
  }
}

function isFalseFilterSet(filters: PolicyExpr[]): boolean {
  return filters.length === 1 && filters[0]?.type === "False";
}

function isOuterRowEquality(expr: PolicyExpr): boolean {
  return (
    expr.type === "Cmp" && expr.op === "Eq" && policyOperandValue(expr.value).type === "OuterRowRef"
  );
}

function policyOperandValue(value: PolicyValue): PolicyOperandValue {
  if (value.type === "SessionRef" && value.path[0] === OUTER_ROW_SESSION_PREFIX) {
    const column = value.path[1];
    if (!column || value.path.length !== 2) {
      throw new Error(`Invalid outer row reference ${value.path.join(".")}.`);
    }
    return { type: "OuterRowRef", column };
  }
  return value;
}

function sessionRefClaimName(path: string[]): string {
  if (path.length === 1) {
    if (path[0] === "userId") return "user_id";
    return path[0]!;
  }
  if (path.length === 2 && path[0] === "claims") {
    return path[1]!;
  }
  throw new Error(
    `Core runtime schema policies only support session claims, got ${path.join(".")}.`,
  );
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
