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
type InternalPolicyValue = PolicyValue | { type: "FrontierRowRef" };
type LoweredPolicyExpr = PolicyExpr & { scope?: string };

type PolicyQueryShape = {
  filters: PolicyExpr[];
  joins: PolicyJoin[];
  reachable: PolicyReachable[];
  inherits: PolicyInherits[];
};

type PolicyInherits = {
  parentColumn: string;
};

type PolicyJoin = {
  table: string;
  scope?: string;
  onColumn: string;
  target: "Column" | "RowId";
  sourceColumn?: string;
  sourceLookup?: {
    table: string;
    rowIdSourceColumn: string;
    valueColumn: string;
  };
  correlatedFilters?: {
    column: string;
    sourceColumn: string;
  }[];
  filters: PolicyExpr[];
  nestedJoins?: PolicyJoin[];
};

type PolicyReachable = {
  accessTable: string;
  accessRowColumn: string;
  accessTeamColumn: string;
  accessTeamTarget: "Column" | "RowId";
  from: PolicyOperandValue;
  accessFilters: PolicyExpr[];
  edgeTable: string;
  edgeMemberColumn: string;
  edgeParentColumn: string;
  edgeFilters: PolicyExpr[];
  maxDepth: number;
  seed?: PolicyReachableSeed;
};

type PolicyReachableSeed = {
  table: string;
  userColumn?: string;
  userClaim?: string;
  teamColumn: string;
  filters: PolicyExpr[];
};

type PendingPolicyReachable = Omit<
  PolicyReachable,
  "accessTable" | "accessRowColumn" | "accessTeamColumn" | "accessTeamTarget" | "accessFilters"
>;

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
      writeLargeValueKind(column, columnSpec);
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
    writePolicy(table, schema, tableName, definition.policies?.insert?.with_check);
    writePolicy(table, schema, tableName, definition.policies?.update?.using);
    writePolicy(table, schema, tableName, definition.policies?.update?.with_check);
    writePolicy(table, schema, tableName, definition.policies?.delete?.using);
    writeIndexedColumns(table, definition.indexed_columns);
    writeMergeStrategies(table, definition.columns);
  }, tables.length);
  writer.none();
  writer.none();
  return writer.finish();
}

export function columnValueType(column: ColumnDescriptor): ValueType {
  const valueType = columnTypeToValueType(column.column_type);
  return column.nullable ? { tag: 12, inner: valueType } : valueType;
}

function writeLargeValueKind(writer: PostcardWriter, column: ColumnDescriptor) {
  const largeValue = column.large_value;
  if (largeValue == null) {
    writer.none();
    return;
  }
  if (column.column_type.type !== "Bytea") {
    throw new Error(`large_value is only supported on Bytea columns: ${column.name}`);
  }
  writer.some((kind) => kind.enumUnit(largeValue === "Text" ? 0 : 1));
}

function writeIndexedColumns(writer: PostcardWriter, indexedColumns: string[] | undefined): void {
  const columns = [...(indexedColumns ?? [])].sort();
  writer.set(columns.length);
  for (const column of columns) {
    writer.string(column);
  }
}

function writeMergeStrategies(writer: PostcardWriter, columns: ColumnDescriptor[]): void {
  const mergeColumns = columns
    .filter((column) => column.merge_strategy != null)
    .sort((left, right) => left.name.localeCompare(right.name));
  writer.map(mergeColumns.length);
  for (const column of mergeColumns) {
    writer.string(column.name);
    switch (column.merge_strategy) {
      case "Counter":
        writer.enumUnit(1);
        break;
      case "GSet":
        throw new Error("Core runtime does not encode GSet merge strategies yet");
      default:
        throw new Error(`Unsupported merge strategy for ${column.name}`);
    }
  }
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
  const alternatives = policyExprToAlternatives(schema, table, expr).map((alternative) =>
    normalizePolicyQueryShape(schema, alternative),
  );
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
  writer.vec(
    (reachable, index) => writePolicyReachable(reachable, query.reachable[index]!),
    query.reachable.length,
  );
  writer.vec(
    (inherits, index) => writePolicyInherits(inherits, query.inherits[index]!),
    query.inherits.length,
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
    case "In":
      writer.u64(10); // Predicate::Contains
      writePolicyOperand(writer, { type: "SessionRef", path: expr.session_path });
      writer.u64(0); // Operand::Column
      writer.string(expr.column);
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
    case "Integer":
      writer.u64(2); // groove::records::Value::I64
      writer.u64(value.value);
      return;
    case "BigInt":
    case "Timestamp":
      writer.u64(3); // groove::records::Value::U64
      writer.u64(value.value);
      return;
    case "Double":
      writer.u64(4); // groove::records::Value::F64
      writer.bytes(f64Bytes(value.value), false);
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
  writer.vec((correlation, index) => {
    const correlatedFilter = join.correlatedFilters![index]!;
    correlation.string(correlatedFilter.column);
    correlation.string(correlatedFilter.sourceColumn);
  }, join.correlatedFilters?.length ?? 0);
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
  writer.vec(
    (reachable, index) => writePolicyReachable(reachable, branch.reachable[index]!),
    branch.reachable.length,
  );
  writer.vec(
    (inherits, index) => writePolicyInherits(inherits, branch.inherits[index]!),
    branch.inherits.length,
  );
}

function writePolicyInherits(writer: PostcardWriter, inherits: PolicyInherits): void {
  writer.string(inherits.parentColumn);
}

function writePolicyReachable(writer: PostcardWriter, reachable: PolicyReachable): void {
  const accessFilters = uniquePolicyFilters(reachable.accessFilters);
  const edgeFilters = uniquePolicyFilters(reachable.edgeFilters);
  writer.string(reachable.accessTable);
  writer.string(reachable.accessRowColumn);
  writer.string(reachable.accessTeamColumn);
  writer.u64(reachable.accessTeamTarget === "Column" ? 0 : 1);
  writePolicyOperand(writer, reachable.from);
  writer.vec(
    (filter, index) => writePolicyPredicate(filter, accessFilters[index]!),
    accessFilters.length,
  );
  writer.string(reachable.edgeTable);
  writer.string(reachable.edgeMemberColumn);
  writer.string(reachable.edgeParentColumn);
  writer.vec(
    (filter, index) => writePolicyPredicate(filter, edgeFilters[index]!),
    edgeFilters.length,
  );
  // RecursionBound::MaxDepth(maxDepth)
  writer.u64(1);
  writer.u64(reachable.maxDepth);
  if (reachable.seed) {
    writer.some((seed) => {
      seed.string(reachable.seed!.table);
      if (reachable.seed!.userColumn == null) {
        seed.none();
      } else {
        seed.some((userColumn) => userColumn.string(reachable.seed!.userColumn!));
      }
      if (reachable.seed!.userClaim == null) {
        seed.none();
      } else {
        seed.some((userClaim) => userClaim.string(reachable.seed!.userClaim!));
      }
      seed.string(reachable.seed!.teamColumn);
      seed.vec(
        (filter, index) => writePolicyPredicate(filter, reachable.seed!.filters[index]!),
        reachable.seed!.filters.length,
      );
    });
  } else {
    writer.none();
  }
}

function policyExprToAlternatives(
  schema: WasmSchema,
  table: string,
  expr: PolicyExpr,
): PolicyQueryShape[] {
  if (expr.type === "InheritsReferencing") {
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
          reachable: [...left.reachable, ...right.reachable],
          inherits: [...left.inherits, ...right.inherits],
        })),
      );
    },
    [{ filters: [], joins: [], reachable: [], inherits: [] }],
  );
}

function policyExprToQueryShape(
  schema: WasmSchema,
  table: string,
  expr: PolicyExpr,
): PolicyQueryShape {
  if (expr.type === "True") return { filters: [], joins: [], reachable: [], inherits: [] };
  if (expr.type === "False") {
    return { filters: [expr], joins: [], reachable: [], inherits: [] };
  }
  if (expr.type === "And") {
    return expr.exprs.reduce<PolicyQueryShape>(
      (shape, child) => {
        const childShape = policyExprToQueryShape(schema, table, child);
        shape.filters.push(...childShape.filters);
        shape.joins.push(...childShape.joins);
        shape.reachable.push(...childShape.reachable);
        shape.inherits.push(...childShape.inherits);
        return shape;
      },
      { filters: [], joins: [], reachable: [], inherits: [] },
    );
  }
  if (expr.type === "Exists") {
    return { filters: [], joins: [policyExistsToJoin(schema, expr)], reachable: [], inherits: [] };
  }
  if (expr.type === "ExistsRel") {
    return policyExistsRelToQueryShape(schema, table, expr.rel);
  }
  if (expr.type === "Inherits") {
    return {
      filters: [],
      joins: [],
      reachable: [],
      inherits: [{ parentColumn: expr.via_column }],
    };
  }
  if (expr.type === "InheritsReferencing") {
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
  return { filters: [expr], joins: [], reachable: [], inherits: [] };
}

type RelExprObject = Record<string, unknown>;

function policyExistsRelToQueryShape(
  schema: WasmSchema,
  rootTable: string,
  rel: unknown,
): PolicyQueryShape {
  const lowered = lowerExistsRel(rel);
  const join = policyExistsRelLoweredToJoin(schema, rootTable, lowered);
  return { filters: [], joins: join ? [join] : [], reachable: lowered.reachable, inherits: [] };
}

function policyExistsRelLoweredToJoin(
  schema: WasmSchema,
  rootTable: string,
  lowered: LoweredRelExpr,
): PolicyJoin | undefined {
  const filters = [...lowered.filters] as PolicyExpr[];
  let nestedJoins = lowered.joins;
  let table = lowered.table;
  let correlationIndex = filters.findIndex(isOuterRowEquality);
  if (correlationIndex === -1 && lowered.joins.length === 1) {
    const [join] = lowered.joins;
    const joinCorrelationIndex = join?.filters.findIndex(isOuterRowEquality) ?? -1;
    if (join && joinCorrelationIndex !== -1) {
      const [joinCorrelation] = join.filters.splice(joinCorrelationIndex, 1);
      if (joinCorrelation) {
        filters.push(...join.filters);
        filters.push(joinCorrelation);
        table = join.table;
        nestedJoins = join.nestedJoins ?? [];
        correlationIndex = filters.length - 1;
      }
    }
  }
  if (correlationIndex === -1) {
    const correlatedJoin = lowered.joins.find((join) => join.filters.some(isOuterRowEquality));
    if (correlatedJoin) {
      const joinCorrelationIndex = correlatedJoin.filters.findIndex(isOuterRowEquality);
      const [joinCorrelation] = correlatedJoin.filters.splice(joinCorrelationIndex, 1);
      if (joinCorrelation) {
        if (correlatedJoin.onColumn === "id" && correlatedJoin.sourceColumn != null) {
          filters.length = 0;
          filters.push(...lowered.filters, ...correlatedJoin.filters, {
            ...joinCorrelation,
            column: correlatedJoin.sourceColumn,
          } as LoweredPolicyExpr);
          table = lowered.table;
          nestedJoins = lowered.joins.filter((join) => join !== correlatedJoin);
          correlationIndex = filters.length - 1;
        } else {
          filters.length = 0;
          filters.push(...correlatedJoin.filters, joinCorrelation);
          table = correlatedJoin.table;
          nestedJoins = correlatedJoin.nestedJoins ?? [];
          correlationIndex = filters.length - 1;
        }
      }
    }
  }
  if (correlationIndex === -1) {
    throw new Error("Core runtime schema ExistsRel policies must include an outer row equality.");
  }
  const [rawCorrelation] = filters.splice(correlationIndex, 1);
  if (!rawCorrelation || rawCorrelation.type !== "Cmp" || rawCorrelation.op !== "Eq") {
    throw new Error(
      "Core runtime schema ExistsRel policies must use equality for outer row correlation.",
    );
  }
  let correlation = rawCorrelation;
  if (correlation.column === "id" && lowered.joins.length === 1) {
    const [hopJoin] = lowered.joins;
    if (hopJoin?.onColumn === "id" && hopJoin.sourceColumn != null) {
      correlation = { ...correlation, column: hopJoin.sourceColumn };
      nestedJoins = [];
    }
  }
  const outer = policyOperandValue(correlation.value);
  if (outer.type !== "OuterRowRef") {
    throw new Error(
      "Core runtime schema ExistsRel policies must correlate to an outer row reference.",
    );
  }
  for (const reachable of lowered.reachable) {
    if (reachable.accessRowColumn === "__pending_outer_row") {
      reachable.accessRowColumn = correlation.column;
      reachable.accessFilters = uniquePolicyFilters([...reachable.accessFilters, ...filters]);
    }
  }
  if (lowered.pendingReachable) {
    lowered.reachable.push({
      ...lowered.pendingReachable,
      accessTable: rootTable,
      accessRowColumn: "id",
      accessTeamColumn: outer.column,
      accessTeamTarget: outer.column === "id" ? "RowId" : "Column",
      accessFilters: [],
    });
  }
  if (lowered.reachable.length > 0) {
    for (const reachable of lowered.reachable) {
      reachable.accessFilters.push(...filters);
    }
    return undefined;
  }

  return normalizePolicyJoinTable(schema, {
    table,
    onColumn: correlation.column,
    target: correlation.column === "id" && outer.column !== "id" ? "RowId" : "Column",
    sourceColumn: outer.column,
    filters,
    nestedJoins,
  });
}

function normalizePolicyJoinTable(schema: WasmSchema, join: PolicyJoin): PolicyJoin {
  const scopedTable =
    join.scope != null &&
    schema[join.scope]?.columns.some((column) => column.name === join.onColumn)
      ? join.scope
      : undefined;
  const filterScopedTable = join.filters
    .map((filter) => (filter as LoweredPolicyExpr).scope)
    .find(
      (scope): scope is string =>
        scope != null && schema[scope]?.columns.some((column) => column.name === join.onColumn),
    );
  const tableHasOnColumn = schema[join.table]?.columns.some(
    (column) => column.name === join.onColumn,
  );
  const table = !tableHasOnColumn ? (scopedTable ?? filterScopedTable ?? join.table) : join.table;
  return {
    ...join,
    table,
    nestedJoins: join.nestedJoins?.map((nested) => normalizePolicyJoinTable(schema, nested)),
  };
}

function normalizePolicyQueryShape(schema: WasmSchema, shape: PolicyQueryShape): PolicyQueryShape {
  return {
    filters: shape.filters,
    joins: shape.joins.map((join) => normalizePolicyJoinTable(schema, join)),
    reachable: shape.reachable,
    inherits: shape.inherits,
  };
}

function lowerExistsRel(rel: unknown): {
  table: string;
  filters: LoweredPolicyExpr[];
  joins: PolicyJoin[];
  reachable: PolicyReachable[];
  inherits: PolicyInherits[];
  pendingReachable?: PendingPolicyReachable;
} {
  if (!isRecord(rel)) throw new Error("Core runtime schema ExistsRel relation must be an object.");

  if (isRecord(rel.TableScan)) {
    const table = rel.TableScan.table;
    if (typeof table !== "string") {
      throw new Error("Core runtime schema ExistsRel TableScan is missing table.");
    }
    return { table, filters: [], joins: [], reachable: [], inherits: [] };
  }

  if (isRecord(rel.Filter)) {
    const input = lowerExistsRel(rel.Filter.input);
    appendRelFilters(input, relPredicateToPolicyExprs(rel.Filter.predicate));
    return input;
  }

  if (isRecord(rel.Project)) {
    return lowerExistsRel(rel.Project.input);
  }

  if (isRecord(rel.Gather)) {
    return lowerGatherRel(rel.Gather);
  }

  if (isRecord(rel.Join)) {
    const left = lowerExistsRel(rel.Join.left);
    const right = lowerExistsRel(rel.Join.right);
    const on = Array.isArray(rel.Join.on) ? rel.Join.on[0] : undefined;
    if (!isRecord(on) || !isColumnRef(on.left) || !isColumnRef(on.right)) {
      throw new Error("Core runtime schema ExistsRel joins must have a column equality.");
    }
    const rightCorrelationIndex = right.filters.findIndex(isOuterRowEquality);
    if (rightCorrelationIndex !== -1 && right.joins.length === 0 && on.right.column === "id") {
      const rightFilters = [...right.filters];
      const [rightCorrelation] = rightFilters.splice(rightCorrelationIndex, 1);
      if (rightCorrelation?.type !== "Cmp" || rightCorrelation.op !== "Eq") {
        throw new Error(
          "Core runtime schema ExistsRel policies must use equality for outer row correlation.",
        );
      }
      return {
        table: left.table,
        filters: [
          ...left.filters,
          ...rightFilters,
          { ...rightCorrelation, column: on.left.column },
        ],
        joins: left.joins,
        reachable: [...left.reachable, ...right.reachable],
        inherits: [...left.inherits, ...right.inherits],
        pendingReachable: left.pendingReachable,
      };
    }
    const rightFilters = right.filters;
    const correlatedLeftFilters: LoweredPolicyExpr[] = [];
    const leftFilters: LoweredPolicyExpr[] = [];
    for (const filter of left.filters) {
      if (
        filter.type === "Cmp" &&
        filter.op === "Eq" &&
        filter.column === on.left.column &&
        policyOperandValue(filter.value).type === "OuterRowRef"
      ) {
        correlatedLeftFilters.push({ ...filter, column: on.right.column });
      } else {
        leftFilters.push(filter);
      }
    }
    if (correlatedLeftFilters.length > 0 && leftFilters.length === 0 && left.joins.length === 0) {
      return {
        table: right.table,
        filters: [...rightFilters, ...correlatedLeftFilters],
        joins: right.joins,
        reachable: [...left.reachable, ...right.reachable],
        inherits: [...left.inherits, ...right.inherits],
        pendingReachable: right.pendingReachable,
      };
    }

    if (left.pendingReachable && on.left.column === "id") {
      return {
        table: right.table,
        filters: rightFilters,
        joins: right.joins,
        reachable: [
          ...left.reachable,
          ...right.reachable,
          {
            ...left.pendingReachable,
            accessTable: right.table,
            accessRowColumn: "__pending_outer_row",
            accessTeamColumn: on.right.column,
            accessTeamTarget: on.right.column === "id" ? "RowId" : "Column",
            accessFilters: [],
          },
        ],
        inherits: [...left.inherits, ...right.inherits],
      };
    }

    const join: PolicyJoin = {
      table: right.table,
      scope: on.right.scope,
      onColumn: on.right.column,
      target: on.right.column === "id" ? "RowId" : "Column",
      sourceColumn: on.left.column,
      filters: rightFilters,
      nestedJoins: right.joins,
    };
    return {
      table: left.table,
      filters: leftFilters,
      joins: [...left.joins, join],
      reachable: [...left.reachable, ...right.reachable],
      inherits: [...left.inherits, ...right.inherits],
      pendingReachable: left.pendingReachable,
    };
  }

  throw new Error("Core runtime schema policies do not support this ExistsRel relation yet.");
}

type LoweredRelExpr = ReturnType<typeof lowerExistsRel>;

function lowerGatherRel(gather: RelExprObject): LoweredRelExpr {
  const seed = lowerGatherSeed(gather.seed);
  const step = lowerGatherStep(gather.step);
  return {
    table: step.outputTable,
    filters: [],
    joins: [],
    reachable: [],
    inherits: [],
    pendingReachable: {
      from: seed.from,
      seed: seed.seed,
      edgeTable: step.edgeTable,
      edgeMemberColumn: step.edgeMemberColumn,
      edgeParentColumn: step.edgeParentColumn,
      edgeFilters: step.edgeFilters,
      maxDepth: gatherMaxDepth(gather),
    },
  };
}

function gatherMaxDepth(gather: RelExprObject): number {
  if (
    isRecord(gather.bound) &&
    typeof gather.bound.MaxDepth === "number" &&
    Number.isInteger(gather.bound.MaxDepth) &&
    gather.bound.MaxDepth > 0
  ) {
    return gather.bound.MaxDepth;
  }
  throw new Error("Gather relation policies require bound: { MaxDepth: positive integer }.");
}

function lowerGatherSeed(seed: unknown): {
  from: PolicyOperandValue;
  seed: PolicyReachableSeed;
} {
  const projected = unwrapProject(seed);
  const filtered = unwrapSingleFilter(projected ?? seed);
  const join =
    isRecord(filtered.input) && isRecord(filtered.input.Join) ? filtered.input.Join : undefined;
  if (!join) {
    const table = tableScanName(filtered.input);
    if (!table) {
      throw new Error(
        "Core runtime schema Gather policies require table or projected hop relation seeds.",
      );
    }
    const seedFilter = findClaimSeedFilter(filtered.filters);
    if (!seedFilter) {
      throw new Error(
        "Core runtime schema Gather same-table seeds require one claim-keyed equality filter.",
      );
    }
    return {
      from: policyOperandValue(seedFilter.filter.value),
      seed: {
        table,
        userColumn: seedFilter.filter.column,
        userClaim: seedFilter.claim,
        teamColumn: "id",
        filters: withoutFilterAt(filtered.filters, seedFilter.index),
      },
    };
  }
  const on = Array.isArray(join.on) ? join.on[0] : undefined;
  if (!isRecord(on) || !isColumnRef(on.left) || !isColumnRef(on.right)) {
    throw new Error("Core runtime schema Gather policies require seed joins with column equality.");
  }
  const left = unwrapSingleFilter(join.left);
  const right = unwrapSingleFilter(join.right);
  const leftTable = tableScanName(left.input);
  const rightTable = tableScanName(right.input);
  const leftFilters = [...filtersForScope(filtered.filters, on.left.scope), ...left.filters];
  const rightFilters = [...filtersForScope(filtered.filters, on.right.scope), ...right.filters];
  const leftSeedFilter = findClaimSeedFilter(leftFilters);
  const rightSeedFilter = findClaimSeedFilter(rightFilters);
  const seedIsRightSide = rightSeedFilter != null && rightTable != null;
  const seedFilter = seedIsRightSide ? rightSeedFilter : leftSeedFilter;
  if (!seedFilter) {
    throw new Error("Core runtime schema Gather policies could not identify the seed edge.");
  }
  const table = seedIsRightSide ? rightTable : leftTable;
  const teamColumn = seedIsRightSide ? on.right.column : on.left.column;
  const filters = seedIsRightSide ? rightFilters : leftFilters;
  if (!table) {
    throw new Error("Core runtime schema Gather policies could not identify the seed edge.");
  }
  return {
    from: policyOperandValue(seedFilter.filter.value),
    seed: {
      table,
      userColumn: seedFilter.filter.column,
      userClaim: seedFilter.claim,
      teamColumn,
      filters: withoutFilterAt(filters, seedFilter.index),
    },
  };
}

function findClaimSeedFilter(
  filters: LoweredPolicyExpr[],
):
  | { filter: Extract<LoweredPolicyExpr, { type: "Cmp" }>; index: number; claim: string }
  | undefined {
  for (const [index, filter] of filters.entries()) {
    if (filter.type !== "Cmp" || filter.op !== "Eq" || filter.column === "id") {
      continue;
    }
    const value = policyOperandValue(filter.value);
    if (value.type !== "SessionRef") {
      continue;
    }
    return { filter, index, claim: sessionRefClaimName(value.path) };
  }
  return undefined;
}

function withoutFilterAt<T>(filters: T[], index: number): T[] {
  return filters.filter((_, candidate) => candidate !== index);
}

function uniquePolicyFilters<T extends PolicyExpr>(filters: T[]): T[] {
  const seen = new Set<string>();
  return filters.filter((filter) => {
    const key = policyFilterKey(filter);
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function policyFilterKey(filter: PolicyExpr): string {
  if (filter.type === "Cmp") {
    return JSON.stringify({
      type: filter.type,
      column: filter.column,
      op: filter.op,
      value: policyOperandValue(filter.value),
    });
  }
  const { scope: _scope, ...encodedFilter } = filter as PolicyExpr & { scope?: string };
  return JSON.stringify(encodedFilter);
}

function lowerGatherStep(step: unknown): {
  outputTable: string;
  edgeTable: string;
  edgeMemberColumn: string;
  edgeParentColumn: string;
  edgeFilters: PolicyExpr[];
} {
  const projected = unwrapProject(step);
  const join = projected && isRecord(projected.Join) ? projected.Join : undefined;
  const on = join && Array.isArray(join.on) ? join.on[0] : undefined;
  if (!join || !isRecord(on) || !isColumnRef(on.left) || !isColumnRef(on.right)) {
    throw new Error("Core runtime schema Gather policies require projected recursive hops.");
  }
  const left = unwrapSingleFilter(join.left);
  const edgeTable = tableScanName(left.input);
  const outputTable = tableScanName(join.right);
  if (!edgeTable || !outputTable) {
    throw new Error("Core runtime schema Gather policies could not identify recursive hop tables.");
  }
  const frontierIndex = left.filters.findIndex(isFrontierRowEquality);
  if (frontierIndex === -1) {
    throw new Error("Core runtime schema Gather policies require a frontier equality.");
  }
  const edgeFilters = [...left.filters];
  const [frontierFilter] = edgeFilters.splice(frontierIndex, 1);
  if (!frontierFilter || frontierFilter.type !== "Cmp") {
    throw new Error("Core runtime schema Gather policies require an equality frontier filter.");
  }
  return {
    outputTable,
    edgeTable,
    edgeMemberColumn: frontierFilter.column,
    edgeParentColumn: on.left.column,
    edgeFilters,
  };
}

function unwrapProject(rel: unknown): RelExprObject | undefined {
  return isRecord(rel) && isRecord(rel.Project) && isRecord(rel.Project.input)
    ? rel.Project.input
    : undefined;
}

function unwrapSingleFilter(rel: unknown): { input: unknown; filters: LoweredPolicyExpr[] } {
  if (isRecord(rel) && isRecord(rel.Filter)) {
    return {
      input: rel.Filter.input,
      filters: relPredicateToPolicyExprs(rel.Filter.predicate),
    };
  }
  return { input: rel, filters: [] };
}

function tableScanName(rel: unknown): string | undefined {
  return isRecord(rel) && isRecord(rel.TableScan) && typeof rel.TableScan.table === "string"
    ? rel.TableScan.table
    : undefined;
}

function appendRelFilters(
  lowered: { table: string; filters: LoweredPolicyExpr[]; joins: PolicyJoin[] },
  filters: LoweredPolicyExpr[],
): void {
  for (const filter of filters) {
    const scopedJoin =
      filter.scope == null
        ? undefined
        : lowered.joins.find((join) => join.scope === filter.scope || join.table === filter.scope);
    if (scopedJoin) {
      scopedJoin.filters.push(filter);
    } else {
      lowered.filters.push(filter);
    }
  }
}

function relPredicateToPolicyExprs(predicate: unknown): LoweredPolicyExpr[] {
  if (predicate === "True") return [];
  if (predicate === "False") return [{ type: "False" }];
  if (!isRecord(predicate)) {
    throw new Error("Core runtime schema ExistsRel predicate must be an object.");
  }
  if (isRecord(predicate.And)) {
    return Object.values(predicate.And).flatMap(relPredicateToPolicyExprs);
  }
  if (Array.isArray(predicate.And)) {
    return predicate.And.flatMap(relPredicateToPolicyExprs);
  }
  if (Array.isArray(predicate.Or)) {
    return [
      { type: "Or", exprs: predicate.Or.flatMap((child) => relPredicateToPolicyExprs(child)) },
    ];
  }
  if (predicate.Not) {
    const exprs = relPredicateToPolicyExprs(predicate.Not);
    return exprs.length === 1
      ? [{ type: "Not", expr: exprs[0]! }]
      : [{ type: "Not", expr: { type: "And", exprs } }];
  }
  if (isRecord(predicate.Cmp) && isColumnRef(predicate.Cmp.left)) {
    return [
      {
        type: "Cmp",
        column: predicate.Cmp.left.column,
        scope: predicate.Cmp.left.scope,
        op: relCmpOp(predicate.Cmp.op),
        value: relValueToPolicyValue(predicate.Cmp.right),
      },
    ];
  }
  if (isRecord(predicate.IsNull) && isColumnRef(predicate.IsNull.column)) {
    return [
      {
        type: "IsNull",
        column: predicate.IsNull.column.column,
        scope: predicate.IsNull.column.scope,
      },
    ];
  }
  if (isRecord(predicate.IsNotNull) && isColumnRef(predicate.IsNotNull.column)) {
    return [
      {
        type: "IsNotNull",
        column: predicate.IsNotNull.column.column,
        scope: predicate.IsNotNull.column.scope,
      },
    ];
  }
  if (
    isRecord(predicate.In) &&
    isColumnRef(predicate.In.left) &&
    Array.isArray(predicate.In.values)
  ) {
    return [
      {
        type: "InList",
        column: predicate.In.left.column,
        scope: predicate.In.left.scope,
        values: predicate.In.values.map(relValueToPolicyValue),
      },
    ];
  }
  if (isRecord(predicate.Contains) && isColumnRef(predicate.Contains.left)) {
    return [
      {
        type: "Contains",
        column: predicate.Contains.left.column,
        scope: predicate.Contains.left.scope,
        value: relValueToPolicyValue(predicate.Contains.right),
      },
    ];
  }
  throw new Error("Core runtime schema policies do not support this ExistsRel predicate yet.");
}

function relValueToPolicyValue(value: unknown): PolicyValue {
  if (isRecord(value) && "OuterColumn" in value && isColumnRef(value.OuterColumn)) {
    return { type: "SessionRef", path: [OUTER_ROW_SESSION_PREFIX, value.OuterColumn.column] };
  }
  if (isRecord(value) && Array.isArray(value.SessionRef)) {
    return {
      type: "SessionRef",
      path: value.SessionRef.filter((part): part is string => typeof part === "string"),
    };
  }
  if (isRecord(value) && "Literal" in value) {
    return { type: "Literal", value: relLiteralToValue(value.Literal) };
  }
  if (isRecord(value) && value.RowId === "Outer") {
    return { type: "SessionRef", path: [OUTER_ROW_SESSION_PREFIX, "id"] };
  }
  if (isRecord(value) && value.RowId === "Frontier") {
    return { type: "FrontierRowRef" } as InternalPolicyValue as PolicyValue;
  }
  throw new Error("Core runtime schema policies do not support this ExistsRel value yet.");
}

function relLiteralToValue(value: unknown): Value {
  if (isRecord(value) && typeof value.type === "string") {
    return value as Value;
  }
  if (value === null) return { type: "Null" };
  if (typeof value === "boolean") return { type: "Boolean", value };
  if (typeof value === "number" && Number.isInteger(value)) return { type: "Integer", value };
  if (typeof value === "string") return { type: "Text", value };
  throw new Error("Core runtime schema policies do not support this ExistsRel literal yet.");
}

function relCmpOp(op: unknown): "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge" {
  if (op === "Eq" || op === "Ne" || op === "Lt" || op === "Le" || op === "Gt" || op === "Ge") {
    return op;
  }
  throw new Error(
    `Core runtime schema policies do not support ExistsRel comparison ${String(op)}.`,
  );
}

function isColumnRef(value: unknown): value is { column: string; scope?: string } {
  return isRecord(value) && typeof value.column === "string";
}

function isRecord(value: unknown): value is RelExprObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
  const correlatedFilters: PolicyJoin["correlatedFilters"] = [];
  for (let index = filters.length - 1; index >= 0; index -= 1) {
    const filter = filters[index]!;
    if (!isOuterRowEquality(filter) || filter.type !== "Cmp" || filter.op !== "Eq") continue;
    const filterOuter = policyOperandValue(filter.value);
    if (filterOuter.type !== "OuterRowRef") continue;
    filters.splice(index, 1);
    correlatedFilters.unshift({
      column: filter.column,
      sourceColumn: filterOuter.column,
    });
  }

  return {
    table: expr.table,
    onColumn: correlation.column,
    target: correlation.column === "id" && outer.column !== "id" ? "RowId" : "Column",
    sourceColumn: outer.column,
    correlatedFilters,
    filters,
  };
}

function inheritedPolicyToQueryShapes(
  schema: WasmSchema,
  table: string,
  operation: "Select" | "Insert" | "Update" | "Delete",
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
  const parentPolicy = sourceOperationPolicy(schema[parentTable]?.policies, operation) ?? {
    type: "False" as const,
  };

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
  if (isFalseFilterSet(branch.filters)) {
    return { filters: branch.filters, joins: [], reachable: [], inherits: [] };
  }

  const joins: PolicyJoin[] = [];
  if (branch.filters.length > 0) {
    joins.push({
      table: parentTable,
      onColumn: "id",
      target: "RowId",
      sourceColumn: viaColumn,
      filters: branch.filters,
    });
  }
  for (const join of branch.joins) {
    const sourceColumn = join.sourceLookup?.rowIdSourceColumn ?? join.sourceColumn;
    joins.push({
      ...join,
      sourceColumn,
      sourceLookup:
        sourceColumn == null
          ? undefined
          : {
              table: parentTable,
              rowIdSourceColumn: viaColumn,
              valueColumn: sourceColumn,
            },
    });
  }
  return { filters: [], joins, reachable: [], inherits: [] };
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
    type: "False" as const,
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
    reachable: [],
    inherits: [],
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

function isFrontierRowEquality(expr: LoweredPolicyExpr): boolean {
  return (
    expr.type === "Cmp" &&
    expr.op === "Eq" &&
    (expr.value as InternalPolicyValue).type === "FrontierRowRef"
  );
}

function filtersForScope(
  filters: LoweredPolicyExpr[],
  scope: string | undefined,
): LoweredPolicyExpr[] {
  return filters.filter((filter) => filter.scope == null || filter.scope === scope);
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

function uuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let index = 0; index < 16; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function f64Bytes(value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, value, true);
  return bytes;
}
