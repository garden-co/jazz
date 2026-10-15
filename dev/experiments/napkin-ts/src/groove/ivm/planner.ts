// Query lowering, name resolution, and type checking for IVM graphs.
import {
  type DatabaseSchema,
  type Expr,
  type Query,
  type Select,
  type SelectItem,
  type TableRef,
  type TableSchema,
  type ValueType,
  RecordDescriptor,
  enumSchemaDiscriminant,
} from "../../_extern/groove-ivm-plan.js";
import { JazzError } from "../../support/runtime.js";
import { blake3 } from "../../support/hash.js";
import { toHex, utf8 } from "../../support/bytes.js";
import { GraphBuilder, ProjectField } from "./graph.js";
import {
  type LiteralValue,
  type PredicateExpr,
  type PredicateKind,
  canonicalizePredicate,
  literalFromValue,
  literalValueType,
  reversedPredicateKind,
  structEq,
  structKey,
} from "./op_types.js";

export interface LogicalField {
  qualifier?: string;
  name: string;
  sourceName: string;
  valueType: ValueType;
}

export type LogicalPlan =
  | { kind: "Scan"; table: string; alias?: string; fields: LogicalField[] }
  | { kind: "BindingRelation"; shape: string; fields: LogicalField[] }
  | { kind: "Filter"; input: LogicalPlan; predicate: PredicateExpr; fields: LogicalField[] }
  | { kind: "Project"; input: LogicalPlan; fields: LogicalField[] }
  | {
      kind: "Join";
      left: LogicalPlan;
      right: LogicalPlan;
      leftOn: string[];
      rightOn: string[];
      fields: LogicalField[];
    }
  | { kind: "UnionAll"; inputs: LogicalPlan[]; fields: LogicalField[] };

export function canonicalizeLogical(plan: LogicalPlan): LogicalPlan {
  switch (plan.kind) {
    case "Filter":
      return {
        ...plan,
        input: canonicalizeLogical(plan.input),
        predicate: canonicalizePredicate(plan.predicate),
      };
    case "Project":
      return { ...plan, input: canonicalizeLogical(plan.input) };
    case "Join":
      return {
        ...plan,
        left: canonicalizeLogical(plan.left),
        right: canonicalizeLogical(plan.right),
      };
    case "UnionAll":
      return { ...plan, inputs: plan.inputs.map(canonicalizeLogical) };
    default:
      return plan;
  }
}

export interface PlannedQuery {
  logical: LogicalPlan;
  graph: GraphBuilder;
  output: LogicalField[];
}

export const plannedOutputDescriptor = (p: PlannedQuery): RecordDescriptor =>
  RecordDescriptor.new(p.output.map((f) => [f.name, f.valueType]));

export interface QueryParameter {
  name: string;
  valueType: ValueType;
}

export interface PlannedPreparedShape {
  planned: PlannedQuery;
  shape: string;
  parameters: QueryParameter[];
  bindingDescriptor: RecordDescriptor;
  outputKeyFields: string[];
  publicOutput: LogicalField[];
}

export class PlannerError extends JazzError {
  static ambiguousColumn = (c: string) =>
    new PlannerError("AmbiguousColumn", `ambiguous column: ${c}`);
  static columnNotFound = (c: string) =>
    new PlannerError("ColumnNotFound", `column not found: ${c}`);
  static outputMismatch = () => new PlannerError("OutputMismatch", "query outputs do not match");
  static tableNotFound = (t: string) => new PlannerError("TableNotFound", `table not found: ${t}`);
  static typeMismatch = (left: ValueType, right: ValueType) =>
    new PlannerError("TypeMismatch", `type mismatch: ${structKey(left)} vs ${structKey(right)}`);
  static unsupportedExpression = (m: string) => new PlannerError("UnsupportedExpression", m);
  static unsupportedQuery = (m: string) => new PlannerError("UnsupportedQuery", m);
}

const BINDING_QUALIFIER = "__bindings";

export function planQuery(query: Query, schema: DatabaseSchema): PlannedQuery {
  const planned = new Planner(schema).planQuery(query);
  if (planned.parameters.length)
    throw PlannerError.unsupportedQuery("query parameters require prepare_query");
  return planned.planned;
}

export function planPreparedShape(query: Query, schema: DatabaseSchema): PlannedPreparedShape {
  const planned = new Planner(schema).planQuery(query);
  if (!planned.parameters.length)
    throw PlannerError.unsupportedQuery("prepare_query requires at least one query parameter");
  return {
    planned: planned.planned,
    shape: planned.shape,
    parameters: planned.parameters,
    bindingDescriptor: RecordDescriptor.new(planned.parameters.map((p) => [p.name, p.valueType])),
    outputKeyFields: planned.parameters.map((p) => p.name),
    publicOutput: planned.planned.output.filter((f) => f.qualifier !== BINDING_QUALIFIER),
  };
}

interface BindingPredicate {
  field: LogicalField;
  parameter: string;
}

type PredicateOperand =
  | { kind: "Field"; field: LogicalField }
  | { kind: "Literal"; value: LiteralValue };

class Planner {
  private ctes = new Map<string, LogicalPlan>();
  constructor(private readonly schema: DatabaseSchema) {}

  planQuery(query: Query): { planned: PlannedQuery; parameters: QueryParameter[]; shape: string } {
    let logical = canonicalizeLogical(this.lowerQuery(query));
    const parameters = collectBindingFields(logical);
    let shape = "";
    if (parameters.length) {
      shape = preparedShapeName(logical, parameters);
      logical = rewriteParamShape(logical, shape);
    }
    const graph = graphFromLogical(logical);
    return { planned: { logical, graph, output: logical.fields.slice() }, parameters, shape };
  }

  lowerQuery(query: Query): LogicalPlan {
    switch (query.kind) {
      case "Select":
        return this.lowerSelect(query.select);
      case "Set": {
        const set = query.set;
        if (set.op !== "Union" || set.quantifier !== "All")
          throw PlannerError.unsupportedQuery("only UNION ALL set queries are currently lowerable");
        const left = this.lowerQuery(set.left);
        const right = this.lowerQuery(set.right);
        if (!comparableFields(left.fields, right.fields)) throw PlannerError.outputMismatch();
        return { kind: "UnionAll", fields: left.fields.slice(), inputs: [left, right] };
      }
      case "With": {
        const w = query.with;
        if (w.recursive)
          throw PlannerError.unsupportedQuery("recursive CTE lowering is not implemented yet");
        const oldCtes = new Map(this.ctes);
        try {
          for (const cte of w.ctes) {
            this.ctes.set(
              cte.name,
              applyRelationAlias(this.lowerQuery(cte.query), cte.name, cte.columns),
            );
          }
          return this.lowerQuery(w.query);
        } finally {
          this.ctes = oldCtes;
        }
      }
      default:
        throw PlannerError.unsupportedQuery("unknown query kind");
    }
  }

  lowerSelect(select: Select): LogicalPlan {
    if (select.quantifier !== "All")
      throw PlannerError.unsupportedQuery("SELECT DISTINCT lowering is not implemented yet");
    if (
      select.groupBy.length ||
      select.having ||
      select.orderBy.length ||
      select.limit ||
      select.offset
    )
      throw PlannerError.unsupportedQuery(
        "GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET are not implemented yet",
      );
    let input = this.lowerFrom(select.from);
    let bindingSourceFields: [string, LogicalField][] = [];
    if (select.selection) {
      const [bindingPredicates, residual] = this.extractBindingPredicates(
        select.selection,
        input.fields,
      );
      if (bindingPredicates.length) {
        bindingSourceFields = bindingPredicates.map((p) => [p.parameter, p.field]);
        input = placeBindingJoin(input, bindingPredicates);
      }
      if (residual) {
        const predicate = canonicalizePredicate(this.lowerPredicate(residual, input.fields));
        input = { kind: "Filter", fields: input.fields.slice(), input, predicate };
      }
    }
    const bindingFields = input.fields.filter((f) => f.qualifier === BINDING_QUALIFIER);
    let projected = this.lowerProjection(input, select.projection);
    if (bindingFields.length)
      projected = appendMissingBindingFields(projected, bindingFields, bindingSourceFields);
    return projected;
  }

  lowerFrom(from: TableRef[]): LogicalPlan {
    if (!from.length) throw PlannerError.unsupportedQuery("SELECT without FROM");
    const plan = this.lowerTableRef(from[0]);
    if (from.length > 1)
      throw PlannerError.unsupportedQuery("implicit joins need an ON clause for now");
    return plan;
  }

  lowerTableRef(ref: TableRef): LogicalPlan {
    switch (ref.kind) {
      case "Named": {
        const tableName = singleName(ref.name);
        const alias = ref.alias;
        if (tableName === BINDING_QUALIFIER || alias?.name === BINDING_QUALIFIER)
          throw PlannerError.unsupportedQuery(
            "relation name is reserved for prepared-query bindings",
          );
        const cte = this.ctes.get(tableName);
        if (cte)
          return applyRelationAlias(
            cte,
            alias ? alias.name : tableName,
            alias ? alias.columns : [],
          );
        const table = this.schema.table(tableName);
        if (!table) throw PlannerError.tableNotFound(tableName);
        const plan = scanPlan(table, alias?.name);
        return alias && alias.columns.length
          ? applyRelationAlias(plan, alias.name, alias.columns)
          : plan;
      }
      case "Join":
        return this.lowerJoin(ref.left, ref.right, ref.joinKind, ref.constraint);
      default:
        throw PlannerError.unsupportedQuery("derived table lowering is not implemented yet");
    }
  }

  lowerJoin(leftRef: TableRef, rightRef: TableRef, kind: string, constraint: any): LogicalPlan {
    if (kind !== "Inner")
      throw PlannerError.unsupportedQuery("only inner equi-join lowering is implemented");
    const left = this.lowerTableRef(leftRef);
    const right = this.lowerTableRef(rightRef);
    if (constraint.kind !== "On")
      throw PlannerError.unsupportedQuery("only ON join constraints are implemented");
    const [leftOn, rightOn] = lowerJoinKeys(constraint.expr, left.fields, right.fields);
    return {
      kind: "Join",
      left,
      right,
      leftOn,
      rightOn,
      fields: joinFields(left.fields, right.fields),
    };
  }

  lowerProjection(input: LogicalPlan, projection: SelectItem[]): LogicalPlan {
    let fields: LogicalField[];
    if (!projection.length) fields = input.fields.slice();
    else {
      fields = [];
      for (const item of projection) {
        switch (item.kind) {
          case "Wildcard":
            fields.push(...input.fields);
            break;
          case "QualifiedWildcard": {
            const qualifier = item.qualifier.join(".");
            const qualified = input.fields.filter((f) => f.qualifier === qualifier);
            if (!qualified.length) throw PlannerError.columnNotFound(`${qualifier}.*`);
            fields.push(...qualified);
            break;
          }
          case "Expr":
            fields.push(this.resolveProjectedField(input.fields, item.expr, item.alias));
        }
      }
    }
    return { kind: "Project", input, fields };
  }

  resolveProjectedField(inputFields: LogicalField[], expr: Expr, alias?: string): LogicalField {
    if (expr.kind !== "Column")
      throw PlannerError.unsupportedExpression("only column projection is currently lowerable");
    const field = { ...resolveColumn(inputFields, expr.column.qualifier, expr.column.name) };
    if (alias !== undefined) {
      field.name = alias;
      field.qualifier = undefined;
    }
    return field;
  }

  lowerPredicate(expr: Expr, fields: LogicalField[]): PredicateExpr {
    if (expr.kind === "Binary" && (expr.op === "And" || expr.op === "Or"))
      return {
        kind: expr.op,
        children: [this.lowerPredicate(expr.left, fields), this.lowerPredicate(expr.right, fields)],
      };
    if (expr.kind === "Binary") return this.lowerComparison(expr.left, expr.op, expr.right, fields);
    if (expr.kind === "Unary") return this.lowerUnaryPredicate(expr.op, expr.expr, fields);
    throw PlannerError.unsupportedExpression(
      "only binary and IS NULL predicates are currently lowerable",
    );
  }

  lowerUnaryPredicate(op: string, expr: Expr, fields: LogicalField[]): PredicateExpr {
    if (expr.kind !== "Column")
      throw PlannerError.unsupportedExpression("IS NULL predicates require a column operand");
    const field = resolveColumn(fields, expr.column.qualifier, expr.column.name);
    if (op === "IsNull") return { kind: "IsNull", field: field.name };
    if (op === "IsNotNull") return { kind: "IsNotNull", field: field.name };
    throw PlannerError.unsupportedExpression(
      "only IS NULL and IS NOT NULL unary predicates are lowerable",
    );
  }

  lowerComparison(
    leftExpr: Expr,
    op: string,
    rightExpr: Expr,
    fields: LogicalField[],
  ): PredicateExpr {
    const kinds: Record<string, PredicateKind> = {
      Eq: "Eq",
      NotEq: "Neq",
      Gt: "Gt",
      GtEq: "GtEq",
      Lt: "Lt",
      LtEq: "LtEq",
    };
    const kind = kinds[op];
    if (!kind)
      throw PlannerError.unsupportedExpression(
        "only comparison predicates are currently lowerable",
      );
    const left = this.lowerPredicateOperand(leftExpr, fields);
    const right = this.lowerPredicateOperand(rightExpr, fields);
    typeCheckComparison(left, right);
    if (left.kind === "Field" && right.kind === "Literal") {
      const value = normalizeLiteralForField(right.value, left.field.valueType);
      rejectNullLiteral(value);
      return { kind, field: left.field.name, value };
    }
    if (left.kind === "Literal" && right.kind === "Field") {
      const value = normalizeLiteralForField(left.value, right.field.valueType);
      rejectNullLiteral(value);
      return { kind: reversedPredicateKind(kind), field: right.field.name, value };
    }
    throw PlannerError.unsupportedExpression(
      "only field-literal predicates are executable for now",
    );
  }

  lowerPredicateOperand(expr: Expr, fields: LogicalField[]): PredicateOperand {
    switch (expr.kind) {
      case "Column":
        return {
          kind: "Field",
          field: resolveColumn(fields, expr.column.qualifier, expr.column.name),
        };
      case "Literal":
        return { kind: "Literal", value: literalFromValue(expr.value) };
      case "Null":
        return { kind: "Literal", value: { kind: "Nullable" } };
      case "Parameter":
        throw PlannerError.unsupportedExpression(
          "only equality parameter predicates are supported",
        );
      default:
        throw PlannerError.unsupportedExpression(
          "only column and literal scalar expressions are currently lowerable",
        );
    }
  }

  extractBindingPredicates(
    expr: Expr,
    fields: LogicalField[],
  ): [BindingPredicate[], Expr | undefined] {
    if (expr.kind === "Binary" && expr.op === "And") {
      const [leftParams, leftResidual] = this.extractBindingPredicates(expr.left, fields);
      const [rightParams, rightResidual] = this.extractBindingPredicates(expr.right, fields);
      const residual =
        leftResidual && rightResidual
          ? ({ kind: "Binary", left: leftResidual, op: "And", right: rightResidual } as Expr)
          : (leftResidual ?? rightResidual);
      return [[...leftParams, ...rightParams], residual];
    }
    if (containsBindingParameter(expr))
      return [[this.lowerBindingPredicate(expr, fields)], undefined];
    return [[], expr];
  }

  lowerBindingPredicate(expr: Expr, fields: LogicalField[]): BindingPredicate {
    if (expr.kind !== "Binary" || expr.op !== "Eq")
      throw PlannerError.unsupportedExpression("only equality parameter predicates are supported");
    const { left, right } = expr;
    const pair =
      left.kind === "Column" && right.kind === "Parameter"
        ? [left, right]
        : left.kind === "Parameter" && right.kind === "Column"
          ? [right, left]
          : undefined;
    if (!pair)
      throw PlannerError.unsupportedExpression("only column = parameter predicates are supported");
    const [column, parameter] = pair;
    return {
      field: resolveColumn(fields, column.column.qualifier, column.column.name),
      parameter: parameter.name,
    };
  }
}

function graphFromLogical(plan: LogicalPlan, required?: LogicalField[]): GraphBuilder {
  switch (plan.kind) {
    case "Scan":
      return projectRequiredGraph(GraphBuilder.table(plan.table), plan.fields, required);
    case "BindingRelation":
      return GraphBuilder.bindingSource(
        plan.shape,
        RecordDescriptor.new(plan.fields.map((f) => [f.name, f.valueType])),
      );
    case "Filter":
      return projectRequiredGraph(
        GraphBuilder.filter(graphFromLogical(plan.input), plan.predicate),
        plan.fields,
        required,
      );
    case "Project": {
      if (structEq(plan.fields, plan.input.fields)) return graphFromLogical(plan.input, required);
      if (!required) return graphFromLogical(plan.input, plan.fields);
      const selected = required.map((f) => fieldForSource(plan.fields, f.sourceName));
      return projectRequiredGraph(graphFromLogical(plan.input, selected), selected, required);
    }
    case "Join": {
      const req = required ?? plan.fields;
      const leftRequired = joinChildRequiredFields(plan.left.fields, plan.leftOn, req, "left");
      const rightRequired = joinChildRequiredFields(plan.right.fields, plan.rightOn, req, "right");
      const leftKeys = plan.leftOn.map((k) => fieldForSource(leftRequired, k).name);
      const rightKeys = plan.rightOn.map((k) => fieldForSource(rightRequired, k).name);
      const leftGraph = filterSqlNullableJoinKeys(
        graphFromLogical(plan.left, leftRequired),
        leftRequired,
        plan.leftOn,
        leftKeys,
      );
      const rightGraph = filterSqlNullableJoinKeys(
        graphFromLogical(plan.right, rightRequired),
        rightRequired,
        plan.rightOn,
        rightKeys,
      );
      return projectJoinRequiredGraph(
        GraphBuilder.join(leftGraph, rightGraph, leftKeys, rightKeys),
        leftRequired,
        rightRequired,
        req,
      );
    }
    case "UnionAll": {
      const graph = GraphBuilder.union(
        plan.inputs.map((input) =>
          projectPositionalGraph(graphFromLogical(input), input.fields, plan.fields),
        ),
      );
      return projectRequiredGraph(graph, plan.fields, required);
    }
  }
}

function filterSqlNullableJoinKeys(
  graph: GraphBuilder,
  fields: LogicalField[],
  sourceKeys: string[],
  visibleKeys: string[],
): GraphBuilder {
  const predicates: PredicateExpr[] = [];
  sourceKeys.forEach((sourceKey, i) => {
    if (fieldForSource(fields, sourceKey).valueType.kind === "Nullable")
      predicates.push({ kind: "IsNotNull", field: visibleKeys[i] });
  });
  if (!predicates.length) return graph;
  const predicate =
    predicates.length === 1
      ? predicates[0]
      : canonicalizePredicate({ kind: "And", children: predicates });
  return GraphBuilder.filter(graph, predicate);
}

function projectJoinRequiredGraph(
  join: GraphBuilder,
  leftRequired: LogicalField[],
  rightRequired: LogicalField[],
  required: LogicalField[],
): GraphBuilder {
  return GraphBuilder.projectFields(
    join,
    required.map((field) => {
      if (field.sourceName.startsWith("left.")) {
        const child = fieldForSource(leftRequired, field.sourceName.slice(5));
        return ProjectField.renamed(`left.${child.name}`, field.name);
      }
      if (field.sourceName.startsWith("right.")) {
        const child = fieldForSource(rightRequired, field.sourceName.slice(6));
        return ProjectField.renamed(`right.${child.name}`, field.name);
      }
      return ProjectField.renamed(field.sourceName, field.name);
    }),
  );
}

function projectRequiredGraph(
  graph: GraphBuilder,
  available: LogicalField[],
  required?: LogicalField[],
): GraphBuilder {
  if (!required) return graph;
  if (
    required.length === available.length &&
    required.every(
      (r, i) =>
        r.sourceName === available[i].sourceName &&
        r.name === available[i].name &&
        structEq(r.valueType, available[i].valueType),
    )
  )
    return graph;
  return GraphBuilder.projectFields(
    graph,
    required.map((f) => ProjectField.renamed(fieldForSource(available, f.sourceName).name, f.name)),
  );
}

function projectPositionalGraph(
  graph: GraphBuilder,
  available: LogicalField[],
  target: LogicalField[],
): GraphBuilder {
  if (
    available.length !== target.length ||
    available.some((a, i) => !structEq(a.valueType, target[i].valueType))
  )
    throw PlannerError.unsupportedQuery("set query inputs must have compatible columns");
  if (available.every((a, i) => a.name === target[i].name)) return graph;
  return GraphBuilder.projectFields(
    graph,
    available.map((a, i) => ProjectField.renamed(a.name, target[i].name)),
  );
}

function joinChildRequiredFields(
  childFields: LogicalField[],
  joinKeys: string[],
  parentRequired: LogicalField[],
  side: string,
): LogicalField[] {
  const fields: LogicalField[] = [];
  const pushUnique = (f: LogicalField) => {
    if (!fields.some((c) => c.sourceName === f.sourceName)) fields.push(f);
  };
  for (const key of joinKeys) pushUnique(fieldForSource(childFields, key));
  const prefix = `${side}.`;
  for (const field of parentRequired) {
    if (field.sourceName.startsWith(prefix))
      pushUnique(fieldForSource(childFields, field.sourceName.slice(prefix.length)));
  }
  return fields;
}

function fieldForSource(fields: LogicalField[], sourceName: string): LogicalField {
  const f = fields.find((f) => f.sourceName === sourceName);
  if (!f) throw PlannerError.columnNotFound(sourceName);
  return f;
}

function addBindingJoin(input: LogicalPlan, predicates: BindingPredicate[]): LogicalPlan {
  const paramTypes = new Map<string, ValueType>();
  for (const p of predicates) {
    const existing = paramTypes.get(p.parameter);
    if (existing) {
      if (!structEq(existing, p.field.valueType))
        throw PlannerError.typeMismatch(existing, p.field.valueType);
    } else paramTypes.set(p.parameter, p.field.valueType);
  }
  const bindingFields = [...paramTypes]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([name, valueType]) => ({
      qualifier: BINDING_QUALIFIER,
      sourceName: name,
      name,
      valueType,
    }));
  const paramRelation: LogicalPlan = { kind: "BindingRelation", shape: "", fields: bindingFields };
  return {
    kind: "Join",
    left: input,
    right: paramRelation,
    leftOn: predicates.map((p) => p.field.sourceName),
    rightOn: predicates.map((p) => p.parameter),
    fields: joinFields(input.fields, paramRelation.fields),
  };
}

function placeBindingJoin(input: LogicalPlan, predicates: BindingPredicate[]): LogicalPlan {
  const qualifiers = new Set(predicates.map((p) => p.field.qualifier));
  if (qualifiers.size === 1) {
    const qualifier = [...qualifiers][0];
    if (qualifier !== undefined) {
      const pushed = pushParamJoinIntoQualifier(input, predicates, qualifier);
      if (pushed) return pushed;
    }
  }
  return addBindingJoin(input, predicates);
}

function pushParamJoinIntoQualifier(
  input: LogicalPlan,
  predicates: BindingPredicate[],
  qualifier: string,
): LogicalPlan | undefined {
  switch (input.kind) {
    case "Scan": {
      if (!input.fields.some((f) => f.qualifier === qualifier)) return undefined;
      const local = predicates.map((p) => ({
        field: resolveFieldByOutputIdentity(input.fields, p.field),
        parameter: p.parameter,
      }));
      return addBindingJoin(input, local);
    }
    case "Join": {
      const { left, right } = input;
      if (planHasQualifier(left, qualifier)) {
        const newLeft = pushParamJoinIntoQualifier(left, predicates, qualifier);
        if (newLeft)
          return {
            ...input,
            left: newLeft,
            leftOn: remapJoinKeys(left.fields, newLeft.fields, input.leftOn),
            fields: joinFields(newLeft.fields, right.fields),
          };
      }
      if (planHasQualifier(right, qualifier)) {
        const newRight = pushParamJoinIntoQualifier(right, predicates, qualifier);
        if (newRight)
          return {
            ...input,
            right: newRight,
            rightOn: remapJoinKeys(right.fields, newRight.fields, input.rightOn),
            fields: joinFields(left.fields, newRight.fields),
          };
      }
      return undefined;
    }
    case "Filter": {
      const newInput = pushParamJoinIntoQualifier(input.input, predicates, qualifier);
      return (
        newInput && {
          kind: "Filter",
          fields: newInput.fields.slice(),
          input: newInput,
          predicate: input.predicate,
        }
      );
    }
    default:
      return undefined;
  }
}

const planHasQualifier = (plan: LogicalPlan, qualifier: string) =>
  plan.fields.some((f) => f.qualifier === qualifier);

function resolveFieldByOutputIdentity(fields: LogicalField[], target: LogicalField): LogicalField {
  const f = fields.find((f) => f.qualifier === target.qualifier && f.name === target.name);
  if (!f) throw PlannerError.columnNotFound(target.name);
  return f;
}

function remapJoinKeys(
  oldFields: LogicalField[],
  newFields: LogicalField[],
  keys: string[],
): string[] {
  return keys.map((key) => {
    const old = oldFields.find((f) => f.sourceName === key);
    if (!old) throw PlannerError.columnNotFound(key);
    return resolveFieldByOutputIdentity(newFields, old).sourceName;
  });
}

function appendMissingBindingFields(
  input: LogicalPlan,
  bindingFields: LogicalField[],
  bindingSourceFields: [string, LogicalField][],
): LogicalPlan {
  const [inner, fields] =
    input.kind === "Project" ? [input.input, input.fields.slice()] : [input, input.fields.slice()];
  const collision = () =>
    PlannerError.unsupportedQuery("projected output names must not collide with parameter names");
  for (const field of bindingFields) {
    let existingName = false;
    for (const existing of fields.filter((c) => c.name === field.name)) {
      existingName = true;
      if (existing.qualifier === field.qualifier) continue;
      const source = bindingSourceFields.find(([parameter]) => parameter === field.name)?.[1];
      if (!source || existing.qualifier !== source.qualifier || existing.name !== source.name)
        throw collision();
    }
    if (!existingName) fields.push(field);
  }
  return { kind: "Project", input: inner, fields };
}

function collectBindingFields(plan: LogicalPlan): QueryParameter[] {
  const parameters = new Map<string, ValueType>();
  const visit = (p: LogicalPlan): void => {
    switch (p.kind) {
      case "BindingRelation":
        for (const f of p.fields) parameters.set(f.name, f.valueType);
        return;
      case "Filter":
      case "Project":
        return visit(p.input);
      case "Join":
        visit(p.left);
        return visit(p.right);
      case "UnionAll":
        return p.inputs.forEach(visit);
    }
  };
  visit(plan);
  return [...parameters]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([name, valueType]) => ({ name, valueType }));
}

function rewriteParamShape(plan: LogicalPlan, shape: string): LogicalPlan {
  switch (plan.kind) {
    case "BindingRelation":
      return { ...plan, shape };
    case "Filter":
    case "Project":
      return { ...plan, input: rewriteParamShape(plan.input, shape) } as LogicalPlan;
    case "Join":
      return {
        ...plan,
        left: rewriteParamShape(plan.left, shape),
        right: rewriteParamShape(plan.right, shape),
      };
    case "UnionAll":
      return { ...plan, inputs: plan.inputs.map((i) => rewriteParamShape(i, shape)) };
    default:
      return plan;
  }
}

function preparedShapeName(plan: LogicalPlan, parameters: QueryParameter[]): string {
  // Prepared shape names are in-memory only.
  const hash = blake3(utf8(structKey([rewriteParamShape(plan, ""), parameters])), 8);
  return `prepared_${toHex(hash)}`;
}

function containsBindingParameter(expr: Expr): boolean {
  switch (expr.kind) {
    case "Parameter":
      return true;
    case "Unary":
    case "InSubquery":
    case "Cast":
      return containsBindingParameter(expr.expr);
    case "Binary":
      return containsBindingParameter(expr.left) || containsBindingParameter(expr.right);
    case "Between":
      return (
        containsBindingParameter(expr.expr) ||
        containsBindingParameter(expr.low) ||
        containsBindingParameter(expr.high)
      );
    case "InList":
      return containsBindingParameter(expr.expr) || expr.list.some(containsBindingParameter);
    case "Function":
      return expr.function.args.some(
        (a: any) => a.kind === "Expr" && containsBindingParameter(a.expr),
      );
    case "Case":
      return (
        (expr.operand !== undefined && containsBindingParameter(expr.operand)) ||
        expr.whenThen.some(
          ([w, t]: [Expr, Expr]) => containsBindingParameter(w) || containsBindingParameter(t),
        ) ||
        (expr.elseExpr !== undefined && containsBindingParameter(expr.elseExpr))
      );
    default:
      return false;
  }
}

function scanPlan(table: TableSchema, alias?: string): LogicalPlan {
  const qualifier = alias ?? table.name;
  return {
    kind: "Scan",
    table: table.name,
    alias,
    fields: table.columns.map((c: any) => ({
      qualifier,
      name: c.name,
      sourceName: c.name,
      valueType: c.columnType,
    })),
  };
}

function applyRelationAlias(
  plan: LogicalPlan,
  qualifier: string,
  columnAliases: string[],
): LogicalPlan {
  const publicCount = plan.fields.filter((f) => f.qualifier !== BINDING_QUALIFIER).length;
  if (columnAliases.length > publicCount)
    throw PlannerError.unsupportedQuery("column alias list has more entries than source columns");
  let publicIndex = 0;
  const fields = plan.fields.map((source) => {
    if (source.qualifier === BINDING_QUALIFIER) return source;
    const field = { ...source, qualifier };
    const alias = columnAliases[publicIndex++];
    if (alias !== undefined) field.name = alias;
    return field;
  });
  return { kind: "Project", input: plan, fields };
}

function resolveColumn(
  fields: LogicalField[],
  qualifierParts: string[],
  name: string,
): LogicalField {
  const qualifier = qualifierParts.length ? qualifierParts.join(".") : undefined;
  const matches = fields.filter(
    (f) => f.name === name && (qualifier === undefined || f.qualifier === qualifier),
  );
  if (matches.length === 1) return matches[0];
  if (!matches.length) throw PlannerError.columnNotFound(name);
  throw PlannerError.ambiguousColumn(name);
}

function rejectNullLiteral(value: LiteralValue): void {
  if (value.kind === "Nullable" && value.value === undefined)
    throw PlannerError.unsupportedExpression("NULL literal predicates are not executable yet");
}

function operandType(o: PredicateOperand): ValueType | undefined {
  return o.kind === "Field" ? o.field.valueType : literalValueType(o.value);
}

function typeCheckComparison(left: PredicateOperand, right: PredicateOperand): void {
  const l = operandType(left);
  const r = operandType(right);
  if (l && r && !comparableValueTypes(l, r)) throw PlannerError.typeMismatch(l, r);
}

function normalizeLiteralForField(value: LiteralValue, fieldType: ValueType): LiteralValue {
  if (value.kind !== "String") return value;
  const enumType =
    fieldType.kind === "EnumTag"
      ? fieldType
      : fieldType.kind === "Nullable" && fieldType.inner.kind === "EnumTag"
        ? fieldType.inner
        : undefined;
  if (!enumType) return value;
  const tag = enumSchemaDiscriminant(enumType.schema, value.value);
  if (tag === undefined) throw PlannerError.typeMismatch(fieldType, { kind: "String" });
  return { kind: "EnumTag", value: tag };
}

function comparableValueTypes(left: ValueType, right: ValueType): boolean {
  if (structEq(left, right)) return true;
  const lu = left.kind === "Nullable" ? left.inner : left;
  const ru = right.kind === "Nullable" ? right.inner : right;
  if (
    (lu.kind === "EnumTag" && ru.kind === "String") ||
    (lu.kind === "String" && ru.kind === "EnumTag")
  )
    return true;
  if (left.kind === "Nullable") return structEq(left.inner, right);
  if (right.kind === "Nullable") return structEq(left, right.inner);
  return false;
}

function lowerJoinKeys(
  predicate: Expr,
  leftFields: LogicalField[],
  rightFields: LogicalField[],
): [string[], string[]] {
  if (predicate.kind === "Binary" && predicate.op === "And") {
    const [l1, r1] = lowerJoinKeys(predicate.left, leftFields, rightFields);
    const [l2, r2] = lowerJoinKeys(predicate.right, leftFields, rightFields);
    return [
      [...l1, ...l2],
      [...r1, ...r2],
    ];
  }
  if (predicate.kind !== "Binary" || predicate.op !== "Eq")
    throw PlannerError.unsupportedExpression("only equality join predicates are implemented");
  if (predicate.left.kind !== "Column" || predicate.right.kind !== "Column")
    throw PlannerError.unsupportedExpression("join keys must be columns");
  const l = resolveJoinColumn(leftFields, rightFields, predicate.left.column);
  const r = resolveJoinColumn(leftFields, rightFields, predicate.right.column);
  if (l.side === r.side)
    throw PlannerError.unsupportedExpression("join keys must reference opposite join inputs");
  const [leftField, rightField] = l.side === "Left" ? [l.field, r.field] : [r.field, l.field];
  if (!structEq(leftField.valueType, rightField.valueType))
    throw PlannerError.typeMismatch(leftField.valueType, rightField.valueType);
  return [[leftField.sourceName], [rightField.sourceName]];
}

function resolveJoinColumn(
  leftFields: LogicalField[],
  rightFields: LogicalField[],
  column: { qualifier: string[]; name: string },
): { side: "Left" | "Right"; field: LogicalField } {
  const qualifier = column.qualifier.length ? column.qualifier.join(".") : undefined;
  let resolved: { side: "Left" | "Right"; field: LogicalField } | undefined;
  let count = 0;
  for (const [side, fields] of [
    ["Left", leftFields],
    ["Right", rightFields],
  ] as const) {
    for (const field of fields) {
      // Prepared bindings are carried through CTEs but are not SQL-visible.
      if (
        field.qualifier !== BINDING_QUALIFIER &&
        field.name === column.name &&
        (qualifier === undefined || field.qualifier === qualifier)
      ) {
        count++;
        resolved = { side, field };
      }
    }
  }
  if (count === 1 && resolved) return resolved;
  if (count === 0) throw PlannerError.columnNotFound(column.name);
  throw PlannerError.ambiguousColumn(column.name);
}

function joinFields(left: LogicalField[], right: LogicalField[]): LogicalField[] {
  return [
    ...left.map((f) => ({ ...f, sourceName: `left.${f.sourceName}` })),
    ...right.map((f) => ({ ...f, sourceName: `right.${f.sourceName}` })),
  ];
}

function comparableFields(left: LogicalField[], right: LogicalField[]): boolean {
  return (
    left.length === right.length && left.every((l, i) => structEq(l.valueType, right[i].valueType))
  );
}

function singleName(name: string[]): string {
  if (name.length !== 1)
    throw PlannerError.unsupportedQuery("only single-part object names are implemented");
  return name[0];
}
