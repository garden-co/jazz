import { SortedMap, cmpStr } from "../../../support/index.js";
import type {
  NodeState,
  TableSchema,
  JazzQuery,
  Predicate,
  Operand,
  Value,
  AuthorSubject,
  RuntimeSchema,
  ProgramClaimParam,
  ColumnType,
  ValidatedQuery,
  Binding,
  SourceId,
  NormalizedRowSetShape,
  RowSetExpr,
  NormalizedValueRef,
  NormalizedPredicateExpr,
  PolicyContext,
  ProgramBinding,
  ArraySubquery,
  JoinVia,
  ReachableVia,
} from "../../../_extern/node-qe-b.js";
import {
  Err,
  QueryError,
  installNodeMethods,
  queryFrom,
  operandClaimPath,
  policyClaimAtPath,
  authorPolicyClaims,
  claimParamField,
  claimPathFromParamField,
  debugFmt,
  rootSourceId,
  flatJoinPredicateSources,
  unqualifyFlatJoinPredicate,
  qualifyFlatJoinSourcePredicate,
  operandColumnType,
  containsNeedleType,
  coerceLiteralForColumnType,
  schemaColumnType,
  coercePreparedBindingValue,
  bindingIdForValues,
} from "../../../_extern/node-qe-b.js";
import {
  preparedClaimValue,
  queryBindingSourceShapeForPartsIfNeeded,
} from "./prepared_bindings.js";

export enum PreparedClaimBindingMode {
  Strict,
  FailClosedAuthorizationSupport,
}

export function authorizationQueryFromReadPolicy(table: TableSchema): JazzQuery {
  const query = queryFrom(table.name);
  const policy = table.readPolicy;
  if (!policy) {
    query.filters.push({ kind: "Any", items: [] });
    return query;
  }
  query.filters = [...policy.filters];
  query.joins = [...policy.joins];
  query.reachable = [...policy.reachable];
  query.inherits = [...policy.inherits];
  query.includes = [...policy.includes];
  query.policyBranches = [...policy.policyBranches];
  return query;
}

type Claims = Map<string, Value> | undefined;

export function rewriteClaimJoinForBinding(join: JoinVia, claims: Claims): JoinVia {
  return {
    ...join,
    filters: join.filters.map((p: Predicate) => rewriteClaimPredicateForBinding(p, claims)),
    nestedJoins: join.nestedJoins.map((j: JoinVia) => rewriteClaimJoinForBinding(j, claims)),
  };
}

export function rewriteClaimPredicateForBinding(predicate: Predicate, claims: Claims): Predicate {
  switch (predicate.kind) {
    case "All":
    case "Any":
      return {
        kind: predicate.kind,
        items: predicate.items.map((p: Predicate) => rewriteClaimPredicateForBinding(p, claims)),
      };
    case "Not":
      return predicateContainsUnboundClaim(predicate.inner, claims)
        ? falsePredicate()
        : { kind: "Not", inner: rewriteClaimPredicateForBinding(predicate.inner, claims) };
    case "Eq":
    case "Ne":
    case "Contains":
      return operandsContainUnboundClaim([predicate.left, predicate.right], claims)
        ? falsePredicate()
        : predicate;
    case "In":
      return operandsContainUnboundClaim([predicate.operand, ...predicate.values], claims)
        ? falsePredicate()
        : predicate;
    case "Gt":
    case "Gte":
    case "Lt":
    case "Lte":
      return falsePredicate();
    case "EnumMatch":
      return { ...predicate, payload: rewriteClaimPredicateForBinding(predicate.payload, claims) };
    case "IsNull": {
      if (predicate.operand.kind !== "Claim") return predicate;
      const path = operandClaimPath(predicate.operand.name);
      const value = claims && policyClaimAtPath(claims, path);
      return value?.kind === "Nullable" && value.value == null
        ? { kind: "All", items: [] }
        : falsePredicate();
    }
  }
  return predicate;
}

export const defaultPermissionScopeClaimValues = (writer: AuthorSubject) =>
  defaultPolicyClaimValues(writer);

export function defaultPolicyClaimValues(writer: AuthorSubject): Map<string, Value> {
  const claims: Map<string, Value> = authorPolicyClaims(writer);
  claims.set("isAdmin", { kind: "Bool", value: false });
  return claims;
}

const BUILTIN_POLICY_CLAIMS = ["user", "isAdmin"];

const isBuiltinPolicyClaim = (name: string) =>
  BUILTIN_POLICY_CLAIMS.includes(name) ||
  ["user.account", "user.identity", "user.identity.issuer", "user.identity.subject"].includes(name);

export function bindScopeClaimOperands(
  query: JazzQuery,
  claimValues: Map<string, Value>,
  bindingValues: Map<string, Value>,
): void {
  query.forEachConditionMut((p: Predicate) =>
    bindScopeClaimPredicate(p, claimValues, bindingValues),
  );
}

/** Mutates `predicate` in place (Rust `&mut Predicate`). */
function bindScopeClaimPredicate(
  predicate: Predicate,
  claimValues: Map<string, Value>,
  bindingValues: Map<string, Value>,
): void {
  const replace = (next: Predicate) => {
    for (const k of Object.keys(predicate)) delete predicate[k];
    Object.assign(predicate, next);
  };
  switch (predicate.kind) {
    case "Not":
      if (predicateContainsUnboundClaim(predicate.inner, claimValues)) replace(falsePredicate());
      else bindScopeClaimPredicate(predicate.inner, claimValues, bindingValues);
      return;
    case "IsNull":
      if (predicate.operand.kind === "Claim")
        replace(rewriteClaimPredicateForBinding({ ...predicate }, claimValues));
      else bindScopeClaimOperand(predicate, "operand", claimValues, bindingValues);
      return;
    case "All":
    case "Any":
      for (const p of predicate.items) bindScopeClaimPredicate(p, claimValues, bindingValues);
      return;
    case "Eq":
    case "Ne":
    case "Gt":
    case "Gte":
    case "Lt":
    case "Lte":
    case "Contains":
      bindScopeClaimOperand(predicate, "left", claimValues, bindingValues);
      bindScopeClaimOperand(predicate, "right", claimValues, bindingValues);
      return;
    case "In":
      bindScopeClaimOperand(predicate, "operand", claimValues, bindingValues);
      for (let i = 0; i < predicate.values.length; i++)
        bindScopeClaimOperand(predicate.values, i, claimValues, bindingValues);
      return;
    case "EnumMatch":
      bindScopeClaimPredicate(predicate.payload, claimValues, bindingValues);
  }
}

function bindScopeClaimOperand(
  holder: any,
  slot: string | number,
  claimValues: Map<string, Value>,
  bindingValues: Map<string, Value>,
): void {
  const operand: Operand = holder[slot];
  if (operand.kind !== "Claim") return;
  const path = operandClaimPath(operand.name);
  const value = policyClaimAtPath(claimValues, path);
  if (value === undefined) return;
  const param = claimParamField(path);
  bindingValues.set(param, value);
  holder[slot] = { kind: "Param", name: param };
}

export function disambiguatePolicyClaimParams(
  query: JazzQuery,
  schema: RuntimeSchema,
  bindingValues: Map<string, Value>,
): SortedMap<string, ProgramClaimParam> {
  return disambiguatePolicyClaimParamsWithOuterSlots(
    query,
    schema,
    bindingValues,
    new SortedMap(cmpStr),
  );
}

export function disambiguatePolicyClaimParamsWithOuterSlots(
  query: JazzQuery,
  schema: RuntimeSchema,
  bindingValues: Map<string, Value>,
  outerSlots: SortedMap<string, ProgramClaimParam>,
): SortedMap<string, ProgramClaimParam> {
  const shape = query.validateRuntime(schema);
  const aliases = new Map<string, string>();
  const claims = new SortedMap<string, ProgramClaimParam>(cmpStr);
  for (const [name, ty] of shape.params()) {
    const path = claimPathFromParamField(name);
    if (path === undefined) continue;
    let alias: string | undefined;
    for (const [slotName, slot] of outerSlots)
      if (pathEq(slot.path, path) && debugFmt(slot.ty) === debugFmt(ty)) {
        alias = slotName;
        break;
      }
    alias ??= typedClaimParamAlias(name, ty);
    aliases.set(name, alias);
    claims.set(alias, { path, ty });
  }
  renameQueryParams(query, aliases);
  for (const [name, alias] of aliases) {
    const value = bindingValues.get(name);
    if (value !== undefined) {
      bindingValues.delete(name);
      bindingValues.set(alias, value);
    }
  }
  return claims;
}

const pathEq = (a: string[], b: string[]) => a.length === b.length && a.every((s, i) => s === b[i]);

export function typedClaimParamAlias(name: string, ty: ColumnType): string {
  const t = debugFmt(ty);
  return `__jazz_claim_typed:${t.length}:${t}:${name}`;
}

function renameQueryParams(query: JazzQuery, aliases: Map<string, string>): void {
  const rename = (operand: Operand) => {
    if (operand.kind === "Param") operand.name = aliases.get(operand.name) ?? operand.name;
  };
  query.forEachConditionMut((p: any) => p.forEachOperandMut(rename));
  for (const reachable of [
    ...query.reachable,
    ...query.policyBranches.flatMap((b: any) => b.reachable),
  ])
    rename(reachable.from);
}

function falsePredicate(): Predicate {
  return {
    kind: "Eq",
    left: { kind: "Literal", value: { kind: "Bool", value: true } },
    right: { kind: "Literal", value: { kind: "Bool", value: false } },
  };
}

function predicateContainsUnboundClaim(predicate: Predicate, claims: Claims): boolean {
  return predicate.anyOperand((o: Operand) => operandContainsUnboundClaim(o, claims));
}

const operandsContainUnboundClaim = (operands: Operand[], claims: Claims) =>
  operands.some((o) => operandContainsUnboundClaim(o, claims));

function operandContainsUnboundClaim(operand: Operand, claims: Claims): boolean {
  return (
    operand.kind === "Claim" &&
    !isBuiltinPolicyClaim(operand.name) &&
    !(claims && policyClaimAtPath(claims, operandClaimPath(operand.name)) !== undefined)
  );
}

export enum ParamBindingMode {
  InlineAllReachableSeeds,
  RetainAllParams,
}

export const bindingUserParamsCacheKey = (params: Map<string, ColumnType>) => debugFmt(params);
export const bindingClaimParamsCacheKey = (params: Iterable<[string, ProgramClaimParam]>) =>
  debugFmt(params);

export function bindQueryParamsWithMode(
  shape: ValidatedQuery,
  binding: Binding,
  schema: RuntimeSchema,
  mode: ParamBindingMode,
): ValidatedQuery {
  const query = shape.query().clone();
  const rootSource = rootSourceId(query.table);
  const flatJoin = query.flatJoin;
  if (flatJoin) {
    const rootScope = flatJoin.rootAlias ?? query.table;
    const sourceTables = new Map<string, string>([[rootScope, query.table]]);
    for (const source of flatJoin.sources)
      sourceTables.set(source.alias ?? source.table, source.table);
    query.filters = query.filters.map((p: Predicate) =>
      bindFlatJoinPredicate(p, binding, schema, mode, rootScope, sourceTables),
    );
  } else query.filters = bindPredicates(query.filters, binding, schema, rootSource, mode);
  query.joins = query.joins.map((j: JoinVia) => bindJoinFilterLiterals(j, binding, schema, mode));
  query.reachable = query.reachable.map((r: ReachableVia) =>
    bindReachable(r, binding, schema, mode),
  );
  query.arraySubqueries = query.arraySubqueries.map((s: ArraySubquery) =>
    bindArraySubqueryFilterLiterals(s, binding, schema, mode),
  );
  query.policyBranches = query.policyBranches.map((branch: any) => ({
    ...branch,
    filters: bindPredicates(branch.filters, binding, schema, rootSource, mode),
    joins: branch.joins.map((j: JoinVia) => bindJoinFilterLiterals(j, binding, schema, mode)),
    reachable: branch.reachable.map((r: ReachableVia) => bindReachable(r, binding, schema, mode)),
  }));
  const rebound = query.validateWithSchemaVersion(schema, shape.schemaVersion());
  if (rebound.schemaVersion() !== shape.schemaVersion())
    throw Err.InvalidStoredValue("bound query schema changed");
  return rebound;
}

function bindFlatJoinPredicate(
  predicate: Predicate,
  binding: Binding,
  schema: RuntimeSchema,
  mode: ParamBindingMode,
  rootScope: string,
  sourceTables: Map<string, string>,
): Predicate {
  if (predicate.kind === "All")
    return {
      kind: "All",
      items: predicate.items.map((p: Predicate) =>
        bindFlatJoinPredicate(p, binding, schema, mode, rootScope, sourceTables),
      ),
    };
  const sources: Iterable<string> = flatJoinPredicateSources(predicate);
  const scope = [...sources][0] ?? rootScope;
  const table = sourceTables.get(scope);
  if (table === undefined)
    throw Err.QueryLowering(`flat join filter references unknown source ${scope}`);
  const unqualified = unqualifyFlatJoinPredicate(predicate, scope);
  const bound = bindQueryPredicate(unqualified, binding, schema, bindSourceForTable(table), mode);
  return qualifyFlatJoinSourcePredicate(bound, scope);
}

function bindArraySubqueryFilterLiterals(
  subquery: ArraySubquery,
  binding: Binding,
  schema: RuntimeSchema,
  mode: ParamBindingMode,
): ArraySubquery {
  const source = bindSourceForTable(subquery.table);
  return {
    ...subquery,
    filters: bindPredicates(subquery.filters, binding, schema, source, mode),
    nestedArrays: subquery.nestedArrays.map((n: ArraySubquery) =>
      bindArraySubqueryFilterLiterals(n, binding, schema, mode),
    ),
  };
}

export function inlineSnapshotBindFilterLiterals(
  shape: ValidatedQuery,
  binding: Binding,
  schema: RuntimeSchema,
): ValidatedQuery {
  return bindQueryParamsWithMode(shape, binding, schema, ParamBindingMode.InlineAllReachableSeeds);
}

export function retargetBindingValueSources(
  shape: NormalizedRowSetShape,
  bindingSourceShape: string,
): void {
  for (const [, node] of shape.nodes)
    if (node.kind === "ValueSource" && node.mode === "Binding") node.shape = bindingSourceShape;
}

export function bindingClaimParamsForShape(
  shape: NormalizedRowSetShape,
  paramTypes: Map<string, ColumnType>,
): SortedMap<string, ProgramClaimParam> {
  const params = new SortedMap<string, ProgramClaimParam>(cmpStr);
  for (const [, node] of shape.nodes) {
    if (node.kind === "ValueSource" && node.mode === "Binding")
      for (const column of node.columns)
        if (column.value.kind === "Claim")
          params.set(claimParamField(column.value.path), {
            path: column.value.path,
            ty: column.ty,
          });
    collectClaimFieldParamsFromNode(node, paramTypes, params);
  }
  return params;
}

export function normalizedSourceTables(shape: NormalizedRowSetShape): Set<string> {
  const out = new Set<string>();
  for (const [, node] of shape.nodes) if (node.kind === "Source") out.add(node.source.table);
  for (const source of shape.auxiliarySources) out.add(source.table);
  return out;
}

export function collectReachableSeedClaimParams(
  schema: RuntimeSchema,
  query: JazzQuery,
  params: SortedMap<string, ProgramClaimParam>,
): void {
  for (const reachable of [
    ...query.reachable,
    ...query.policyBranches.flatMap((b: any) => b.reachable),
  ]) {
    const seed = reachable.seed;
    if (!seed || seed.userColumn === undefined || seed.userClaim === undefined) continue;
    const ty = schemaColumnType(schema, seed.table, seed.userColumn);
    const path = operandClaimPath(seed.userClaim);
    params.set(claimParamField(path), { path, ty });
  }
}

const UUID: ColumnType = { kind: "Uuid" };

function collectClaimFieldParamsFromNode(
  node: RowSetExpr,
  paramTypes: Map<string, ColumnType>,
  params: SortedMap<string, ProgramClaimParam>,
): void {
  const auth = (value: NormalizedValueRef, ty: ColumnType) =>
    collectClaimFieldParamAuthoritative(value, ty, params);
  switch (node.kind) {
    case "Filter":
      return collectClaimFieldParamsFromPredicate(node.predicate, paramTypes, params);
    case "Join":
      return collectClaimFieldParamsFromPredicate(node.on, paramTypes, params);
    case "RecursiveRelation":
      auth(node.frontierKey, UUID);
      for (const key of node.dedupeKeys) auth(key, UUID);
      return;
    case "Project":
      for (const column of node.columns) auth(column.value, column.output.ty);
      return;
    case "Distinct":
      for (const key of node.keys) auth(key, UUID);
      return;
    case "CorrelatedPathProjection":
      return collectClaimFieldParamsFromPredicate(node.correlation, paramTypes, params);
    case "OrderBy":
      for (const key of node.keys) auth(key.value, UUID);
      return;
    case "Slice":
      for (const value of [...node.partitionBy, ...(node.tieBreaker ?? [])]) auth(value, UUID);
      return;
    case "Aggregate":
      for (const value of node.groupBy) auth(value, UUID);
      for (const output of node.outputs)
        if (output.input !== undefined) auth(output.input, output.output.ty);
      return;
  }
}

function collectClaimFieldParamsFromPredicate(
  predicate: NormalizedPredicateExpr,
  paramTypes: Map<string, ColumnType>,
  params: SortedMap<string, ProgramClaimParam>,
): void {
  predicate.tryForEachValue((value: NormalizedValueRef) => {
    if (value.kind !== "Param") return;
    const path = claimPathFromParamField(value.name);
    const ty = paramTypes.get(value.name);
    if (path === undefined || ty === undefined) return;
    if (!params.has(value.name)) params.set(value.name, { path, ty });
  });
}

function collectClaimFieldParamAuthoritative(
  value: NormalizedValueRef,
  ty: ColumnType,
  params: SortedMap<string, ProgramClaimParam>,
): void {
  if (value.kind !== "Param") return;
  const path = claimPathFromParamField(value.name);
  if (path !== undefined) params.set(value.name, { path, ty });
}

function bindQueryPredicate(
  predicate: Predicate,
  binding: Binding,
  schema: RuntimeSchema,
  source: SourceId,
  mode: ParamBindingMode,
): Predicate {
  switch (predicate.kind) {
    case "All":
    case "Any":
      return {
        kind: predicate.kind,
        items: bindPredicates(predicate.items, binding, schema, source, mode),
      };
    case "Not":
      return {
        kind: "Not",
        inner: bindQueryPredicate(predicate.inner, binding, schema, source, mode),
      };
    case "Eq":
    case "Ne":
    case "Gt":
    case "Gte":
    case "Lt":
    case "Lte": {
      const leftType = operandColumnType(schema, source, predicate.left);
      const rightType = operandColumnType(schema, source, predicate.right);
      return {
        kind: predicate.kind,
        left: bindQueryOperandWithTargetType(predicate.left, binding, rightType, mode),
        right: bindQueryOperandWithTargetType(predicate.right, binding, leftType, mode),
      };
    }
    case "In": {
      const left = bindQueryOperand(predicate.operand, binding, mode);
      const targetType = operandColumnType(schema, source, left);
      return {
        kind: "In",
        operand: left,
        values: predicate.values.map((o: Operand) =>
          bindQueryOperandWithTargetType(o, binding, targetType, mode),
        ),
      };
    }
    case "Contains": {
      const left = bindQueryOperand(predicate.left, binding, mode);
      const needleType = containsNeedleType(schema, source, left);
      const right = bindQueryOperandWithTargetType(predicate.right, binding, needleType, mode);
      if (left.kind === "Literal" && left.value.kind === "Array") {
        const targetType = operandColumnType(schema, source, right);
        return {
          kind: "In",
          operand: right,
          values: left.value.value.map((v: Value) => ({
            kind: "Literal",
            value: targetType ? coerceLiteralForColumnType(v, targetType) : v,
          })),
        };
      }
      return { kind: "Contains", left, right };
    }
    case "IsNull":
      return { kind: "IsNull", operand: bindQueryOperand(predicate.operand, binding, mode) };
  }
  return predicate;
}

const bindPredicates = (
  predicates: Predicate[],
  binding: Binding,
  schema: RuntimeSchema,
  source: SourceId,
  mode: ParamBindingMode,
) => predicates.map((p) => bindQueryPredicate(p, binding, schema, source, mode));

function bindReachable(
  reachable: ReachableVia,
  binding: Binding,
  schema: RuntimeSchema,
  mode: ParamBindingMode,
): ReachableVia {
  const out = { ...reachable };
  if (reachable.from.kind === "Param" && mode === ParamBindingMode.InlineAllReachableSeeds)
    out.from = bindQueryOperand(reachable.from, binding, mode);
  out.accessFilters = bindPredicates(
    reachable.accessFilters,
    binding,
    schema,
    bindSourceForTable(reachable.accessTable),
    mode,
  );
  out.edgeFilters = bindPredicates(
    reachable.edgeFilters,
    binding,
    schema,
    bindSourceForTable(reachable.edgeTable),
    mode,
  );
  if (out.seed)
    out.seed = {
      ...out.seed,
      filters: bindPredicates(
        out.seed.filters,
        binding,
        schema,
        bindSourceForTable(out.seed.table),
        mode,
      ),
    };
  return out;
}

function bindJoinFilterLiterals(
  join: JoinVia,
  binding: Binding,
  schema: RuntimeSchema,
  mode: ParamBindingMode,
): JoinVia {
  return {
    ...join,
    filters: bindPredicates(join.filters, binding, schema, bindSourceForTable(join.table), mode),
    nestedJoins: join.nestedJoins.map((j: JoinVia) =>
      bindJoinFilterLiterals(j, binding, schema, mode),
    ),
  };
}

const bindSourceForTable = (table: string): SourceId => ({ table, path: { components: [] } });

const bindQueryOperand = (operand: Operand, binding: Binding, mode: ParamBindingMode) =>
  bindQueryOperandWithTargetType(operand, binding, undefined, mode);

function bindQueryOperandWithTargetType(
  operand: Operand,
  binding: Binding,
  targetType: ColumnType | undefined,
  mode: ParamBindingMode,
): Operand {
  const coerce = (v: Value) => (targetType ? coerceLiteralForColumnType(v, targetType) : v);
  switch (operand.kind) {
    case "Param": {
      if (mode === ParamBindingMode.RetainAllParams) return operand;
      const value = binding.values().get(operand.name);
      if (value === undefined) throw QueryError.MissingParam(operand.name);
      return { kind: "Literal", value: coerce(value) };
    }
    case "Literal":
      return { kind: "Literal", value: coerce(operand.value) };
  }
  return operand;
}

function programBindingForShape(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  sourceShape: string | undefined,
  extraUserParams: Map<string, ColumnType>,
  claimParams: SortedMap<string, ProgramClaimParam>,
): ProgramBinding {
  return {
    id: binding.bindingId(),
    sourceShape,
    extraUserParams,
    paramTypes: new Map([...shape.params(), ...extraUserParams]),
    claimParams,
    values: new Map(binding.values()),
  };
}

function programBindingForShapeAndPolicy(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  sourceShape: string | undefined,
  extraUserParams: Map<string, ColumnType>,
  claimParams: SortedMap<string, ProgramClaimParam>,
  policy: PolicyContext,
): ProgramBinding {
  return this.programBindingForShapeAndPolicyWithPreparedClaimMode(
    shape,
    binding,
    sourceShape,
    extraUserParams,
    claimParams,
    policy,
    PreparedClaimBindingMode.Strict,
  );
}

function programBindingForShapeAndPolicyWithPreparedClaimMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  sourceShape: string | undefined,
  extraUserParams: Map<string, ColumnType>,
  claimParams: SortedMap<string, ProgramClaimParam>,
  policy: PolicyContext,
  mode: PreparedClaimBindingMode,
): ProgramBinding {
  if (policy.kind === "System") {
    const paramTypes = new Map<string, any>([...shape.params(), ...extraUserParams]);
    sourceShape =
      sourceShape === undefined
        ? undefined
        : queryBindingSourceShapeForPartsIfNeeded(paramTypes, new Map<string, any>());
    claimParams = new SortedMap(cmpStr);
  }
  const programBinding = this.programBindingForShape(
    shape,
    binding,
    sourceShape,
    extraUserParams,
    claimParams.clone(),
  );
  if (claimParams.size) {
    const values = new Map(binding.values());
    for (const [name, claim] of claimParams) {
      const value = preparedClaimValue(claim.path, policy);
      if (value === undefined) {
        if (mode === PreparedClaimBindingMode.FailClosedAuthorizationSupport)
          throw Err.AuthorizationSupportMissingClaim(claim.path.join("."));
        if (policy.kind === "AuthorizationSubplan")
          throw Err.QueryCapability(
            `policy authorization requires unbound claim ${claim.path.join(".")}`,
          );
        throw Err.InvalidStoredValue("claim prepared param is not bound");
      }
      values.set(name, coercePreparedBindingValue(value, claim.ty));
    }
    programBinding.id = bindingIdForValues(values);
    programBinding.values = values;
  }
  return programBinding;
}

installNodeMethods({
  programBindingForShape,
  programBindingForShapeAndPolicy,
  programBindingForShapeAndPolicyWithPreparedClaimMode,
});
