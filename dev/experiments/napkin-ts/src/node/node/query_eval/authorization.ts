// Query authorization semantics and policy-proof graph construction.
//
// Selects operation-specific policies and policy-owning schemas, constructs
// read/write authorization requests and support scopes, and caches executable
// proof graphs. Parameter-slot and claim rewriting mechanics live in bindings.
import { blake3, SortedMap, cmpStr } from "../../../support/index.js";
import type {
  NodeState,
  GraphBuilder,
  ProjectField,
  JazzQuery,
  Value,
  RuntimeSchema,
  ValidatedQuery,
  Binding,
  Predicate,
  Operand,
  PermissionAdviceAction,
  TableSchema,
  RowUuid,
  AuthorSubject,
  OpenTransactionId,
  SchemaVersionId,
  QueryProgramRequest,
  SourceId,
  CurrentAccessPath,
  PolicyContext,
  RowSetProgramInput,
  QueryProgram,
  RowProvenance,
  ColumnType,
  ProgramClaimParam,
  GlobalTime,
  AuthorizationSupportScopeKey,
  AuthorizationOperationKey,
  RegisterShapeOptions as RegisterShapeOptionsT,
  DurabilityTier as Tier,
} from "../../../_extern/node-qe-b.js";
import {
  Err,
  installNodeMethods,
  GraphBuilderStatic,
  emptyAuthorizedRowIdGraph,
  col,
  eq,
  lit,
  policyClaimAtPath,
  operandClaimPath,
  authorPolicyClaims,
  QueryAuthorizationMode,
  DurabilityTier,
  rootSourceId,
  currentQueryOutputRequest,
  PolicyDecisionRole,
  PolicyEnforcementMode,
  AuthorSubjectStatic,
  TransactionWriteOverlay,
  currentRowFromCellsWithExplicitProvenance,
  RecordDescriptorNew,
  ProjectFieldRenamed,
  ProjectExprField,
  FieldRefStoredName,
  leftField,
  rightField,
  JazzQueryFrom,
  InheritsOperation,
  globalCurrentStorageFields,
  layerStack,
  notDeletedPredicate,
  RegisterShapeOptions,
  postcardToAllocvec,
  valueDiscriminant,
  valueEq,
} from "../../../_extern/node-qe-b.js";
import {
  CurrentQueryProgramOutput,
  JAZZ_APP_ROWS_SINK,
  takeRequiredSinkDeltas,
} from "../query_eval.js";
import {
  authorizationQueryFromReadPolicy,
  rewriteClaimJoinForBinding,
  rewriteClaimPredicateForBinding,
  bindScopeClaimOperands,
  defaultPolicyClaimValues,
  defaultPermissionScopeClaimValues,
  ParamBindingMode,
  PreparedClaimBindingMode,
  bindQueryParamsWithMode,
  bindingClaimParamsForShape,
  disambiguatePolicyClaimParamsWithOuterSlots,
  collectReachableSeedClaimParams,
  retargetBindingValueSources,
  bindingUserParamsCacheKey,
  bindingClaimParamsCacheKey,
} from "./bindings.js";
import {
  coerceBindingValuesForShape,
  queryBindingSourceShapeForPartsIfNeeded,
  authorizationBindingSourceShape,
  bindingValuesForPlan,
} from "./prepared_bindings.js";
import { currentQueryReadSet, historicalQueryReadSet, txQueryReadSet } from "./query_read_sets.js";
import {
  loweredTerminalGraph,
  outputRoutingFieldsForQueryEval,
  policyAuthorizationGraphCacheKey,
  type PolicyAuthorizationGraph,
} from "./lowering.js";

type Claims = Map<string, Value>;

/** Exact, action-specific policy support compiled for a hypothetical operation. */
export interface AuthorizationSupportScope {
  key: AuthorizationSupportScopeKey;
  operation: AuthorizationOperationKey;
  /** The sole read/serving semantics under which support can authorize an operation. */
  options: RegisterShapeOptionsT;
  subscriptions: [ValidatedQuery, Binding][];
}

export enum AuthorizationScopeOperation {
  Read,
  Insert,
  Update,
  Delete,
}

function emptyPolicyFilteredCurrentSourceGraph(
  base: GraphBuilder,
  outputFields: ProjectField[],
): PolicyAuthorizationGraph {
  const keys = ["row_uuid"];
  return {
    graph: GraphBuilderStatic.join(base, emptyAuthorizedRowIdGraph(), keys, keys).projectFields([
      ...outputFields,
    ]),
    routeFields: new Set<string>(),
    accessPaths: new SortedMap<SourceId, CurrentAccessPath>(cmpStr as any),
  } as PolicyAuthorizationGraph;
}

function compilePermissionScopePolicy(
  query: JazzQuery,
  claims: Claims | undefined,
  claimValues: Claims,
  schema: RuntimeSchema,
): [ValidatedQuery, Binding] {
  const rewrite = (p: Predicate) => rewriteClaimPredicateForBinding(p, claims);
  query.filters = query.filters.map(rewrite);
  query.joins = query.joins.map((j: any) => rewriteClaimJoinForBinding(j, claims));
  query.reachable = query.reachable.map((reachable: any) => {
    reachable.accessFilters = reachable.accessFilters.map(rewrite);
    reachable.edgeFilters = reachable.edgeFilters.map(rewrite);
    if (reachable.seed) reachable.seed.filters = reachable.seed.filters.map(rewrite);
    return reachable;
  });
  pruneSessionUnsatisfiableBranches(query, claimValues);
  const values: Claims = new Map();
  bindScopeClaimOperands(query, claimValues, values);
  const shape = query.validateRuntime(schema);
  coerceBindingValuesForShape(shape, values);
  return [shape, shape.bind(values)];
}

/**
 * Drops the policy alternatives this session's claims can never satisfy.
 * When every alternative would be dropped, the query is left unchanged, so an
 * empty branch list is never mistaken for an unrestricted one.
 */
export function pruneSessionUnsatisfiableBranches(query: JazzQuery, claimValues: Claims): void {
  const satisfiable: boolean[] = query.policyBranches.map(
    (branch: any) =>
      !branch.filters.some((p: Predicate) => claimOnlyPredicateValue(p, claimValues) === false),
  );
  if (satisfiable.every((k) => k) || !satisfiable.some((k) => k)) return;
  query.policyBranches = query.policyBranches.filter((_: any, i: number) => satisfiable[i] ?? true);
}

/** Exact value of a claims/literals-only predicate, or undefined when undecidable. */
function claimOnlyPredicateValue(predicate: Predicate, claimValues: Claims): boolean | undefined {
  switch (predicate.kind) {
    case "All": {
      let all: boolean | undefined = true;
      for (const p of predicate.items) {
        const v = claimOnlyPredicateValue(p, claimValues);
        if (v === false) return false;
        if (v === undefined) all = undefined;
      }
      return all;
    }
    case "Any": {
      let any: boolean | undefined = false;
      for (const p of predicate.items) {
        const v = claimOnlyPredicateValue(p, claimValues);
        if (v === true) return true;
        if (v === undefined) any = undefined;
      }
      return any;
    }
    case "Not": {
      const v = claimOnlyPredicateValue(predicate.inner, claimValues);
      return v === undefined ? undefined : !v;
    }
    case "Eq":
      return claimOnlyOperandsEqual(predicate.left, predicate.right, claimValues);
    case "Ne": {
      const v = claimOnlyOperandsEqual(predicate.left, predicate.right, claimValues);
      return v === undefined ? undefined : !v;
    }
    case "In": {
      let any: boolean | undefined = false;
      for (const value of predicate.values) {
        const v = claimOnlyOperandsEqual(predicate.operand, value, claimValues);
        if (v === true) return true;
        if (v === undefined) any = undefined;
      }
      return any;
    }
    default:
      return undefined;
  }
}

function claimOnlyOperandsEqual(
  left: Operand,
  right: Operand,
  claimValues: Claims,
): boolean | undefined {
  const l = claimOnlyOperandValue(left, claimValues);
  if (l === undefined) return undefined;
  const r = claimOnlyOperandValue(right, claimValues);
  if (r === undefined) return undefined;
  return valueDiscriminant(l) === valueDiscriminant(r) ? valueEq(l, r) : undefined;
}

function claimOnlyOperandValue(operand: Operand, claimValues: Claims): Value | undefined {
  let value: Value | undefined;
  if (operand.kind === "Literal") value = operand.value;
  else if (operand.kind === "Claim") {
    value = policyClaimAtPath(claimValues, operandClaimPath(operand.name));
    if (value === undefined) return undefined;
  } else return undefined;
  if (value.kind === "Nullable") return value.value ?? undefined;
  return value;
}

function authorizationScopeAction(
  action: PermissionAdviceAction,
): [AuthorizationScopeOperation, string] {
  switch (action.kind) {
    case "Read":
      return [AuthorizationScopeOperation.Read, action.table];
    case "Insert":
      return [AuthorizationScopeOperation.Insert, action.table];
    case "Update":
      return [AuthorizationScopeOperation.Update, action.table];
    default:
      return [AuthorizationScopeOperation.Delete, action.table];
  }
}

function authorizationPolicyQueries(
  table: TableSchema,
  operation: AuthorizationScopeOperation,
): JazzQuery[] {
  const wp = table.writePolicies;
  switch (operation) {
    // No dependency data can prove an absent read grant.
    case AuthorizationScopeOperation.Read:
      return table.readPolicy === undefined ? [] : [authorizationQueryFromReadPolicy(table)];
    case AuthorizationScopeOperation.Insert:
      return wp.insertCheck ? [wp.insertCheck.clone()] : [];
    case AuthorizationScopeOperation.Update:
      return [wp.updateUsing, wp.updateCheck]
        .filter((q) => q !== undefined)
        .map((q: any) => q.clone());
    case AuthorizationScopeOperation.Delete:
      return wp.deleteUsing ? [wp.deleteUsing.clone()] : [];
  }
}

/**
 * Policy clauses for one advice action, seeded with the target row where the
 * clause is evaluated on the stored row. `update_check` stays unseeded.
 */
function authorizationRowPolicyQueries(
  table: TableSchema,
  action: PermissionAdviceAction,
): JazzQuery[] {
  const seed = (policy: JazzQuery, row: RowUuid) =>
    policy.filter(eq(col("id"), lit({ kind: "Uuid", value: row })));
  switch (action.kind) {
    case "Read":
      return authorizationPolicyQueries(table, AuthorizationScopeOperation.Read).map((p) =>
        seed(p, action.row),
      );
    case "Insert":
      return authorizationPolicyQueries(table, AuthorizationScopeOperation.Insert);
    case "Update":
      return [
        table.writePolicies.updateUsing
          ? seed(table.writePolicies.updateUsing.clone(), action.row)
          : undefined,
        table.writePolicies.updateCheck?.clone(),
      ].filter((q) => q !== undefined);
    default:
      return authorizationPolicyQueries(table, AuthorizationScopeOperation.Delete).map((p) =>
        seed(p, action.row),
      );
  }
}

function authorizationOperationKey(
  operation: AuthorizationScopeOperation,
  table: string,
  action: PermissionAdviceAction,
  actionBytes: Uint8Array,
): AuthorizationOperationKey {
  return {
    operation,
    table,
    row: action.kind === "Insert" ? undefined : action.row,
    candidateDigest: blake3(actionBytes),
  };
}

function serializeOr(value: unknown, message: string): Uint8Array {
  const bytes = postcardToAllocvec(value);
  if (bytes === undefined) throw Err.InvalidStoredValue(message);
  return bytes;
}

function primaryKeyAccessPaths(
  table: string,
  rowUuid: RowUuid,
): SortedMap<SourceId, CurrentAccessPath> {
  const paths = new SortedMap<SourceId, CurrentAccessPath>(cmpStr as any);
  paths.set(rootSourceId(table), {
    kind: "PrimaryKey",
    values: [{ kind: "Uuid", value: rowUuid }],
  });
  return paths;
}

async function readPolicyQueryAllowsOpenTxRow(
  this: NodeState,
  txId: OpenTransactionId,
  policy: JazzQuery,
  policySchemaVersion: SchemaVersionId,
  rowUuid: RowUuid,
  identity: AuthorSubject,
): Promise<boolean> {
  const policySchema = this.catalogue.schemaForVersion(policySchemaVersion);
  if (!policySchema) throw Err.InvalidStoredValue("policy schema payload missing");
  // Keep the target coordinate in the policy AST as well so inline transaction
  // overlays cannot authorize one row from another visible row.
  const seeded = policy.clone().filter(eq(col("id"), lit({ kind: "Uuid", value: rowUuid })));
  const validated = seeded.validateWithSchemaVersion(policySchema, policySchemaVersion);
  const [policyShape, binding, input] = this.inlinePolicySubplanInput(validated, policySchema);
  const table = policyShape.query().table;
  const policyContext = this.authorizationSubplanPolicy(identity, table, PolicyDecisionRole.Read);
  const snapshot = this.openTx(txId).baseSnapshot.clone();
  const request: QueryProgramRequest = {
    authorizationMode: QueryAuthorizationMode.TrustedServing,
    reads: txQueryReadSet(input.shape, policyShape.schemaVersion(), txId, snapshot),
    policy: policyContext,
    input,
    output: currentQueryOutputRequest(
      CurrentQueryProgramOutput.PolicyPredicate,
      policyShape.query(),
    ),
  };
  const program = await this.compileQueryProgramRequestWithAccessPaths(
    request,
    primaryKeyAccessPaths(table, rowUuid),
  );
  return this.writePolicyQueryProgramAllows(program, policyShape, binding);
}

async function policyAuthorizationRowIdGraph(
  this: NodeState,
  request: QueryProgramRequest,
): Promise<PolicyAuthorizationGraph> {
  this.queryEngineReadMetrics.policyAuthorizationGraphs += 1;
  const cached = this.policyAuthorizationGraphCacheGet(policyAuthorizationGraphCacheKey(request));
  if (cached) return cached;
  return this.policyAuthorizationRowIdGraphInner(request, undefined, true, undefined);
}

async function pointPolicyAuthorizationRowIdGraph(
  this: NodeState,
  request: QueryProgramRequest,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
  boundedDeletionRegister: [SourceId, GraphBuilder] | undefined,
): Promise<PolicyAuthorizationGraph> {
  this.queryEngineReadMetrics.policyAuthorizationGraphs += 1;
  return this.policyAuthorizationRowIdGraphInner(
    request,
    accessPaths,
    false,
    boundedDeletionRegister,
  );
}

async function policyAuthorizationRowIdGraphInner(
  this: NodeState,
  request: QueryProgramRequest,
  forcedAccessPaths: SortedMap<SourceId, CurrentAccessPath> | undefined,
  cache: boolean,
  boundedDeletionRegister: [SourceId, GraphBuilder] | undefined,
): Promise<PolicyAuthorizationGraph> {
  const cacheKey = policyAuthorizationGraphCacheKey(request);
  this.restoreExpiredPolicyCompilationState();
  const policy: PolicyContext = request.policy;
  const proofTable: string | undefined =
    policy.kind === "AuthorizationSubplan" ? policy.protectedSource.table : undefined;
  const proofLease = proofTable !== undefined ? { alive: true } : undefined;
  if (proofTable !== undefined) {
    const stack = this.query.policyProofStack;
    const depth = stack.length;
    if (stack.some((active: any) => active.table === proofTable)) {
      throw Err.PolicyProofCycle({ table: proofTable, depth });
    }
    stack.push({ table: proofTable, lease: proofLease });
  }
  try {
    // Cached authorization dependencies deliberately do not carry secondary
    // index prefixes resolved from this identity's claims.
    const accessPaths = forcedAccessPaths ?? this.queryProgramAccessPaths(request, false);
    const program: QueryProgram = cache
      ? await this.compileQueryProgramRequestWithAccessPaths(request, accessPaths.clone())
      : await this.compileQueryProgramRequestWithSharedAccessPaths(
          request,
          accessPaths.clone(),
          boundedDeletionRegister,
        );
    const graph = loweredTerminalGraph(program, "policy.authorized_rows");
    const terminal = program.lowered.terminals.find(
      (t: any) => t.sink === "policy.authorized_rows",
    );
    const routeFields: Set<string> =
      terminal === undefined
        ? new Set()
        : terminal.output.kind === "Fact"
          ? outputRoutingFieldsForQueryEval(terminal.output.fact)
          : new Set();
    const result: PolicyAuthorizationGraph = {
      graph,
      routeFields,
      accessPaths,
    } as PolicyAuthorizationGraph;
    if (cache) this.query.policyAuthorizationGraphCache.set(cacheKey, result);
    return result;
  } finally {
    if (proofLease) {
      const entry = this.query.policyProofStack.pop();
      if (!entry) throw new Error("policy proof stack entry is balanced");
      proofLease.alive = false;
    }
  }
}

/** The policy context of an authorization subplan protecting `table` for `identity`. */
function authorizationSubplanPolicy(
  this: NodeState,
  identity: AuthorSubject,
  table: string,
  role: PolicyDecisionRole,
): PolicyContext {
  const ctx: PolicyContext = this.queryProgramPolicyContext(identity);
  if (ctx.kind !== "Identity") return ctx;
  return {
    kind: "AuthorizationSubplan",
    protectedSource: rootSourceId(table),
    role,
    mode: ctx.mode,
    permissionSubject: ctx.permissionSubject,
    claims: ctx.claims,
    attribution: ctx.attribution,
  };
}

/** Inline a policy query's reachable seeds and build its row-set input. */
function inlinePolicySubplanInput(
  this: NodeState,
  policyShape: ValidatedQuery,
  schema: RuntimeSchema,
): [ValidatedQuery, Binding, RowSetProgramInput] {
  const policyBinding = policyShape.bind(new Map());
  const shape = bindQueryParamsWithMode(
    policyShape,
    policyBinding,
    schema,
    ParamBindingMode.InlineAllReachableSeeds,
  );
  const binding = shape.bind(new Map());
  const inputShape = this.normalizedRowSetShape(shape, binding);
  const claimParams = bindingClaimParamsForShape(inputShape, shape.params());
  const input: RowSetProgramInput = {
    binding: this.programBindingForShape(
      shape,
      binding,
      queryBindingSourceShapeForPartsIfNeeded(shape.params(), claimParams),
      new Map(),
      claimParams,
    ),
    shape: inputShape,
  };
  return [shape, binding, input];
}

function queryProgramPolicyContext(this: NodeState, identity: AuthorSubject): PolicyContext {
  if (identity === AuthorSubjectStatic.SYSTEM) return { kind: "System" };
  const claims = defaultPolicyClaimValues(identity);
  const active = this.activeSessionClaims;
  const session =
    active !== undefined && active[0] === identity ? active[1] : this.sessionClaims.get(identity);
  if (session) for (const [k, v] of session) claims.set(k, v);
  for (const [k, v] of authorPolicyClaims(identity)) claims.set(k, v);
  return {
    kind: "Identity",
    mode: PolicyEnforcementMode.Enforcing,
    permissionSubject: identity,
    claims,
    attribution: undefined,
  };
}

async function writePolicyQueryAllowsCurrentRow(
  this: NodeState,
  policy: JazzQuery,
  rowUuid: RowUuid,
  identity: AuthorSubject,
): Promise<boolean> {
  const validated = policy.validate(this.catalogue.schema);
  const [policyShape, binding, input] = this.inlinePolicySubplanInput(
    validated,
    this.catalogue.schema,
  );
  const table = policyShape.query().table;
  const request: QueryProgramRequest = {
    authorizationMode: QueryAuthorizationMode.TrustedServing,
    reads: currentQueryReadSet(
      input.shape,
      policyShape.schemaVersion(),
      policyShape.schemaVersion(),
      DurabilityTier.Local,
      undefined,
      undefined,
      false,
    ),
    policy: this.authorizationSubplanPolicy(identity, table, PolicyDecisionRole.Write),
    input,
    output: currentQueryOutputRequest(
      CurrentQueryProgramOutput.PolicyPredicate,
      policyShape.query(),
    ),
  };
  // A primary-key access path addresses the physical row UUID without
  // overloading public `id`, which may be a declared user column.
  const program = await this.compileQueryProgramRequestWithAccessPaths(
    request,
    primaryKeyAccessPaths(table, rowUuid),
  );
  return this.writePolicyQueryProgramAllows(program, policyShape, binding);
}

/**
 * Authorize an inline old/candidate row through the query program. Insert
 * candidates reinterpret plain `inherits(parent)` as parent update-using.
 */
async function writePolicyQueryAllowsCandidate(
  this: NodeState,
  table: TableSchema,
  policy: JazzQuery,
  rowUuid: RowUuid,
  cells: Map<string, Value>,
  identity: AuthorSubject,
  insertCandidate: boolean,
): Promise<boolean> {
  const cat = this.catalogue;
  const has = (tables: TableSchema[]) => tables.some((known) => known.equals(table));
  let version: SchemaVersionId;
  if (has(cat.activeSchema.compiled.tables)) version = cat.activeSchema.schema;
  else if (has(cat.schema.tables)) version = cat.localSchemaVersionId;
  else {
    version = cat.localSchemaVersionId;
    for (const [schemaVersion, payload] of cat.catalogueSchemas) {
      if (has(payload.schema.tables)) {
        version = schemaVersion;
        break;
      }
    }
  }
  return this.writePolicyQueryAllowsCandidateForSchema(
    version,
    table,
    policy,
    rowUuid,
    cells,
    identity,
    insertCandidate,
  );
}

async function writePolicyQueryAllowsCandidateForSchema(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  table: TableSchema,
  policy: JazzQuery,
  rowUuid: RowUuid,
  cells: Map<string, Value>,
  identity: AuthorSubject,
  insertCandidate: boolean,
): Promise<boolean> {
  const provenance: RowProvenance = {
    createdBy: identity,
    createdAt: 0,
    updatedBy: identity,
    updatedAt: 0,
  };
  return this.writePolicyQueryAllowsCandidateWithProvenanceForSchema(
    policySchemaVersion,
    table,
    policy,
    rowUuid,
    cells,
    identity,
    insertCandidate,
    provenance,
  );
}

/** Authorize an inline candidate with the provenance the pending version will expose. */
async function writePolicyQueryAllowsCandidateWithProvenanceForSchema(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  table: TableSchema,
  policy: JazzQuery,
  rowUuid: RowUuid,
  cells: Map<string, Value>,
  identity: AuthorSubject,
  insertCandidate: boolean,
  provenance: RowProvenance,
): Promise<boolean> {
  return this.policyQueryAllowsCandidateWithProvenanceForSchema(
    policySchemaVersion,
    table,
    policy,
    rowUuid,
    cells,
    identity,
    insertCandidate,
    provenance,
    PolicyDecisionRole.Write,
    TransactionWriteOverlay.default(),
  );
}

/** Policy evidence is committed state overlaid with its own transaction's other writes (`INV-RLS-9`). */
async function writePolicyQueryAllowsCandidateOverTransaction(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  table: TableSchema,
  policy: JazzQuery,
  rowUuid: RowUuid,
  cells: Map<string, Value>,
  identity: AuthorSubject,
  insertCandidate: boolean,
  provenance: RowProvenance,
  transactionOverlay: any,
): Promise<boolean> {
  return this.policyQueryAllowsCandidateWithProvenanceForSchema(
    policySchemaVersion,
    table,
    policy,
    rowUuid,
    cells,
    identity,
    insertCandidate,
    provenance,
    PolicyDecisionRole.Write,
    transactionOverlay,
  );
}

/** Read-for-write visibility of a row a commit unit acts on (`INV-RLS-9`). */
async function readPolicyQueryAllowsCandidateWithProvenanceForSchema(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  table: TableSchema,
  policy: JazzQuery,
  rowUuid: RowUuid,
  cells: Map<string, Value>,
  identity: AuthorSubject,
  provenance: RowProvenance,
): Promise<boolean> {
  return this.policyQueryAllowsCandidateWithProvenanceForSchema(
    policySchemaVersion,
    table,
    policy,
    rowUuid,
    cells,
    identity,
    false,
    provenance,
    PolicyDecisionRole.Read,
    TransactionWriteOverlay.acceptedState(),
  );
}

async function policyQueryAllowsCandidateWithProvenanceForSchema(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  table: TableSchema,
  policyIn: JazzQuery,
  rowUuid: RowUuid,
  cells: Map<string, Value>,
  identity: AuthorSubject,
  insertCandidate: boolean,
  provenance: RowProvenance,
  role: PolicyDecisionRole,
  transactionOverlay: any,
): Promise<boolean> {
  const policy = policyIn.clone();
  if (insertCandidate) {
    const promote = (inherits: any) => {
      if (inherits.operation === InheritsOperation.Select)
        inherits.operation = InheritsOperation.Update;
    };
    policy.inherits.forEach(promote);
    for (const branch of policy.policyBranches) branch.inherits.forEach(promote);
  }
  const policySchema = this.catalogue.schemaForVersion(policySchemaVersion);
  if (!policySchema) throw Err.InvalidStoredValue("policy schema payload missing");
  const validated = policy.clone().validateWithSchemaVersion(policySchema, policySchemaVersion);
  const [policyShape, binding, input] = this.inlinePolicySubplanInput(validated, policySchema);
  const tableName = policyShape.query().table;
  const request: QueryProgramRequest = {
    authorizationMode: QueryAuthorizationMode.TrustedServing,
    reads: currentQueryReadSet(
      input.shape,
      policyShape.schemaVersion(),
      policyShape.schemaVersion(),
      transactionOverlay.evidenceTier(),
      undefined,
      undefined,
      false,
    ),
    policy: this.authorizationSubplanPolicy(identity, tableName, role),
    input,
    output: currentQueryOutputRequest(
      CurrentQueryProgramOutput.PolicyPredicate,
      policyShape.query(),
    ),
  };
  const candidate = currentRowFromCellsWithExplicitProvenance(
    table,
    rowUuid,
    cells,
    provenance,
    undefined,
  );
  const inlineSources = new SortedMap<SourceId, any[]>(cmpStr as any);
  inlineSources.set(rootSourceId(tableName), [candidate]);
  const accessPaths = this.currentQueryPrimaryKeyAccessPaths(policyShape, binding);
  const program = !transactionOverlay.isActive()
    ? await this.compileQueryProgramRequestWithInlineSourcesAndAccessPaths(
        request,
        inlineSources,
        accessPaths,
      )
    : await this.compileQueryProgramRequestWithInlineSourcesAndTransactionOverlay(
        request,
        inlineSources,
        accessPaths,
        transactionOverlay.clone(),
      );
  return this.writePolicyQueryProgramAllows(program, policyShape, binding);
}

async function writePolicyQueryProgramAllows(
  this: NodeState,
  program: QueryProgram,
  policyShape: ValidatedQuery,
  binding: Binding,
): Promise<boolean> {
  const plan = await this.preparedQueryPlanFromProgram(program, policyShape, binding);
  let deltas: [unknown, number][];
  switch (plan.kind) {
    case "Graph":
      try {
        deltas = await this.database.queryGraph(plan.graph);
      } catch (e) {
        throw Err.Groove(e);
      }
      break;
    case "Prepared": {
      const values = bindingValuesForPlan(
        binding,
        plan.params,
        program.request.policy,
        PreparedClaimBindingMode.Strict,
      );
      deltas = takeRequiredSinkDeltas(
        await this.bindShapeSnapshot(plan.shape, values),
        JAZZ_APP_ROWS_SINK,
      );
      break;
    }
    default:
      throw Err.InvalidStoredValue("peer maintained marker cannot execute write policy plan");
  }
  return deltas.some(([, weight]) => weight > 0);
}

/** Evaluate a validated query shape against this node's local knowledge. */
async function queryRows(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
): Promise<any[]> {
  this.requireCatalogueReady();
  return this.queryRowsWithPreparedPlan(shape, binding, tier, undefined);
}

function composePolicyFilteredCurrentSourceGraph(
  this: NodeState,
  policyRequest: { ok: QueryProgramRequest } | { err: any },
  base: GraphBuilder,
  outputFieldNames: string[],
): PolicyAuthorizationGraph {
  this.queryEngineReadMetrics.policyAuthorizedSourceJoins += 1;
  // Policy proof joins restore the protected source's exact descriptor.
  const descriptor = this.database.graphOutputDescriptor(base);
  const outputFields: ProjectField[] = outputFieldNames.map((name) => {
    const field = descriptor.fields().find((f: any) => f.name === name);
    if (!field) throw Err.InvalidStoredValue("policy source output carrier is missing");
    if (field.identity === undefined)
      throw Err.InvalidStoredValue("policy source output identity is missing");
    return {
      expression: ProjectExprField(FieldRefStoredName(leftField(name))),
      outputName: name,
      outputIdentity: field.identity,
    };
  });
  let request: QueryProgramRequest;
  if ("err" in policyRequest) {
    const err = policyRequest.err;
    if (err.kind === "QueryCapability" && String(err.message).includes("PolicyProofCycle"))
      throw err;
    if (err.kind === "QueryCapability")
      return emptyPolicyFilteredCurrentSourceGraph(base, outputFields);
    throw err;
  }
  request = policyRequest.ok;
  // Carry the enclosing prepared binding's descriptor alongside the source
  // before joining the proof so a later storage delta has every route field.
  const b = request.input.binding;
  const bindingRoutes: [GraphBuilder, Set<string>] | undefined =
    b.sourceShape === undefined
      ? undefined
      : [
          GraphBuilderStatic.bindingSource(
            b.sourceShape,
            RecordDescriptorNew([
              ...[...b.paramTypes].map(([n, ty]: any) => [n, ty]),
              ...[...b.claimParams].map(([n, claim]: any) => [n, claim.ty]),
            ]),
          ),
          new Set<string>([...b.paramTypes.keys(), ...b.claimParams.keys()].sort()),
        ];
  const authorized = this.policyAuthorizationGraphCacheGet(
    policyAuthorizationGraphCacheKey(request),
  );
  if (!authorized)
    throw Err.InvalidStoredValue("policy dependency was not prepared before source composition");
  // Authorization is existential per protected row and binding route: collapse
  // alternative proofs before the protected source reaches relational operators.
  const authorizationKeys = ["row_uuid", ...authorized.routeFields];
  const authorizedGraph = GraphBuilderStatic.argMaxBy(
    authorized.graph,
    authorizationKeys,
    authorizationKeys,
  );
  const joinKeys = ["row_uuid", ...authorized.routeFields];
  const empty = () => new SortedMap<SourceId, CurrentAccessPath>(cmpStr as any);
  if (!bindingRoutes) {
    const fields = [
      ...outputFields,
      ...[...authorized.routeFields].map((f: string) => ProjectFieldRenamed(rightField(f), f)),
    ];
    return {
      graph: GraphBuilderStatic.join(base, authorizedGraph, joinKeys, joinKeys).projectFields(
        fields,
      ),
      routeFields: new Set(),
      accessPaths: empty(),
    } as PolicyAuthorizationGraph;
  }
  const [bindingGraph, bindingRouteFields] = bindingRoutes;
  const withBinding = (graph: GraphBuilder, keys: string[], carried: string[]) =>
    GraphBuilderStatic.join(graph, bindingGraph, keys, keys).projectFields([
      ...outputFields.map((field) => ({
        expression: ProjectExprField(FieldRefStoredName(leftField(field.outputName))),
        outputName: field.outputName,
        outputIdentity: field.outputIdentity,
      })),
      ...carried.map((f) => ProjectFieldRenamed(leftField(f), f)),
      ...[...bindingRouteFields]
        .filter((f) => !carried.includes(f))
        .map((f) => ProjectFieldRenamed(rightField(f), f)),
    ]);
  if (authorized.routeFields.size === 0) {
    // The proof admits a row for every binding alike; pair it with each binding.
    const authorizedRows = GraphBuilderStatic.join(
      base,
      authorizedGraph,
      joinKeys,
      joinKeys,
    ).projectFields([...outputFields]);
    return {
      graph: withBinding(authorizedRows, [], []),
      routeFields: bindingRouteFields,
      accessPaths: empty(),
    } as PolicyAuthorizationGraph;
  }
  // Take the policy's own routes from its proof first, then attach the
  // enclosing binding's other slots only to bindings with those route values.
  const proofRoutes: string[] = [...authorized.routeFields];
  const authorizedRows = GraphBuilderStatic.join(
    base,
    authorizedGraph,
    ["row_uuid"],
    ["row_uuid"],
  ).projectFields([
    ...outputFields,
    ...proofRoutes.map((f) => ProjectFieldRenamed(rightField(f), f)),
  ]);
  const bindingKeys = proofRoutes.filter((f) => bindingRouteFields.has(f));
  const routeFields = new Set<string>([...bindingRouteFields, ...proofRoutes]);
  const subset = [...bindingRouteFields].every((f) => authorized.routeFields.has(f));
  const graph = subset ? authorizedRows : withBinding(authorizedRows, bindingKeys, proofRoutes);
  return { graph, routeFields, accessPaths: empty() } as PolicyAuthorizationGraph;
}

function tableReadPolicyAuthorizationRequest(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  tableName: string,
  identity: AuthorSubject,
  paramBindingMode: ParamBindingMode,
  tier: Tier,
  bindingSourceShape: string | undefined,
  bindingUserParams: Map<string, ColumnType>,
  bindingClaimParams: SortedMap<string, ProgramClaimParam>,
): QueryProgramRequest {
  return this.tableReadPolicyAuthorizationRequestWithRootVisibility(
    policySchemaVersion,
    tableName,
    identity,
    paramBindingMode,
    tier,
    bindingSourceShape,
    bindingUserParams,
    bindingClaimParams,
    false,
  );
}

function policyTable(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  tableName: string,
): [RuntimeSchema, TableSchema] {
  const policySchema = this.catalogue.schemaForVersion(policySchemaVersion);
  if (!policySchema) throw Err.InvalidStoredValue("policy schema version is unknown");
  const table = policySchema.tables.find((c: TableSchema) => c.name === tableName);
  if (!table) throw Err.TableNotFound(tableName);
  return [policySchema, table];
}

function tableReadPolicyAuthorizationRequestAt(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  tableName: string,
  identity: AuthorSubject,
  paramBindingMode: ParamBindingMode,
  position: GlobalTime,
  bindingSourceShapeIn: string | undefined,
  bindingUserParams: Map<string, ColumnType>,
  bindingClaimParams: SortedMap<string, ProgramClaimParam>,
): QueryProgramRequest {
  const [policySchema, table] = policyTable.call(this, policySchemaVersion, tableName);
  // System authority bypasses the table policy; use the unfiltered table query.
  const query =
    identity === AuthorSubjectStatic.SYSTEM
      ? JazzQueryFrom(table.name)
      : authorizationQueryFromReadPolicy(table);
  if (query.includes.length > 0) {
    throw Err.InvalidStoredValue(
      "historical policy source filters do not support include policies",
    );
  }
  const validated = query.validate(policySchema);
  const policyShape = bindQueryParamsWithMode(
    validated,
    validated.bind(new Map()),
    policySchema,
    paramBindingMode,
  );
  if (policyShape.params().size > 0) {
    throw Err.QueryCapability(
      "historical policy source filters with runtime parameters must lower through query-engine binding sources",
    );
  }
  const binding = policyShape.bind(new Map());
  const inputShape = this.normalizedRowSetShape(policyShape, binding);
  const claimParams = bindingClaimParams;
  for (const [k, v] of bindingClaimParamsForShape(inputShape, policyShape.params()))
    claimParams.set(k, v);
  collectReachableSeedClaimParams(policySchema, policyShape.query(), claimParams);
  const bindingSourceShape =
    bindingSourceShapeIn ??
    authorizationBindingSourceShape(policyShape, bindingUserParams, claimParams);
  if (bindingSourceShape !== undefined) retargetBindingValueSources(inputShape, bindingSourceShape);
  const policy = this.authorizationSubplanPolicy(
    identity,
    policyShape.query().table,
    PolicyDecisionRole.Read,
  );
  const input: RowSetProgramInput = {
    binding: this.programBindingForShapeAndPolicy(
      policyShape,
      binding,
      bindingSourceShape,
      bindingUserParams,
      claimParams,
      policy,
    ),
    shape: inputShape,
  };
  return {
    authorizationMode: QueryAuthorizationMode.TrustedServing,
    reads: historicalQueryReadSet(input.shape, policySchemaVersion, position),
    policy,
    input,
    output: currentQueryOutputRequest(
      CurrentQueryProgramOutput.AuthorizedRows,
      policyShape.query(),
    ),
  };
}

function tableReadPolicyAuthorizationRequestForIncludeDeleted(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  tableName: string,
  identity: AuthorSubject,
  tier: Tier,
  bindingSourceShape: string | undefined,
  bindingUserParams: Map<string, ColumnType>,
  bindingClaimParams: SortedMap<string, ProgramClaimParam>,
): QueryProgramRequest {
  return this.tableReadPolicyAuthorizationRequestWithRootVisibility(
    policySchemaVersion,
    tableName,
    identity,
    ParamBindingMode.InlineAllReachableSeeds,
    tier,
    bindingSourceShape,
    bindingUserParams,
    bindingClaimParams,
    true,
  );
}

function tableReadPolicyAuthorizationRequestWithRootVisibility(
  this: NodeState,
  policySchemaVersion: SchemaVersionId,
  tableName: string,
  identity: AuthorSubject,
  paramBindingMode: ParamBindingMode,
  tier: Tier,
  bindingSourceShapeIn: string | undefined,
  bindingUserParams: Map<string, ColumnType>,
  bindingClaimParams: SortedMap<string, ProgramClaimParam>,
  includeDeletedRoot: boolean,
): QueryProgramRequest {
  const cacheKey = JSON.stringify([
    policySchemaVersion,
    tableName,
    identity,
    paramBindingMode,
    tier,
    bindingSourceShapeIn,
    bindingUserParamsCacheKey(bindingUserParams),
    bindingClaimParamsCacheKey(bindingClaimParams),
    this.activeSessionClaimScopeKey(identity) ?? "",
    includeDeletedRoot,
  ]);
  const cached = this.query.readPolicyAuthorizationRequestCache.get(cacheKey);
  if (cached) return cached.clone();
  const [policySchema, table] = policyTable.call(this, policySchemaVersion, tableName);
  const policy = this.authorizationSubplanPolicy(identity, tableName, PolicyDecisionRole.Read);
  // System authority bypasses the table policy: its subplan describes all rows.
  const query =
    identity === AuthorSubjectStatic.SYSTEM
      ? JazzQueryFrom(table.name)
      : authorizationQueryFromReadPolicy(table);
  const policyBindingValues: Claims = new Map();
  if (
    paramBindingMode === ParamBindingMode.RetainAllParams &&
    policy.kind === "AuthorizationSubplan"
  ) {
    bindScopeClaimOperands(query, policy.claims, policyBindingValues);
  }
  if (query.includes.length > 0) {
    throw Err.InvalidStoredValue(
      "maintained subscription view policy slice does not support include policies",
    );
  }
  const declaredClaimParams = disambiguatePolicyClaimParamsWithOuterSlots(
    query,
    policySchema,
    policyBindingValues,
    bindingClaimParams,
  );
  const validated = query.validate(policySchema);
  coerceBindingValuesForShape(validated, policyBindingValues);
  const policyBinding = validated.bind(new Map(policyBindingValues));
  const policyShape = bindQueryParamsWithMode(
    validated,
    policyBinding,
    policySchema,
    paramBindingMode,
  );
  if ([...policyShape.params().keys()].some((name: string) => !policyBindingValues.has(name))) {
    throw Err.QueryCapability(
      "maintained policy source filters with runtime parameters must lower through query-engine binding sources",
    );
  }
  const binding = policyShape.bind(policyBindingValues);
  const inputShape = includeDeletedRoot
    ? this.normalizedIncludeDeletedRowSetShape(policyShape, binding)
    : this.normalizedRowSetShape(policyShape, binding);
  const claimParams = bindingClaimParams;
  for (const [k, v] of bindingClaimParamsForShape(inputShape, policyShape.params()))
    claimParams.set(k, v);
  for (const [k, v] of declaredClaimParams) claimParams.set(k, v);
  collectReachableSeedClaimParams(policySchema, policyShape.query(), claimParams);
  for (const [name, claim] of claimParams) {
    const ty = policyShape.params().get(name);
    if (ty !== undefined) claim.ty = ty;
  }
  const bindingSourceShape =
    bindingSourceShapeIn ??
    authorizationBindingSourceShape(policyShape, bindingUserParams, claimParams);
  if (bindingSourceShape !== undefined) retargetBindingValueSources(inputShape, bindingSourceShape);
  const input: RowSetProgramInput = {
    binding: this.programBindingForShapeAndPolicy(
      policyShape,
      binding,
      bindingSourceShape,
      bindingUserParams,
      claimParams,
      policy,
    ),
    shape: inputShape,
  };
  const request: QueryProgramRequest = {
    authorizationMode: QueryAuthorizationMode.TrustedServing,
    reads: currentQueryReadSet(
      input.shape,
      policySchemaVersion,
      policySchemaVersion,
      tier,
      undefined,
      undefined,
      false,
    ),
    policy,
    input,
    output: currentQueryOutputRequest(
      CurrentQueryProgramOutput.AuthorizedRows,
      policyShape.query(),
    ),
  };
  this.query.readPolicyAuthorizationRequestCache.set(cacheKey, request);
  return request;
}

function maintainedViewContentCurrentWithVersion(
  this: NodeState,
  table: TableSchema,
  tier: Tier,
): GraphBuilder {
  return this.maintainedViewContentCurrentWithVersionInSchema(
    table,
    tier,
    this.catalogue.localSchemaVersionId,
  );
}

function maintainedViewContentCurrentWithVersionInSchema(
  this: NodeState,
  table: TableSchema,
  tier: Tier,
  schemaVersion: SchemaVersionId,
): GraphBuilder {
  const contentFields: string[] = globalCurrentStorageFields(table, true, true);
  const imageFields = [...contentFields, "_deletion"];
  // Select the winning row image, then drop it when it is deleted.
  const layers = layerStack.tryRefLayers(tier, (r: any) =>
    layerStack.Layer(
      this.physicalCurrentSourceGraph(schemaVersion, table.name, r.physicalClass()).project(
        imageFields,
      ),
    ),
  );
  const current =
    tier === DurabilityTier.Global
      ? layerStack.compose(layers)
      : layerStack.compose(layers).project(imageFields);
  return current.filter(notDeletedPredicate()).project(contentFields);
}

/** Pick the policy-owning schema for one authorization-support operation. */
function authorizationScopePolicySchemaForAction(
  this: NodeState,
  table: string,
  _operation: AuthorizationScopeOperation,
): SchemaVersionId {
  const writeSchema = this.catalogue.activeSchema.schema;
  let has = true;
  try {
    this.tableInSchemaRef(table, writeSchema);
  } catch {
    has = false;
  }
  return has ? writeSchema : this.catalogue.localSchemaVersionId;
}

/** Compile the exact policy clauses needed for one non-mutating operation. */
function authorizationSupportScope(
  this: NodeState,
  writer: AuthorSubject,
  action: PermissionAdviceAction,
): AuthorizationSupportScope {
  return this.authorizationSupportScopeForSession(writer, this.sessionClaims.get(writer), action);
}

/** Compile authorization support from the claims admitted by this exact transport session. */
function authorizationSupportScopeForSession(
  this: NodeState,
  writer: AuthorSubject,
  claims: Claims | undefined,
  action: PermissionAdviceAction,
): AuthorizationSupportScope {
  const [operation, tableName] = authorizationScopeAction(action);
  // The action's table belongs to the policy-owning schema; resolve both the
  // policy and its support-query schema from that policy view.
  const policySchemaVersion = this.authorizationScopePolicySchemaForAction(tableName, operation);
  let policySchema: RuntimeSchema;
  if (policySchemaVersion === this.catalogue.activeSchema.schema)
    policySchema = this.catalogue.activeSchema.compiled;
  else {
    const payload = this.catalogue.catalogueSchemas.get(policySchemaVersion);
    if (!payload) throw Err.InvalidStoredValue("authorization policy schema is unknown");
    policySchema = payload.schema;
  }
  const policies = authorizationRowPolicyQueries(
    this.tableInSchema(tableName, policySchemaVersion),
    action,
  );
  const claimValues = permissionScopeClaimValues(writer, claims);
  // Authorization support is authority-current.
  const options = RegisterShapeOptions.default();
  const subscriptions = policies.map((policy) =>
    compilePermissionScopePolicy(policy.clone(), claims, claimValues, policySchema),
  );
  const policyBytes = serializeOr(
    [operation, policies],
    "authorization policy serialization failed",
  );
  const claimBytes = serializeOr(claimValues, "authorization claims serialization failed");
  const supportBytes = serializeOr(
    [
      operation,
      options,
      subscriptions.map(([shape, binding]) => [shape.shapeId(), binding.bindingId()]),
    ],
    "authorization scope serialization failed",
  );
  const actionBytes = serializeOr(action, "authorization operation serialization failed");
  return {
    key: {
      supportShapeDigest: blake3(supportBytes),
      subject: writer,
      claimsDigest: blake3(claimBytes),
      policyDigest: blake3(policyBytes),
    },
    operation: authorizationOperationKey(operation, tableName, action, actionBytes),
    options,
    subscriptions,
  };
}

export function permissionScopeClaimValues(
  writer: AuthorSubject,
  claims: Claims | undefined,
): Claims {
  const claimValues: Claims = new Map(claims ?? []);
  // `user` is Jazz's reserved, issuer-scoped logical identity; other Jazz
  // defaults remain fallbacks.
  for (const [name, value] of defaultPermissionScopeClaimValues(writer)) {
    if (name === "user" || name.startsWith("user.")) claimValues.set(name, value);
    else if (!claimValues.has(name)) claimValues.set(name, value);
  }
  return claimValues;
}

installNodeMethods({
  readPolicyQueryAllowsOpenTxRow,
  policyAuthorizationRowIdGraph,
  pointPolicyAuthorizationRowIdGraph,
  policyAuthorizationRowIdGraphInner,
  authorizationSubplanPolicy,
  inlinePolicySubplanInput,
  queryProgramPolicyContext,
  writePolicyQueryAllowsCurrentRow,
  writePolicyQueryAllowsCandidate,
  writePolicyQueryAllowsCandidateForSchema,
  writePolicyQueryAllowsCandidateWithProvenanceForSchema,
  writePolicyQueryAllowsCandidateOverTransaction,
  readPolicyQueryAllowsCandidateWithProvenanceForSchema,
  policyQueryAllowsCandidateWithProvenanceForSchema,
  writePolicyQueryProgramAllows,
  queryRows,
  composePolicyFilteredCurrentSourceGraph,
  tableReadPolicyAuthorizationRequest,
  tableReadPolicyAuthorizationRequestAt,
  tableReadPolicyAuthorizationRequestForIncludeDeleted,
  tableReadPolicyAuthorizationRequestWithRootVisibility,
  maintainedViewContentCurrentWithVersion,
  maintainedViewContentCurrentWithVersionInSchema,
  authorizationScopePolicySchemaForAction,
  authorizationSupportScope,
  authorizationSupportScopeForSession,
});
