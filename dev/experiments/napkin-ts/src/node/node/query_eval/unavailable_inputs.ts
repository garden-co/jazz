// Context-scoped mutable inputs for locally unavailable current rows.
import { equalBytes, toHex } from "../../../support/index.js";
import {
  type NodeState,
  type PolicyBindingKey,
  Value,
  nodeError,
  AuthorSubject,
  CanonicalPolicyClaims,
  RecordDescriptor,
  GraphBuilder,
  PredicateExpr,
  ProjectField,
  FateTag,
  DurabilityTier,
  leftField,
  MAX_AUTHORIZATION_SCOPES,
  policyBindingKeyEq,
} from "../../../_extern/node-qe-a.js";

export class LocalUnavailableInput {
  id: any = undefined;
  runtimeToken = 0;
  rows = new Set<string>();
}

const descriptor = () => RecordDescriptor.new([["row_uuid", "Uuid"]]);

export function localUnavailablePolicyBinding(request: any): PolicyBindingKey | undefined {
  if (request.authorizationMode === "TrustedServing") return undefined;
  return policyBinding(request.policy);
}

function policyBinding(policy: any): PolicyBindingKey | undefined {
  switch (policy.kind) {
    case "Identity":
      return {
        identity: policy.permissionSubject,
        canonicalClaims: CanonicalPolicyClaims.new(policy.claims),
      };
    default:
      return undefined;
  }
}

export function isCurrentAppSource(source: any): boolean {
  switch (source.kind) {
    case "VisibleCurrent":
      return source.data.kind === "Current";
    case "SettledBindingView":
      return source.currentDefault;
    case "WithOverlays":
      return (
        source.overlays.entries.length === 1 &&
        source.overlays.entries[0].kind === "PendingLocal" &&
        isCurrentAppSource(source.input)
      );
    default:
      return false;
  }
}

export function localReadPolicyBinding(st: NodeState, identity: any): PolicyBindingKey | undefined {
  return policyBinding(st.queryProgramPolicyContext(identity));
}

export function isLocalRowUnavailable(
  st: NodeState,
  scope: PolicyBindingKey,
  table: Uint8Array,
  row: Uint8Array,
) {
  return !!st.query.localUnavailableInputs.get([scope, table])?.rows.has(toHex(row));
}

export function localAvailabilityTableId(st: NodeState, schema: any, table: string): Uint8Array {
  const id = st.catalogue.physicalMappings.get(schema)?.identities.tables.get(table)?.id;
  if (id === undefined) {
    throw nodeError("InvalidStoredValue", "local availability source has no global table identity");
  }
  return id;
}

export async function setLocalRowUnavailable(
  st: NodeState,
  scope: PolicyBindingKey,
  table: string,
  row: Uint8Array,
  unavailable: boolean,
): Promise<boolean> {
  if (AuthorSubject.isSystem(scope.identity)) return false;
  const id = localAvailabilityTableId(st, st.catalogue.localSchemaVersionId, table);
  return updateLocalUnavailableRows(st, scope, [[id, row, unavailable]]);
}

export async function updateLocalUnavailableRows(
  st: NodeState,
  scope: PolicyBindingKey,
  rows: [Uint8Array, Uint8Array, boolean][],
): Promise<boolean> {
  const changes = new Map<string, [Uint8Array, Uint8Array[], Uint8Array[]]>();
  let changed = false;
  for (const [table, row, unavailable] of rows) {
    const input: LocalUnavailableInput | undefined = st.query.localUnavailableInputs.get([
      scope,
      table,
    ]);
    const prior = !!input?.rows.has(toHex(row));
    if (prior === unavailable) continue;
    changed = true;
    if (input && input.id !== undefined && input.runtimeToken === st.grooveRuntimeToken) {
      const record = descriptor().create([Value.Uuid(row)]);
      const tk = toHex(table);
      let entry = changes.get(tk);
      if (!entry) changes.set(tk, (entry = [table, [], []]));
      (unavailable ? entry[1] : entry[2]).push(record);
    }
  }
  const deltas = [...changes.keys()].sort().map((tk) => {
    const [table, adds, removes] = changes.get(tk)!;
    const id = st.query.localUnavailableInputs.get([scope, table]).id;
    if (id === undefined) throw new Error("live input");
    return { id, descriptor: descriptor(), adds, removes };
  });
  if (deltas.length) {
    try {
      await st.database.applyInputSourceDeltas(deltas);
    } catch (e) {
      throw nodeError("Groove", String(e));
    }
  }
  for (const [table, row, unavailable] of rows) {
    const input: LocalUnavailableInput = st.query.localUnavailableInputs.getOrInsert(
      [scope, table],
      () => new LocalUnavailableInput(),
    );
    if (unavailable) input.rows.add(toHex(row));
    else input.rows.delete(toHex(row));
  }
  return changed;
}

export function requireLocalAvailabilityContextCapacity(
  st: NodeState,
  scope: PolicyBindingKey,
): void {
  const live = new Map<string, PolicyBindingKey>();
  for (const [[s], input] of st.query.localUnavailableInputs) {
    if (input.id !== undefined && input.runtimeToken === st.grooveRuntimeToken)
      live.set(scopeKey(s), s);
  }
  for (const [s] of st.query.localAvailabilityAuthorities) live.set(scopeKey(s), s);
  if (!live.has(scopeKey(scope)) && live.size >= MAX_AUTHORIZATION_SCOPES) {
    throw nodeError(
      "QueryCapability",
      "local availability context capacity reached; retire inactive scope graphs before retrying",
    );
  }
}

const scopeKey = (s: PolicyBindingKey): string =>
  toHex(s.directoryDigest?.() ?? new Uint8Array()) + String(s.identity);

async function localUnavailableInput(
  st: NodeState,
  scope: PolicyBindingKey,
  table: Uint8Array,
): Promise<any> {
  const key = [scope, table];
  const existing: LocalUnavailableInput | undefined = st.query.localUnavailableInputs.get(key);
  if (existing && existing.runtimeToken === st.grooveRuntimeToken && existing.id !== undefined)
    return existing.id;
  requireLocalAvailabilityContextCapacity(st, scope);
  const records = [...(existing?.rows ?? [])].map((hex) =>
    descriptor().create([Value.Uuid(hexToBytes(hex))]),
  );
  const id = st.database.allocateInputSource(descriptor());
  if (records.length) {
    try {
      await st.database.replaceInputSources([{ id, descriptor: descriptor(), records }]);
    } catch (e) {
      throw nodeError("Groove", String(e));
    }
  }
  const input: LocalUnavailableInput = st.query.localUnavailableInputs.getOrInsert(
    key,
    () => new LocalUnavailableInput(),
  );
  input.id = id;
  input.runtimeToken = st.grooveRuntimeToken;
  return id;
}

function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

export async function retireLocalAvailabilityScopeInputs(
  st: NodeState,
  scope: PolicyBindingKey,
): Promise<void> {
  const ids: any[] = [];
  for (const [[key], input] of st.query.localUnavailableInputs) {
    if (
      policyBindingKeyEq(key, scope) &&
      input.runtimeToken === st.grooveRuntimeToken &&
      input.id !== undefined
    ) {
      ids.push(input.id);
    }
  }
  if (ids.length) {
    try {
      await st.database.retireInputSources(ids);
    } catch (e) {
      throw nodeError("Groove", String(e));
    }
  }
  for (const [[key], input] of st.query.localUnavailableInputs)
    if (policyBindingKeyEq(key, scope)) input.id = undefined;
  st.query.queryShapeCache.clear();
  st.query.compiledQueryProgramCache.clear();
  st.query.queryProgramTemplates.clear();
  st.query.supportedQueryProgramRequests.clear();
  st.query.localAvailabilityAuthorities.delete(scope);
}

export async function excludeLocalUnavailableGraph(
  st: NodeState,
  scope: PolicyBindingKey,
  schema: any,
  source: any,
  graph: any,
  pendingAhead: boolean,
): Promise<any> {
  const table = localAvailabilityTableId(st, schema, source.source.table);
  const id = await localUnavailableInput(st, scope, table);
  const unavailable = GraphBuilder.inputSource(id, descriptor());
  if (pendingAhead) {
    const fields = ["row_uuid", "tx_time", "tx_node_id"];
    const renamed = () => fields.map((f) => ProjectField.renamed(leftField(f), f));
    const settled = GraphBuilder.join(
      graph,
      GraphBuilder.table("jazz_transactions")
        .filter(
          PredicateExpr.And([
            PredicateExpr.eq("fate", Value.EnumTag(FateTag.Accepted)),
            PredicateExpr.eq("durability", Value.EnumTag(DurabilityTier.discriminant("Global"))),
          ]).canonicalize(),
        )
        .project(["time", "node_id"]),
      ["tx_time", "tx_node_id"],
      ["time", "node_id"],
    ).projectFields(renamed());
    const blocked = GraphBuilder.join(
      settled,
      unavailable,
      ["row_uuid"],
      ["row_uuid"],
    ).projectFields(renamed());
    return GraphBuilder.antiJoin(graph, blocked, fields, fields);
  }
  return GraphBuilder.antiJoin(graph, unavailable, ["row_uuid"], ["row_uuid"]);
}
