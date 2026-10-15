// Local availability record v1 uses Groove's native typed-record codec.
import { equalBytes, toHex } from "../../../support/index.js";
import {
  type NodeState,
  type PolicyBindingKey,
  Value,
  nodeError,
  AuthorSubject,
  PolicyBindingKeyApi,
  MAX_KNOWN_STATE_EXACT_REFS,
  LOCAL_ROW_AVAILABILITY_STORE,
  AUTHORITY_POLICY_BINDINGS_STORE,
  localAvailabilityRecordDescriptor,
  persistPolicyBindingDirectory,
} from "../../../_extern/node-qe-a.js";
import {
  requireLocalAvailabilityContextCapacity,
  updateLocalUnavailableRows,
} from "./unavailable_inputs.js";

export { localAvailabilityRecordDescriptor };

export interface LocalAvailabilityWatermark {
  core: Uint8Array;
  coreEpoch: number;
  claimsRevision: number;
  policyEpoch: number;
  settledThrough: number;
  authorizationProgress: number;
}

export type LocalRowAvailability = "Readable" | "CurrentUnavailable";

export interface LocalAvailabilityRecord {
  status: LocalRowAvailability;
  watermark: LocalAvailabilityWatermark;
}

type Outcome = [table: Uint8Array, row: Uint8Array, status: LocalRowAvailability];

function recordValues(r: LocalAvailabilityRecord): any[] {
  const w = r.watermark;
  return [
    Value.U8(1),
    Value.Bool(r.status === "CurrentUnavailable"),
    Value.Uuid(w.core),
    Value.U64(w.coreEpoch),
    Value.U64(w.claimsRevision),
    Value.U64(w.policyEpoch),
    Value.U64(w.settledThrough),
    Value.U64(w.authorizationProgress),
  ];
}

function decodeRecord(record: any): LocalAvailabilityRecord {
  const values: any[] = record.toValues();
  if (!equalBytes(localAvailabilityRecordDescriptor().create(values), record.raw())) {
    throw nodeError("InvalidStoredValue", "noncanonical local availability record v1");
  }
  const kinds = ["U8", "Bool", "Uuid", "U64", "U64", "U64", "U64", "U64"];
  if (values.length !== 8 || values.some((v, i) => v.kind !== kinds[i]) || values[0].value !== 1) {
    throw nodeError("InvalidStoredValue", "invalid local availability record v1");
  }
  const [, unavailable, core, coreEpoch, claimsRevision, policyEpoch, cut, seq] = values.map(
    (v) => v.value,
  );
  if (seq === 0) {
    throw nodeError(
      "InvalidStoredValue",
      "local availability evaluation sequence must be positive",
    );
  }
  return {
    status: unavailable ? "CurrentUnavailable" : "Readable",
    watermark: {
      core,
      coreEpoch,
      claimsRevision,
      policyEpoch,
      settledThrough: cut,
      authorizationProgress: seq,
    },
  };
}

function watermarkEq(a: LocalAvailabilityWatermark, b: LocalAvailabilityWatermark): boolean {
  return (
    equalBytes(a.core, b.core) &&
    a.coreEpoch === b.coreEpoch &&
    a.claimsRevision === b.claimsRevision &&
    a.policyEpoch === b.policyEpoch &&
    a.settledThrough === b.settledThrough &&
    a.authorizationProgress === b.authorizationProgress
  );
}

function follows(self: LocalAvailabilityRecord, prior: LocalAvailabilityRecord): boolean {
  const next = self.watermark;
  const old = prior.watermark;
  if (next.policyEpoch < old.policyEpoch || next.settledThrough < old.settledThrough) return false;
  if (!equalBytes(next.core, old.core)) return true;
  if (next.coreEpoch !== old.coreEpoch) return true;
  if (
    next.claimsRevision < old.claimsRevision ||
    next.authorizationProgress < old.authorizationProgress
  ) {
    return false;
  }
  if (next.authorizationProgress === old.authorizationProgress) {
    if (self.status !== prior.status || !watermarkEq(next, old)) {
      throw nodeError("InvalidStoredValue", "conflicting local availability evaluation receipt");
    }
    return false;
  }
  return true;
}

export function activateLocalAvailabilityAuthority(
  st: NodeState,
  scope: PolicyBindingKey,
  core: Uint8Array,
  epoch: number,
): void {
  if (AuthorSubject.isSystem(scope.identity)) {
    throw nodeError("InvalidStoredValue", "SYSTEM does not own local availability exclusions");
  }
  requireLocalAvailabilityContextCapacity(st, scope);
  st.query.localAvailabilityAuthorities.set(scope, [core, epoch]);
}

export function applyVerifiedLocalRowAvailability(
  st: NodeState,
  scope: PolicyBindingKey,
  watermark: LocalAvailabilityWatermark,
  outcomes: Outcome[],
): Promise<boolean> {
  return applyLocalRowAvailabilityAfterPersist(st, scope, watermark, outcomes, Promise.resolve());
}

export async function applyLocalRowAvailabilityAfterPersist(
  st: NodeState,
  scope: PolicyBindingKey,
  watermark: LocalAvailabilityWatermark,
  outcomes: Outcome[],
  afterPersist: Promise<void>,
): Promise<boolean> {
  const authority = st.query.localAvailabilityAuthorities.get(scope);
  if (
    !authority ||
    !equalBytes(authority[0], watermark.core) ||
    authority[1] !== watermark.coreEpoch
  ) {
    return false;
  }
  if (outcomes.length > MAX_KNOWN_STATE_EXACT_REFS || watermark.authorizationProgress === 0) {
    throw nodeError("InvalidStoredValue", "invalid local availability receipt bounds");
  }
  const seen = new Set<string>();
  const updates: [[PolicyBindingKey, Uint8Array, Uint8Array], LocalAvailabilityRecord][] = [];
  for (const [table, row, status] of outcomes) {
    const coord = toHex(table) + toHex(row);
    if (seen.has(coord))
      throw nodeError("InvalidStoredValue", "duplicate local availability row coordinate");
    seen.add(coord);
    let known = false;
    for (const mapping of st.catalogue.physicalMappings.values()) {
      for (const identity of mapping.identities.tables.values())
        if (equalBytes(identity.id, table)) known = true;
    }
    if (!known) {
      throw nodeError(
        "InvalidStoredValue",
        "local availability receipt has unknown global table identity",
      );
    }
    const key: [PolicyBindingKey, Uint8Array, Uint8Array] = [scope, table, row];
    const record: LocalAvailabilityRecord = { status, watermark };
    const prior = st.query.localAvailabilityRecords.get(key);
    if (prior && !follows(record, prior)) continue;
    updates.push([key, record]);
  }
  if (!updates.length) return false;
  await persistPolicyBindingDirectory(st, scope);
  const digest = PolicyBindingKeyApi.directoryDigest(scope);
  const writes = updates.map(([[, table, row], record]) => ({
    kind: "Set",
    key: [Value.Bytes(digest), Value.Uuid(table), Value.Uuid(row)],
    value: recordValues(record),
  }));
  const application = st.database.guardHostApplication();
  await st.database.directRecordStore(LOCAL_ROW_AVAILABILITY_STORE).writeMany(writes);
  await afterPersist;
  const changes: [Uint8Array, Uint8Array, boolean][] = updates.map(([[, table, row], record]) => [
    table,
    row,
    record.status === "CurrentUnavailable",
  ]);
  const changed = await updateLocalUnavailableRows(st, scope, changes);
  for (const [key, record] of updates) st.query.localAvailabilityRecords.set(key, record);
  application.complete();
  return changed;
}

export async function recoverLocalAvailabilityRecords(st: NodeState): Promise<void> {
  const store = st.database.directRecordStore(LOCAL_ROW_AVAILABILITY_STORE);
  const entries: any[] = await store.prefixEntries([]);
  const policyStore = st.database.directRecordStore(AUTHORITY_POLICY_BINDINGS_STORE);
  const policies = new Map<string, PolicyBindingKey>();
  const recovered: [[PolicyBindingKey, Uint8Array, Uint8Array], LocalAvailabilityRecord][] = [];
  for (const entry of entries) {
    const k = entry.key;
    if (k.length !== 3 || k[0].kind !== "Bytes" || k[1].kind !== "Uuid" || k[2].kind !== "Uuid") {
      throw nodeError("InvalidStoredValue", "invalid local availability record key");
    }
    const digest: Uint8Array = k[0].value;
    if (digest.length !== 32)
      throw nodeError("InvalidStoredValue", "invalid local availability policy digest");
    const dk = toHex(digest);
    if (!policies.has(dk)) {
      const policy = await policyStore.get([Value.Bytes(digest)]);
      if (!policy)
        throw nodeError(
          "InvalidStoredValue",
          "local availability policy directory entry is missing",
        );
      const subject = policy.getIdx(0);
      if (subject.kind !== "String") {
        throw nodeError("InvalidStoredValue", "invalid local availability policy subject");
      }
      let identity;
      try {
        identity = AuthorSubject.fromCanonical(subject.value);
      } catch {
        throw nodeError("InvalidStoredValue", "invalid local availability subject");
      }
      let key: PolicyBindingKey;
      try {
        key = PolicyBindingKeyApi.fromDirectoryValue(identity, policy.getIdx(1));
      } catch {
        throw nodeError("InvalidStoredValue", "invalid local availability policy claims");
      }
      if (
        AuthorSubject.isSystem(key.identity) ||
        !equalBytes(PolicyBindingKeyApi.directoryDigest(key), digest)
      ) {
        throw nodeError(
          "InvalidStoredValue",
          "local availability policy directory identity mismatch",
        );
      }
      policies.set(dk, key);
    }
    recovered.push([[policies.get(dk)!, k[1].value, k[2].value], decodeRecord(entry.value)]);
  }
  for (const [[scope, table, row], record] of recovered) {
    await updateLocalUnavailableRows(st, scope, [
      [table, row, record.status === "CurrentUnavailable"],
    ]);
  }
  st.query.localAvailabilityRecords.clear();
  for (const [key, record] of recovered) st.query.localAvailabilityRecords.set(key, record);
}
