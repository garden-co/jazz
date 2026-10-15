import { SortedMap, equalBytes } from "../../../support/index.js";
import type {
  NodeState,
  ValidatedQuery,
  Binding,
  AuthorityResultKey,
  AuthorSubject,
  Value,
  GlobalTime,
} from "../../../_extern/node-qe-b.js";
import {
  Err,
  installNodeMethods,
  rowLocalMembership,
  queryUsesClaims,
  SUBSCRIPTION_WATERMARKS_STORE,
  BranchKey,
  DurabilityTier,
  coveredInputCoordinateFrom,
  cmpCoveredInputCoordinate,
  newAuthorityResultState,
} from "../../../_extern/node-qe-b.js";

function watermarkCatchUpView(this: NodeState, shape: ValidatedQuery): boolean {
  const query = shape.query();
  if (!rowLocalMembership(query) || queryUsesClaims(query)) return false;
  let table;
  try {
    table = this.table(query.table);
  } catch {
    return false;
  }
  const policy = table.readPolicy;
  return policy === undefined || (rowLocalMembership(policy) && !queryUsesClaims(policy));
}

async function subscriptionWatermarkKey(
  this: NodeState,
  key: AuthorityResultKey,
): Promise<Value[]> {
  let scope = 0;
  let digest = new Uint8Array(0);
  if (key.policyBinding !== undefined) {
    await this.persistPolicyBindingDirectory(key.policyBinding);
    scope = 1;
    digest = key.policyBinding.directoryDigest();
  }
  return [
    { kind: "Uuid", value: key.bindingView.shapeId },
    { kind: "Uuid", value: key.bindingView.bindingId },
    { kind: "Uuid", value: key.bindingView.readView.id },
    { kind: "U8", value: scope },
    { kind: "Bytes", value: digest },
  ];
}

async function purgeSubscriptionWatermarks(this: NodeState): Promise<void> {
  const store = this.database.directRecordStore(SUBSCRIPTION_WATERMARKS_STORE);
  const deletes = (await store.prefixEntries([])).map((entry: any) => ({
    kind: "Delete",
    key: entry.key,
  }));
  if (deletes.length) await store.writeMany(deletes);
  this.query.persistedWatermarks.clear();
}

async function persistSubscriptionWatermark(
  this: NodeState,
  key: AuthorityResultKey,
  settledThrough: GlobalTime,
  revision: Uint8Array,
): Promise<void> {
  this.query.watermarkRestoreSeen.add(key);
  const prev = this.query.persistedWatermarks.get(key);
  if (prev && prev[0] === settledThrough && equalBytes(prev[1], revision)) return;
  const storeKey = await this.subscriptionWatermarkKey(key);
  await this.database
    .directRecordStore(SUBSCRIPTION_WATERMARKS_STORE)
    .set(storeKey, encodeWatermarkV1(settledThrough, revision));
  this.query.persistedWatermarks.set(key, [settledThrough, revision]);
}

async function restoreSubscriptionWatermark(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  key: AuthorityResultKey,
  identity: AuthorSubject,
): Promise<boolean> {
  if (this.query.watermarksInvalidated || this.query.watermarkRestoreSeen.has(key)) return false;
  this.query.watermarkRestoreSeen.add(key);
  if (this.query.authorityResults.has(key) || !this.watermarkCatchUpView(shape)) return false;
  const storeKey = await this.subscriptionWatermarkKey(key);
  const store = this.database.directRecordStore(SUBSCRIPTION_WATERMARKS_STORE);
  const record = await store.get(storeKey);
  if (record === undefined) return false;
  const [settledThrough, revision] = decodeWatermarkV1(record.toValues());
  const table = shape.query().table;
  const physicalTable = this.scopePhysicalTable(shape.schemaVersion(), table);
  const branch = BranchKey.default().canonicalBytes();
  const held = new SortedMap<any, any>(cmpCoveredInputCoordinate);
  for (const row of await this.queryRowsForLink(shape, binding, DurabilityTier.Global, identity)) {
    const tx = await this.currentRowTxId(row);
    if (tx === undefined) continue;
    const input = {
      physicalTable,
      versionTable: table,
      row: row.rowUuid(),
      version: {
        tx,
        schemaVersion: undefined,
        layer: "Content",
        batch: tx,
        branchOrPrefix: branch.length ? branch : undefined,
        rowDigest: undefined,
      },
    };
    held.set(coveredInputCoordinateFrom(input), input);
  }
  const state = this.query.authorityResults.getOrInsert(key, newAuthorityResultState);
  if (state.settledThrough !== undefined || state.supportingRevision !== undefined) return false;
  state.appliedViewUpdateGeneration = Math.max(state.appliedViewUpdateGeneration, 1);
  state.sourceClosure = { kind: "Claimed", generation: state.appliedViewUpdateGeneration };
  state.coveredInputVersions = held;
  state.settledThrough = settledThrough;
  state.supportingRevision = revision;
  return true;
}

function encodeWatermarkV1(settledThrough: GlobalTime, revision: Uint8Array): Value[] {
  return [
    { kind: "U8", value: 1 },
    { kind: "U64", value: settledThrough },
    { kind: "Bytes", value: revision },
  ];
}

function decodeWatermarkV1(values: Value[]): [GlobalTime, Uint8Array] {
  const [format, seq, rev] = values;
  if (
    values.length !== 3 ||
    format?.kind !== "U8" ||
    format.value !== 1 ||
    seq?.kind !== "U64" ||
    rev?.kind !== "Bytes"
  )
    throw Err.InvalidStoredValue("invalid subscription watermark record v1");
  const revision: Uint8Array = rev.value;
  if (revision.length !== 16)
    throw Err.InvalidStoredValue("subscription watermark revision must be 16 bytes");
  if (seq.value === 0 || revision.every((b) => b === 0))
    throw Err.InvalidStoredValue("subscription watermark record v1 must name a settled revision");
  return [seq.value, revision];
}

installNodeMethods({
  watermarkCatchUpView,
  subscriptionWatermarkKey,
  purgeSubscriptionWatermarks,
  persistSubscriptionWatermark,
  restoreSubscriptionWatermark,
});
