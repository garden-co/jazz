// Simulation-first wire vocabulary for sync messages. Split into
// protocol/{versions,keys,entries,catalogue}.ts; this file owns SyncMessage.
import type { Reader, Writer } from "../support/postcard.js";
import { SortedMap, SortedSet, cmpStr } from "../support/collections.js";
import { uuidV4 } from "../support/hash.js";
import {
  AuthorSubject,
  readUuid,
  writeUuid,
  type GlobalPhysicalTableId,
  type NodeUuid,
  type RowUuid,
  type SchemaVersionId,
} from "../types/ids.js";
import type { GlobalTime } from "../types/time.js";
import { MAX_CHUNK_REQUEST_BATCH_ENTRIES } from "./protocol_limits.js";
import {
  FEATURE_AUTHORIZATION_SCOPE_VIEWS,
  FEATURE_AUXILIARY_CHUNKS,
  FEATURE_NONE,
  type WireFeatures,
} from "./wire.js";
import {
  carrierBundleRefs,
  readPeerPayloadInventory,
  readVersionCarrier,
  readVersionRecord,
  validateVersionCarrierRuns,
  validateVersionRecords,
  VersionBundleRunError,
  writePeerPayloadInventory,
  writeVersionCarrier,
  writeVersionRecord,
  type PeerPayloadInventory,
  type VersionCarrier,
  type VersionRecord,
} from "./protocol/versions.js";
import {
  PolicyBindingKey,
  readDelegatedSessionBinding,
  readPolicyBindingKey,
  readRegisterShapeOptions,
  readShapeAst,
  readSubscribe,
  readSubscribeRejectReason,
  readSubscriptionKey,
  writeDelegatedSessionBinding,
  writePolicyBindingKey,
  writeRegisterShapeOptions,
  writeShapeAst,
  writeSubscribe,
  writeSubscribeRejectReason,
  writeSubscriptionKey,
  type DelegatedSessionBinding,
  type RegisterShapeOptions,
  type ShapeAst,
  type Subscribe,
  type SubscribeRejectReason,
  type SubscriptionKey,
} from "./protocol/keys.js";
import {
  entryKey,
  readSupportingRow,
  supportingRowIsWireValid,
  writeSupportingRow,
  type SupportingRow,
} from "./protocol/entries.js";
import {
  MigrationLens,
  SchemaLineagePublication,
  readCatalogueAck,
  readCatalogueSnapshot,
  readSchemaVersion,
  writeCatalogueAck,
  writeCatalogueSnapshot,
  writeSchemaVersion,
  type CatalogueAck,
  type CatalogueSnapshot,
  type SchemaVersion,
} from "./protocol/catalogue.js";
import {
  readDurabilityTier,
  readFate,
  readLargeValueRef,
  readLocator,
  readNodeRef,
  readShapeId,
  readStagedChunk,
  readTransaction,
  readTxId,
  readValue,
  writeDurabilityTier,
  writeFate,
  writeLargeValueRef,
  writeLocator,
  writeNodeRef,
  writeShapeId,
  writeStagedChunk,
  writeTransaction,
  writeTxId,
  writeValue,
  type DurabilityTier,
  type Fate,
  type ShapeId,
  type Transaction,
  type TxId,
} from "../_extern/types-protocol.js";

export * from "./protocol/versions.js";
export * from "./protocol/keys.js";
export * from "./protocol/entries.js";
export * from "./protocol/catalogue.js";

type Claims = SortedMap<string, any>;
const writeClaims = (w: Writer, c: Claims) =>
  w.seq([...c], (w, [k, v]) => {
    w.str(k);
    writeValue(w, v);
  });
const readClaims = (r: Reader): Claims =>
  new SortedMap(
    cmpStr,
    r.seq((r) => [r.str(), readValue(r)] as [string, any]),
  );
const writeFixed = (w: Writer, b: Uint8Array) => w.raw(b);
const readFixed = (n: number) => (r: Reader) => r.raw(n).slice();

// ---- current rows ----

export const MAX_CURRENT_ROWS = 64;
export interface CurrentRowCoordinate {
  schema: SchemaVersionId;
  table: string;
  physicalTable: GlobalPhysicalTableId;
  row: RowUuid;
}
const writeCoord = (w: Writer, c: CurrentRowCoordinate) => {
  writeUuid(w, c.schema);
  w.str(c.table);
  writeUuid(w, c.physicalTable);
  writeUuid(w, c.row);
};
const readCoord = (r: Reader): CurrentRowCoordinate => ({
  schema: readUuid(r),
  table: r.str(),
  physicalTable: readUuid(r),
  row: readUuid(r),
});

export type PermissionAdviceRequestId = Uint8Array; // 16 bytes
export interface CurrentRowsRequest {
  requestId: PermissionAdviceRequestId;
  rows: CurrentRowCoordinate[];
  delegatedSession?: DelegatedSessionBinding;
}
export type CurrentRowOutcome = "Readable" | "CurrentUnavailable" | "Unknown";
const OUTCOMES: CurrentRowOutcome[] = ["Readable", "CurrentUnavailable", "Unknown"];
export interface CurrentRowsReceipt {
  requestId: PermissionAdviceRequestId;
  rows: CurrentRowCoordinate[];
  outcomes: CurrentRowOutcome[];
  context: PolicyBindingKey;
  core: NodeUuid;
  coreEpoch: number;
  claimsRevision: number;
  policyEpoch: number;
  settledThrough: GlobalTime;
  authorizationProgress: number;
  versionCarriers: VersionCarrier[];
}

// ---- view updates ----

export type SupportingRowsUpdate =
  | { kind: "Snapshot"; revision: Uint8Array; rows: SupportingRow[] }
  | {
      kind: "Delta";
      predecessor: Uint8Array;
      revision: Uint8Array;
      adds: SupportingRow[];
      removes: SupportingRow[];
    }
  | {
      kind: "CatchUp";
      predecessor: Uint8Array;
      revision: Uint8Array;
      changed: SupportingRow[];
      left: SupportingRow[];
    };

export const supportingRowsSnapshot = (rows: SupportingRow[]): SupportingRowsUpdate => ({
  kind: "Snapshot",
  revision: uuidV4(),
  rows,
});
export function supportingAddedRows(u: SupportingRowsUpdate): SupportingRow[] {
  return u.kind === "Snapshot" ? u.rows : u.kind === "Delta" ? u.adds : u.changed;
}
export function supportingRemovedRows(u: SupportingRowsUpdate): SupportingRow[] {
  return u.kind === "Snapshot" ? [] : u.kind === "Delta" ? u.removes : u.left;
}
export const supportingIsSnapshot = (u: SupportingRowsUpdate) => u.kind !== "Delta";

function writeSupportingRowsUpdate(w: Writer, u: SupportingRowsUpdate): void {
  switch (u.kind) {
    case "Snapshot":
      w.variant(0).raw(u.revision).seq(u.rows, writeSupportingRow);
      break;
    case "Delta":
      w.variant(1)
        .raw(u.predecessor)
        .raw(u.revision)
        .seq(u.adds, writeSupportingRow)
        .seq(u.removes, writeSupportingRow);
      break;
    case "CatchUp":
      w.variant(2)
        .raw(u.predecessor)
        .raw(u.revision)
        .seq(u.changed, writeSupportingRow)
        .seq(u.left, writeSupportingRow);
      break;
  }
}
function readSupportingRowsUpdate(r: Reader): SupportingRowsUpdate {
  const rev = readFixed(16);
  switch (r.variant(3, "SupportingRowsUpdate")) {
    case 0:
      return { kind: "Snapshot", revision: rev(r), rows: r.seq(readSupportingRow) };
    case 1:
      return {
        kind: "Delta",
        predecessor: rev(r),
        revision: rev(r),
        adds: r.seq(readSupportingRow),
        removes: r.seq(readSupportingRow),
      };
    default:
      return {
        kind: "CatchUp",
        predecessor: rev(r),
        revision: rev(r),
        changed: r.seq(readSupportingRow),
        left: r.seq(readSupportingRow),
      };
  }
}

export interface ViewUpdatePayload {
  subscription: SubscriptionKey;
  settledThrough: GlobalTime;
  versionCarriers: VersionCarrier[];
  peerPayloadInventory: PeerPayloadInventory;
  supportingRows: SupportingRowsUpdate;
}
const writeViewUpdate = (w: Writer, v: ViewUpdatePayload) => {
  writeSubscriptionKey(w, v.subscription);
  w.varint64(v.settledThrough);
  w.seq(v.versionCarriers, writeVersionCarrier);
  writePeerPayloadInventory(w, v.peerPayloadInventory);
  writeSupportingRowsUpdate(w, v.supportingRows);
};
const readViewUpdate = (r: Reader): ViewUpdatePayload => ({
  subscription: readSubscriptionKey(r),
  settledThrough: r.varint64(),
  versionCarriers: r.seq(readVersionCarrier),
  peerPayloadInventory: readPeerPayloadInventory(r),
  supportingRows: readSupportingRowsUpdate(r),
});

// ---- immutable chunks ----

export interface ChunkRequestEntry {
  requestId: number;
  locator: any;
  expectedHash: Uint8Array;
  remainingHops: number;
}
export type ChunkResponse =
  | { kind: "Found"; bytes: Uint8Array }
  | { kind: "Unavailable" }
  | { kind: "Retryable"; retryAfterMs: number };
export interface ChunkResponseEntry {
  requestId: number;
  result: ChunkResponse;
}
export type ChunkUploadStatus =
  | { kind: "Need"; nodes: any[] }
  | { kind: "Staged" }
  | { kind: "RateLimited" }
  | { kind: "Rejected" };

function readChunkRequests(r: Reader): ChunkRequestEntry[] {
  const declared = r.varint();
  if (declared > MAX_CHUNK_REQUEST_BATCH_ENTRIES)
    throw new Error("chunk request batch exceeds cardinality limit");
  const out: ChunkRequestEntry[] = [];
  for (let i = 0; i < declared; i++)
    out.push({
      requestId: r.varint(),
      locator: readLocator(r),
      expectedHash: readFixed(32)(r),
      remainingHops: r.u8(),
    });
  return out;
}

// ---- permission advice / authorization scopes ----

export type PermissionAdviceAction =
  | { kind: "Insert"; table: string; cells: Claims }
  | { kind: "Read"; table: string; row: RowUuid }
  | { kind: "Update"; table: string; row: RowUuid; patch: Claims }
  | { kind: "Delete"; table: string; row: RowUuid };
export type AuthorizationScopeOperation = "Read" | "Insert" | "Update" | "Delete";
export type PermissionAdvice = "Allowed" | "Denied" | "Unknown";
const ADVICE: PermissionAdvice[] = ["Allowed", "Denied", "Unknown"];

export interface AuthorizationSupportScopeKey {
  supportShapeDigest: Uint8Array;
  subject: AuthorSubject;
  claimsDigest: Uint8Array;
  policyDigest: Uint8Array;
}
export const authorizationScopeKeyString = (k: AuthorizationSupportScopeKey) =>
  entryKey([k.supportShapeDigest, k.subject.canonical(), k.claimsDigest, k.policyDigest]);
export interface AuthorizationOperationKey {
  operation: AuthorizationScopeOperation;
  table: string;
  row?: RowUuid;
  candidateDigest: Uint8Array;
}
export interface AuthorizationScopeReceipt {
  key: AuthorizationSupportScopeKey;
  authority: Uint8Array;
  link: AuthorSubject;
  authorityEpoch: number;
  claimsRevision: number;
  policyEpoch: number;
  settledThrough: GlobalTime;
  authorizationProgress: number;
}

function writeScopeKey(w: Writer, k: AuthorizationSupportScopeKey) {
  w.raw(k.supportShapeDigest);
  AuthorSubject.write(w, k.subject);
  w.raw(k.claimsDigest).raw(k.policyDigest);
}
const readScopeKey = (r: Reader): AuthorizationSupportScopeKey => ({
  supportShapeDigest: readFixed(32)(r),
  subject: AuthorSubject.read(r),
  claimsDigest: readFixed(32)(r),
  policyDigest: readFixed(32)(r),
});

function writeAction(w: Writer, a: PermissionAdviceAction) {
  switch (a.kind) {
    case "Insert":
      writeClaims(w.variant(0).str(a.table), a.cells);
      break;
    case "Read":
      writeUuid(w.variant(1).str(a.table), a.row);
      break;
    case "Update":
      writeUuid(w.variant(2).str(a.table), a.row);
      writeClaims(w, a.patch);
      break;
    case "Delete":
      writeUuid(w.variant(3).str(a.table), a.row);
      break;
  }
}
function readAction(r: Reader): PermissionAdviceAction {
  switch (r.variant(4, "PermissionAdviceAction")) {
    case 0:
      return { kind: "Insert", table: r.str(), cells: readClaims(r) };
    case 1:
      return { kind: "Read", table: r.str(), row: readUuid(r) };
    case 2:
      return { kind: "Update", table: r.str(), row: readUuid(r), patch: readClaims(r) };
    default:
      return { kind: "Delete", table: r.str(), row: readUuid(r) };
  }
}

// ---- SyncMessage ----

export type SyncMessage =
  | { kind: "ChunkRequestBatch"; requests: ChunkRequestEntry[] }
  | { kind: "ChunkResponseBatch"; responses: ChunkResponseEntry[] }
  | { kind: "SessionClaims"; identity: AuthorSubject; claims: Claims }
  | { kind: "CommitUnit"; tx: Transaction; versions: VersionRecord[] }
  | {
      kind: "FateUpdate";
      txId: TxId;
      fate: Fate;
      globalTime?: GlobalTime;
      durability?: DurabilityTier;
    }
  | { kind: "RegisterShape"; shapeId: ShapeId; ast: ShapeAst; opts: RegisterShapeOptions }
  | { kind: "Subscribe"; subscribe: Subscribe }
  | { kind: "SubscribeRejected"; subscription: SubscriptionKey; reason: SubscribeRejectReason }
  | { kind: "Unsubscribe"; subscription: SubscriptionKey }
  | { kind: "PublishSchema"; author: AuthorSubject; schema: SchemaVersion }
  | {
      kind: "PublishSchemaWithLens";
      author: AuthorSubject;
      catalogueSeq: number;
      publication: SchemaLineagePublication;
    }
  | { kind: "PublishLens"; author: AuthorSubject; lens: MigrationLens }
  | { kind: "CatalogueAck"; ack: CatalogueAck }
  | { kind: "ViewUpdate"; view: ViewUpdatePayload }
  | { kind: "CatalogueSnapshot"; snapshot: CatalogueSnapshot }
  | {
      kind: "AuthorizationScopeIntent";
      requestId: PermissionAdviceRequestId;
      action: PermissionAdviceAction;
      delegatedSession?: DelegatedSessionBinding;
    }
  | {
      kind: "AuthorizationScopeView";
      requestId: PermissionAdviceRequestId;
      key: AuthorizationSupportScopeKey;
      clauseIndex: number;
      clauseCount: number;
      view: ViewUpdatePayload;
    }
  | {
      kind: "AuthorizationScopeAggregateReceipt";
      requestId: PermissionAdviceRequestId;
      receipt: AuthorizationScopeReceipt;
    }
  | { kind: "AuthorizationScopeUnavailable"; requestId: PermissionAdviceRequestId }
  | {
      kind: "AuthorizationScopeDecision";
      requestId: PermissionAdviceRequestId;
      advice: PermissionAdvice;
    }
  | { kind: "ChunkUploadStart"; valueRef: any }
  | { kind: "ChunkUploadNodes"; valueRef: any; chunks: any[] }
  | { kind: "ChunkUploadResult"; valueRef: any; status: ChunkUploadStatus }
  | { kind: "CurrentRowsRequest"; request: CurrentRowsRequest }
  | { kind: "CurrentRowsReceipt"; receipt: CurrentRowsReceipt }
  | { kind: "CurrentRowsCancel"; requestId: PermissionAdviceRequestId }
  | { kind: "ViewUpdatePart"; view: ViewUpdatePayload }
  | { kind: "ReadBarrier"; writesThrough: number };

const VARIANTS = [
  "ChunkRequestBatch",
  "ChunkResponseBatch",
  "SessionClaims",
  "CommitUnit",
  "FateUpdate",
  "RegisterShape",
  "Subscribe",
  "SubscribeRejected",
  "Unsubscribe",
  "PublishSchema",
  "PublishSchemaWithLens",
  "PublishLens",
  "CatalogueAck",
  "ViewUpdate",
  "Reserved14",
  "Reserved15",
  "CatalogueSnapshot",
  "AuthorizationScopeIntent",
  "AuthorizationScopeView",
  "AuthorizationScopeAggregateReceipt",
  "AuthorizationScopeUnavailable",
  "AuthorizationScopeDecision",
  "ChunkUploadStart",
  "ChunkUploadNodes",
  "ChunkUploadResult",
  "CurrentRowsRequest",
  "CurrentRowsReceipt",
  "CurrentRowsCancel",
  "ViewUpdatePart",
  "Reserved29",
  "ReadBarrier",
] as const;

export function writeSyncMessage(w: Writer, m: SyncMessage): void {
  w.variant(VARIANTS.indexOf(m.kind));
  switch (m.kind) {
    case "ChunkRequestBatch":
      w.seq(m.requests, (w, e) => {
        w.varint(e.requestId);
        writeLocator(w, e.locator);
        w.raw(e.expectedHash).u8(e.remainingHops);
      });
      break;
    case "ChunkResponseBatch":
      w.seq(m.responses, (w, e) => {
        w.varint(e.requestId);
        const res = e.result;
        if (res.kind === "Found") w.variant(0).bytes(res.bytes);
        else if (res.kind === "Unavailable") w.variant(1);
        else w.variant(2).varint(res.retryAfterMs);
      });
      break;
    case "SessionClaims":
      AuthorSubject.write(w, m.identity);
      writeClaims(w, m.claims);
      break;
    case "CommitUnit":
      writeTransaction(w, m.tx);
      w.seq(m.versions, writeVersionRecord);
      break;
    case "FateUpdate":
      writeTxId(w, m.txId);
      writeFate(w, m.fate);
      w.option(m.globalTime, (w, t) => w.varint64(t));
      w.option(m.durability, writeDurabilityTier);
      break;
    case "RegisterShape":
      writeShapeId(w, m.shapeId);
      writeShapeAst(w, m.ast);
      writeRegisterShapeOptions(w, m.opts);
      break;
    case "Subscribe":
      writeSubscribe(w, m.subscribe);
      break;
    case "SubscribeRejected":
      writeSubscriptionKey(w, m.subscription);
      writeSubscribeRejectReason(w, m.reason);
      break;
    case "Unsubscribe":
      writeSubscriptionKey(w, m.subscription);
      break;
    case "PublishSchema":
      AuthorSubject.write(w, m.author);
      writeSchemaVersion(w, m.schema);
      break;
    case "PublishSchemaWithLens":
      AuthorSubject.write(w, m.author);
      w.varint(m.catalogueSeq);
      SchemaLineagePublication.write(w, m.publication);
      break;
    case "PublishLens":
      AuthorSubject.write(w, m.author);
      MigrationLens.write(w, m.lens);
      break;
    case "CatalogueAck":
      writeCatalogueAck(w, m.ack);
      break;
    case "ViewUpdate":
    case "ViewUpdatePart":
      writeViewUpdate(w, m.view);
      break;
    case "CatalogueSnapshot":
      writeCatalogueSnapshot(w, m.snapshot);
      break;
    case "AuthorizationScopeIntent":
      writeFixed(w, m.requestId);
      writeAction(w, m.action);
      w.option(m.delegatedSession, writeDelegatedSessionBinding);
      break;
    case "AuthorizationScopeView":
      writeFixed(w, m.requestId);
      writeScopeKey(w, m.key);
      w.varint(m.clauseIndex).varint(m.clauseCount);
      writeViewUpdate(w, m.view);
      break;
    case "AuthorizationScopeAggregateReceipt": {
      const rc = m.receipt;
      writeFixed(w, m.requestId);
      writeScopeKey(w, rc.key);
      w.raw(rc.authority);
      AuthorSubject.write(w, rc.link);
      w.varint(rc.authorityEpoch)
        .varint(rc.claimsRevision)
        .varint(rc.policyEpoch)
        .varint64(rc.settledThrough);
      w.varint(rc.authorizationProgress);
      break;
    }
    case "AuthorizationScopeUnavailable":
    case "CurrentRowsCancel":
      writeFixed(w, m.requestId);
      break;
    case "AuthorizationScopeDecision":
      writeFixed(w, m.requestId);
      w.variant(ADVICE.indexOf(m.advice));
      break;
    case "ChunkUploadStart":
      writeLargeValueRef(w, m.valueRef);
      break;
    case "ChunkUploadNodes":
      writeLargeValueRef(w, m.valueRef);
      w.seq(m.chunks, writeStagedChunk);
      break;
    case "ChunkUploadResult": {
      writeLargeValueRef(w, m.valueRef);
      const s = m.status;
      if (s.kind === "Need") w.variant(0).seq(s.nodes, writeNodeRef);
      else w.variant(s.kind === "Staged" ? 1 : s.kind === "RateLimited" ? 2 : 3);
      break;
    }
    case "CurrentRowsRequest":
      writeFixed(w, m.request.requestId);
      w.seq(m.request.rows, writeCoord);
      w.option(m.request.delegatedSession, writeDelegatedSessionBinding);
      break;
    case "CurrentRowsReceipt": {
      const rc = m.receipt;
      writeFixed(w, rc.requestId);
      w.seq(rc.rows, writeCoord);
      w.seq(rc.outcomes, (w, o) => w.variant(OUTCOMES.indexOf(o)));
      writePolicyBindingKey(w, rc.context);
      writeUuid(w, rc.core);
      w.varint(rc.coreEpoch)
        .varint(rc.claimsRevision)
        .varint(rc.policyEpoch)
        .varint64(rc.settledThrough);
      w.varint(rc.authorizationProgress);
      w.seq(rc.versionCarriers, writeVersionCarrier);
      break;
    }
    case "ReadBarrier":
      w.varint(m.writesThrough);
      break;
  }
}

export function readSyncMessage(r: Reader): SyncMessage {
  const kind = VARIANTS[r.variant(VARIANTS.length, "SyncMessage")];
  const rid = readFixed(16);
  switch (kind) {
    case "ChunkRequestBatch":
      return { kind, requests: readChunkRequests(r) };
    case "ChunkResponseBatch":
      return {
        kind,
        responses: r.seq((r) => {
          const requestId = r.varint();
          const tag = r.variant(3, "ChunkResponse");
          const result: ChunkResponse =
            tag === 0
              ? { kind: "Found", bytes: r.bytes().slice() }
              : tag === 1
                ? { kind: "Unavailable" }
                : { kind: "Retryable", retryAfterMs: r.varint() };
          return { requestId, result };
        }),
      };
    case "SessionClaims":
      return { kind, identity: AuthorSubject.read(r), claims: readClaims(r) };
    case "CommitUnit":
      return { kind, tx: readTransaction(r), versions: r.seq(readVersionRecord) };
    case "FateUpdate":
      return {
        kind,
        txId: readTxId(r),
        fate: readFate(r),
        globalTime: r.option((r) => r.varint64()),
        durability: r.option(readDurabilityTier),
      };
    case "RegisterShape":
      return {
        kind,
        shapeId: readShapeId(r),
        ast: readShapeAst(r),
        opts: readRegisterShapeOptions(r),
      };
    case "Subscribe":
      return { kind, subscribe: readSubscribe(r) };
    case "SubscribeRejected":
      return { kind, subscription: readSubscriptionKey(r), reason: readSubscribeRejectReason(r) };
    case "Unsubscribe":
      return { kind, subscription: readSubscriptionKey(r) };
    case "PublishSchema":
      return { kind, author: AuthorSubject.read(r), schema: readSchemaVersion(r) };
    case "PublishSchemaWithLens":
      return {
        kind,
        author: AuthorSubject.read(r),
        catalogueSeq: r.varint(),
        publication: SchemaLineagePublication.read(r),
      };
    case "PublishLens":
      return { kind, author: AuthorSubject.read(r), lens: MigrationLens.read(r) };
    case "CatalogueAck":
      return { kind, ack: readCatalogueAck(r) };
    case "ViewUpdate":
    case "ViewUpdatePart":
      return { kind, view: readViewUpdate(r) };
    case "CatalogueSnapshot":
      return { kind, snapshot: readCatalogueSnapshot(r) };
    case "AuthorizationScopeIntent":
      return {
        kind,
        requestId: rid(r),
        action: readAction(r),
        delegatedSession: r.option(readDelegatedSessionBinding),
      };
    case "AuthorizationScopeView":
      return {
        kind,
        requestId: rid(r),
        key: readScopeKey(r),
        clauseIndex: r.varint(),
        clauseCount: r.varint(),
        view: readViewUpdate(r),
      };
    case "AuthorizationScopeAggregateReceipt":
      return {
        kind,
        requestId: rid(r),
        receipt: {
          key: readScopeKey(r),
          authority: rid(r),
          link: AuthorSubject.read(r),
          authorityEpoch: r.varint(),
          claimsRevision: r.varint(),
          policyEpoch: r.varint(),
          settledThrough: r.varint64(),
          authorizationProgress: r.varint(),
        },
      };
    case "AuthorizationScopeUnavailable":
    case "CurrentRowsCancel":
      return { kind, requestId: rid(r) };
    case "AuthorizationScopeDecision":
      return { kind, requestId: rid(r), advice: ADVICE[r.variant(3, "PermissionAdvice")] };
    case "ChunkUploadStart":
      return { kind, valueRef: readLargeValueRef(r) };
    case "ChunkUploadNodes":
      return { kind, valueRef: readLargeValueRef(r), chunks: r.seq(readStagedChunk) };
    case "ChunkUploadResult": {
      const valueRef = readLargeValueRef(r);
      const tag = r.variant(4, "ChunkUploadStatus");
      const status: ChunkUploadStatus =
        tag === 0
          ? { kind: "Need", nodes: r.seq(readNodeRef) }
          : { kind: (["Staged", "RateLimited", "Rejected"] as const)[tag - 1] };
      return { kind, valueRef, status };
    }
    case "CurrentRowsRequest":
      return {
        kind,
        request: {
          requestId: rid(r),
          rows: r.seq(readCoord),
          delegatedSession: r.option(readDelegatedSessionBinding),
        },
      };
    case "CurrentRowsReceipt":
      return {
        kind,
        receipt: {
          requestId: rid(r),
          rows: r.seq(readCoord),
          outcomes: r.seq((r) => OUTCOMES[r.variant(3, "CurrentRowOutcome")]),
          context: readPolicyBindingKey(r),
          core: readUuid(r),
          coreEpoch: r.varint(),
          claimsRevision: r.varint(),
          policyEpoch: r.varint(),
          settledThrough: r.varint64(),
          authorizationProgress: r.varint(),
          versionCarriers: r.seq(readVersionCarrier),
        },
      };
    case "ReadBarrier":
      return { kind, writesThrough: r.varint() };
    default:
      // Retired tags carry an uninhabited payload.
      throw new Error(`invalid SyncMessage variant ${kind}`);
  }
}

// ---- SyncMessage semantics ----

export const uploadedVersions = (m: SyncMessage): VersionRecord[] =>
  m.kind === "CommitUnit" ? m.versions : [];

export function requiredWireFeatures(m: SyncMessage): WireFeatures {
  switch (m.kind) {
    case "AuthorizationScopeIntent":
    case "AuthorizationScopeView":
    case "AuthorizationScopeAggregateReceipt":
    case "AuthorizationScopeUnavailable":
    case "AuthorizationScopeDecision":
      return FEATURE_AUTHORIZATION_SCOPE_VIEWS;
    case "ChunkRequestBatch":
    case "ChunkResponseBatch":
    case "ChunkUploadStart":
    case "ChunkUploadNodes":
    case "ChunkUploadResult":
      return FEATURE_AUXILIARY_CHUNKS;
    default:
      return FEATURE_NONE;
  }
}

function carriedViewUpdate(m: SyncMessage): ViewUpdatePayload | undefined {
  return m.kind === "ViewUpdate" ||
    m.kind === "ViewUpdatePart" ||
    m.kind === "AuthorizationScopeView"
    ? m.view
    : undefined;
}

function validateCarriers(carriers: VersionCarrier[]): void {
  validateVersionCarrierRuns(carriers);
  for (const c of carriers)
    for (const b of carrierBundleRefs(c)) validateVersionRecords(b.versions);
}

export function validateVersionCarriers(m: SyncMessage): void {
  if (m.kind === "CommitUnit") return validateVersionRecords(m.versions);
  if (m.kind === "CurrentRowsReceipt") return validateCarriers(m.receipt.versionCarriers);
  const view = carriedViewUpdate(m);
  if (view) validateCarriers(view.versionCarriers);
}

export type WireContractErrorKind =
  | "InvalidSupportingRevision"
  | "VersionCarrier"
  | "InvalidSupportingRow"
  | "DuplicateSupportingRow";
export class WireContractError extends Error {
  constructor(
    readonly kind: WireContractErrorKind,
    cause?: VersionBundleRunError,
  ) {
    super(
      kind === "InvalidSupportingRevision"
        ? "supporting transition revision is invalid"
        : kind === "VersionCarrier"
          ? cause!.message
          : kind === "InvalidSupportingRow"
            ? "supporting row reference is invalid"
            : "supporting snapshot duplicates an exact physical row version",
    );
  }
}

const isNil = (b: Uint8Array) => b.every((x) => x === 0);
const sameRev = (a: Uint8Array, b: Uint8Array) => a.every((x, i) => x === b[i]);

export function validateWireContract(m: SyncMessage): void {
  try {
    validateVersionCarriers(m);
  } catch (e) {
    if (e instanceof VersionBundleRunError) throw new WireContractError("VersionCarrier", e);
    throw e;
  }
  const view = carriedViewUpdate(m);
  if (!view) return;
  const u = view.supportingRows;
  if (
    isNil(u.revision) ||
    (u.kind === "Delta" &&
      (isNil(u.predecessor) ||
        (sameRev(u.predecessor, u.revision) && (u.adds.length > 0 || u.removes.length > 0)))) ||
    (u.kind === "CatchUp" && (isNil(u.predecessor) || sameRev(u.predecessor, u.revision)))
  )
    throw new WireContractError("InvalidSupportingRevision");
  const identities = new SortedSet<string>(cmpStr);
  for (const row of [...supportingAddedRows(u), ...supportingRemovedRows(u)]) {
    if (!supportingRowIsWireValid(row)) throw new WireContractError("InvalidSupportingRow");
    if (!identities.add(entryKey(row))) throw new WireContractError("DuplicateSupportingRow");
  }
}

// ---- local events ----

export type LocalEvent =
  | { kind: "Mutate"; table: string }
  | { kind: "OpenTx" }
  | { kind: "WriteInTx" | "CommitTx" | "AbandonTx"; tx: TxId }
  | { kind: "Query"; queryRef: number }
  | { kind: "Subscribe"; queryRef: number; subscription: SubscriptionKey }
  | { kind: "Unsubscribe"; subscription: SubscriptionKey };
export type Event = { kind: "Sync"; message: SyncMessage } | { kind: "Local"; event: LocalEvent };
export type OutboxMessage =
  | { kind: "Sync"; message: SyncMessage }
  | { kind: "QueryResult"; queryRef: number }
  | { kind: "SubscriptionNotification"; subscription: SubscriptionKey }
  | { kind: "TxFate"; txId: TxId; fate: Fate };
