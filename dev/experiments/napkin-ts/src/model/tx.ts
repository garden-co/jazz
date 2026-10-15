// Transaction, fate, durability, snapshot, and read-set vocabulary.
import { SortedMap, SortedSet, cmpStr, cmpBytes } from "../support/collections.js";
import { toHex } from "../support/bytes.js";
import {
  RecordDescriptor,
  EnumSchema,
  EnumCase,
  EnumValue,
  RowAuthor,
  nodeUuidBytes,
  rowUuidBytes,
  txTimePhysicalMs,
  type GValue,
  type GValueType,
  type NodeUuid,
  type GlobalTime,
  type TxTime,
  type RowUuid,
  type PhysicalTableId,
  type SchemaVersionId,
  type Query,
  type ShapeId,
  type BindingId,
} from "../_extern/model-core.js";
import { BranchKey, type SnapshotRef } from "./model/branch.js";
import type { TableSchema } from "./schema.js";

/** Transaction identity: HLC time plus creating node tie-breaker. */
export type TxId = { time: TxTime; node: NodeUuid };
export const txId = (time: TxTime, node: NodeUuid): TxId => ({ time, node });
export const txPhysicalMs = (id: TxId): number => txTimePhysicalMs(id.time);
export const cmpTxId = (a: TxId, b: TxId) =>
  Number(a.time) - Number(b.time) || cmpBytes(nodeUuidBytes(a.node), nodeUuidBytes(b.node));

export type TxKind = "Mergeable" | "Exclusive";
export type DeletionEvent = "Deleted" | "Restored";
export type DurabilityTier = "None" | "Local" | "Global";
export type MergeAspect = "Content" | "Deletion";

export type RejectionReason =
  | { kind: "ClientClockTooFarAhead" }
  | { kind: "AuthorizationDenied" }
  | { kind: "ExclusiveConflict" }
  | { kind: "CausalityViolation" }
  | { kind: "Cascade"; root: TxId }
  | { kind: "MalformedCommit"; message: string };

export type Fate =
  | { kind: "Pending" }
  | { kind: "Accepted" }
  | { kind: "Rejected"; reason: RejectionReason };

const NON_CANONICAL = () => new Error("non-canonical record");
let rejectionSchema: any;

export const RejectionReason = {
  storageValueType(): GValueType {
    const empty = () => new RecordDescriptor([]);
    rejectionSchema ??= new EnumSchema("jazz_rejection_reason", [
      new EnumCase("client_clock_too_far_ahead", empty()),
      new EnumCase("authorization_denied", empty()),
      new EnumCase("exclusive_conflict", empty()),
      new EnumCase("causality_violation", empty()),
      new EnumCase(
        "cascade",
        new RecordDescriptor([
          ["root", { kind: "Tuple", members: [{ kind: "U64" }, { kind: "Uuid" }] }],
        ]),
      ),
      new EnumCase("malformed_commit", new RecordDescriptor([["message", { kind: "String" }]])),
    ]);
    return { kind: "Enum", schema: rejectionSchema };
  },

  toStorageValue(r: RejectionReason): GValue {
    let tag: number;
    let fields: GValue[] = [];
    switch (r.kind) {
      case "ClientClockTooFarAhead":
        tag = 0;
        break;
      case "AuthorizationDenied":
        tag = 1;
        break;
      case "ExclusiveConflict":
        tag = 2;
        break;
      case "CausalityViolation":
        tag = 3;
        break;
      case "Cascade":
        tag = 4;
        fields = [
          {
            kind: "Tuple",
            value: [
              { kind: "U64", value: r.root.time },
              { kind: "Uuid", value: r.root.node },
            ],
          },
        ];
        break;
      case "MalformedCommit":
        tag = 5;
        fields = [{ kind: "String", value: r.message }];
    }
    const schema = RejectionReason.storageValueType().schema;
    return { kind: "Enum", value: EnumValue.create(tag, schema.case(tag).payload, fields) };
  },

  fromStorageValue(value: GValue): RejectionReason {
    if (value.kind !== "Enum") throw NON_CANONICAL();
    const ev = value.value;
    const payload = RejectionReason.storageValueType().schema.case(ev.tag()).payload;
    const fields: GValue[] = payload.bind(ev.record().raw()).toValues();
    if (cmpBytes(payload.create(fields), ev.record().raw()) !== 0) throw NON_CANONICAL();
    switch (ev.tag()) {
      case 0:
      case 1:
      case 2:
      case 3:
        if (fields.length) throw NON_CANONICAL();
        return {
          kind: (
            [
              "ClientClockTooFarAhead",
              "AuthorizationDenied",
              "ExclusiveConflict",
              "CausalityViolation",
            ] as const
          )[ev.tag()],
        };
      case 4:
        if (fields.length !== 1) throw NON_CANONICAL();
        try {
          return { kind: "Cascade", root: txIdFromValue(fields[0]) };
        } catch {
          throw NON_CANONICAL();
        }
      case 5:
        if (fields.length !== 1 || fields[0].kind !== "String") throw NON_CANONICAL();
        return { kind: "MalformedCommit", message: fields[0].value };
      default:
        throw NON_CANONICAL();
    }
  },
};

/** Immutable transaction payload before upstream fate state are learned. */
export type Transaction = {
  tx_id: TxId;
  kind: TxKind;
  n_total_writes: number;
  made_by: any;
  permission_subject?: any;
  base_snapshot?: Snapshot;
  row_read_set?: RowRead[];
  absent_read_set?: AbsentRead[];
  predicate_read_set?: PredicateRead[];
  user_metadata_json?: string;
  contribution_merge?: ContributionMergeProvenance;
};

export type BranchWriteOperation =
  | { kind: "ExactHeadInsert" }
  | { kind: "ExactHeadUpdate" }
  | { kind: "ViewUpdateCopy"; evidence: BranchViewCopyEvidence };

export type BranchWriteIntent = {
  version: number;
  physical_table_id: PhysicalTableId;
  authored_schema: SchemaVersionId;
  row_uuid: RowUuid;
  head: BranchKey;
  operation: BranchWriteOperation;
};

export type BranchViewCopyBase =
  | { kind: "Current"; branch: BranchKey }
  | { kind: "Snapshot"; branch: BranchKey; snapshot: SnapshotRef };

export type BranchViewCopyEvidence = {
  version: number;
  head: BranchKey;
  base: BranchViewCopyBase;
  table: string;
  row_uuid: RowUuid;
  source_version: TxId;
};

export type ContributionComponent =
  | { kind: "Column"; name: string }
  | { kind: "Operation"; column: string; identity: Uint8Array }
  | { kind: "Register" };

export type ContributionCoordinate = {
  branch_key: BranchKey;
  table: string;
  row_uuid: RowUuid;
  layer: MergeAspect;
  component: ContributionComponent;
};
export type ContributionDot = { tx_id: TxId; coordinate: ContributionCoordinate };
export type ContributionSubstitution = {
  target: ContributionCoordinate;
  sources: ContributionDot[];
};

// Derived Ord replaced by a canonical string key (ordering and equality).
const branchKeyKey = (k: BranchKey) => k.values.map(([n, v]) => `${n}=${toHex(v.bytes)}`).join(",");
const componentKey = (c: ContributionComponent) =>
  c.kind === "Column"
    ? `0:${c.name}`
    : c.kind === "Operation"
      ? `1:${c.column}:${toHex(c.identity)}`
      : "2";
export const coordinateKey = (c: ContributionCoordinate) =>
  `${branchKeyKey(c.branch_key)}|${c.table}|${toHex(rowUuidBytes(c.row_uuid))}|${c.layer === "Content" ? 0 : 1}|${componentKey(c.component)}`;
export const dotKey = (d: ContributionDot) =>
  `${String(d.tx_id.time).padStart(20, "0")}:${toHex(nodeUuidBytes(d.tx_id.node))}|${coordinateKey(d.coordinate)}`;
const intentKey = (i: BranchWriteIntent) =>
  `${String(i.physical_table_id).padStart(20, "0")}|${String(i.authored_schema)}|${toHex(rowUuidBytes(i.row_uuid))}|${branchKeyKey(i.head)}`;
const deepKey = (x: unknown) =>
  JSON.stringify(x, (_k, v) =>
    typeof v === "bigint"
      ? v.toString()
      : v instanceof Uint8Array
        ? toHex(v)
        : v instanceof BranchKey
          ? branchKeyKey(v)
          : v,
  );

export class ContributionMergeProvenance {
  constructor(
    public source: BranchKey,
    public target: BranchKey,
    public substitutions: ContributionSubstitution[],
    public branch_view_copies: BranchViewCopyEvidence[] = [],
    public branch_write_intents: BranchWriteIntent[] = [],
  ) {}

  /** Canonicalize and validate locally calculated provenance. */
  static canonical(
    source: BranchKey,
    target: BranchKey,
    substitutions: ContributionSubstitution[],
  ) {
    const subs = substitutions.map((s) => {
      const seen = new Map<string, ContributionDot>();
      for (const d of s.sources) seen.set(dotKey(d), d);
      const sources = [...seen.entries()].sort((a, b) => cmpStr(a[0], b[0])).map((e) => e[1]);
      if (!sources.length) throw new Error("contribution substitution requires a source dot");
      return { target: s.target, sources };
    });
    subs.sort((a, b) => cmpStr(coordinateKey(a.target), coordinateKey(b.target)));
    for (let i = 1; i < subs.length; i++)
      if (coordinateKey(subs[i - 1].target) === coordinateKey(subs[i].target))
        throw new Error("contribution substitution targets must be unique");
    return new ContributionMergeProvenance(source, target, subs);
  }

  static branchViewCopy(evidence: BranchViewCopyEvidence) {
    return new ContributionMergeProvenance(new BranchKey(), new BranchKey(), [], [evidence]);
  }

  /** Reject non-canonical or incomplete provenance received from a helper. */
  validate(): void {
    const ok = (k: BranchKey) => {
      try {
        k.tryCanonicalBytes();
        return true;
      } catch {
        return false;
      }
    };
    if (
      !ok(this.source) ||
      !ok(this.target) ||
      this.substitutions.some(
        (s) => !ok(s.target.branch_key) || s.sources.some((d) => !ok(d.coordinate.branch_key)),
      )
    )
      throw new Error("contribution provenance branch key is invalid");
    const canonical = ContributionMergeProvenance.canonical(
      this.source,
      this.target,
      this.substitutions,
    );
    if (
      branchKeyKey(canonical.source) !== branchKeyKey(this.source) ||
      branchKeyKey(canonical.target) !== branchKeyKey(this.target) ||
      deepKey(canonical.substitutions) !== deepKey(this.substitutions)
    )
      throw new Error("contribution merge provenance must be canonical");
    if (
      this.branch_view_copies.length &&
      (this.substitutions.length || this.source.values.length || this.target.values.length)
    )
      throw new Error("branch-view copy evidence must not carry contribution substitutions");
    const seen = new Set<string>();
    for (const e of this.branch_view_copies) {
      const k = `${e.table}|${toHex(rowUuidBytes(e.row_uuid))}|${branchKeyKey(e.head)}`;
      if (e.version !== 1 || seen.has(k))
        throw new Error("branch-view copy evidence must be canonical v1 provenance");
      seen.add(k);
    }
    const intents = this.branch_write_intents
      .slice()
      .sort((a, b) => cmpStr(intentKey(a), intentKey(b)));
    if (
      deepKey(intents) !== deepKey(this.branch_write_intents) ||
      intents.some((it, i) => i > 0 && intentKey(intents[i - 1]) === intentKey(it)) ||
      intents.some((it) => it.version !== 1 || !it.head.values.length)
    )
      throw new Error("branch write intents must be canonical v1 coordinates");
    const expected: BranchViewCopyEvidence[] = [];
    for (const intent of this.branch_write_intents) {
      if (intent.operation.kind !== "ViewUpdateCopy") continue;
      const e = intent.operation.evidence;
      if (
        deepKey(e.row_uuid) !== deepKey(intent.row_uuid) ||
        branchKeyKey(e.head) !== branchKeyKey(intent.head)
      )
        throw new Error("branch write copy evidence is not bound to its intent");
      expected.push(e);
    }
    if (deepKey(this.branch_view_copies) !== deepKey(expected))
      throw new Error("branch write copy evidence must be the canonical intent projection");
  }
}

/** Index of non-causal substitutions used to compare contribution histories. */
export class ContributionSubstitutionIndex {
  private substitutions = new Map<string, ContributionDot[]>();

  observe(tx: TxId, provenance: ContributionMergeProvenance): void {
    for (const s of provenance.substitutions) {
      const key = dotKey({ tx_id: tx, coordinate: s.target });
      if (this.substitutions.has(key))
        throw new Error("contribution substitution target observed twice");
      this.substitutions.set(key, s.sources);
    }
  }

  expand(dots: Iterable<ContributionDot>): SortedMap<string, ContributionDot> {
    const expanded = new SortedMap<string, ContributionDot>(cmpStr);
    const visit = (dot: ContributionDot, visiting: Set<string>) => {
      const key = dotKey(dot);
      const sources = this.substitutions.get(key);
      if (!sources) {
        expanded.set(key, dot);
        return;
      }
      if (visiting.has(key)) throw new Error("contribution substitution cycle");
      visiting.add(key);
      for (const s of sources) visit(s, visiting);
      visiting.delete(key);
    };
    for (const d of dots) visit(d, new Set());
    return expanded;
  }

  novel(source: Iterable<ContributionDot>, target: Iterable<ContributionDot>): ContributionDot[] {
    const s = this.expand(source);
    const t = this.expand(target);
    return [...s].filter(([k]) => !t.has(k)).map(([, d]) => d);
  }
}

/** Durable transaction audit record. */
export type TransactionRecord = {
  tx_id: TxId;
  made_by: any;
  kind: TxKind;
  n_total_writes: number;
  fate: Fate;
  global_time?: GlobalTime;
  durability: DurabilityTier;
  user_metadata_json?: string;
  contribution_merge?: ContributionMergeProvenance;
};

const HISTORY_USER_CELLS = 8;
const REJ_TX = { KIND: 2, MADE_BY: 3, REJECTION_REASON: 4, USER_METADATA: 5 };
const REJ_VERSION = { ROW_UUID: 2, DELETION: 3, USER_CELLS: 4 };

function nullableValue(v: GValue): GValue | undefined {
  if (v.kind !== "Nullable") throw new Error("nullable value expected");
  return v.value;
}

function deletionFromValue(v: GValue): DeletionEvent | undefined {
  if (v.kind === "Nullable") return v.value === undefined ? undefined : deletionFromValue(v.value);
  if ((v.kind === "EnumTag" || v.kind === "U8") && (v.value === 0 || v.value === 1))
    return v.value === 0 ? "Deleted" : "Restored";
  throw new Error("deletion");
}

function txIdFromValue(v: GValue): TxId {
  if (v.kind !== "Tuple" || v.value.length !== 2) throw new Error("tx id tuple");
  const [t, n] = v.value;
  if (t.kind !== "U64") throw new Error("tx id time");
  if (n.kind !== "Uuid") throw new Error("tx id node");
  return { time: t.value, node: n.value };
}

function tableColumnPosition(table: TableSchema, column: string): number | undefined {
  const i = table.columns.findIndex((c) => c.name === column);
  return i < 0 ? undefined : i;
}

/** Stored edit-history entry for a row. */
export class HistoryEntry {
  constructor(
    readonly table: string,
    private readonly version: any,
    private readonly transaction: TransactionRecord,
    readonly isLocallyCurrent: boolean,
    readonly isGloballyCurrent: boolean,
  ) {}
  txId = () => this.transaction.tx_id;
  madeBy = () => this.transaction.made_by;
  madeAt = () => this.transaction.tx_id.time;
  kind = () => this.transaction.kind;
  fate = () => this.transaction.fate;
  globalTime = () => this.transaction.global_time;
  durability = () => this.transaction.durability;

  cellAt(position: number): GValue | undefined {
    const d = this.version.descriptor();
    const idx = d.fieldIndex("_deletion") ?? d.fieldIndex("updated_at");
    const userCells = idx === undefined ? HISTORY_USER_CELLS : idx + 1;
    return nullableValue(this.version.borrowed().getIdx(userCells + position));
  }
  cell(table: TableSchema, column: string): GValue | undefined {
    const i = tableColumnPosition(table, column);
    return i === undefined ? undefined : this.cellAt(i);
  }
  deletion(): DeletionEvent | undefined {
    const field = this.version.descriptor().fieldIndex("_deletion");
    if (field === undefined) return undefined;
    return deletionFromValue(this.version.borrowed().getIdx(field));
  }
  layer(): MergeAspect {
    return this.deletion() !== undefined ? "Deletion" : "Content";
  }
}

/** Durable rejected transaction payload retained on its originating node. */
export class RejectedTransaction {
  constructor(
    readonly tx_id: TxId,
    private readonly record: any,
    readonly versions: RejectedVersion[],
  ) {}
  kind(): TxKind {
    const d = this.record.borrowed().getEnum(REJ_TX.KIND);
    if (d === 0) return "Mergeable";
    if (d === 1) return "Exclusive";
    throw new Error("unknown tx kind");
  }
  madeBy(): any {
    return RowAuthor.fromRecord(this.record.borrowed().getRecord(REJ_TX.MADE_BY)).asAuthorSubject();
  }
  madeAt(): TxTime {
    return this.tx_id.time;
  }
  reason(): RejectionReason {
    let v: GValue;
    try {
      v = this.record.borrowed().getIdx(REJ_TX.REJECTION_REASON);
      return RejectionReason.fromStorageValue(v);
    } catch {
      throw new Error("valid rejected reason");
    }
  }
  cascadeRoot(): TxId | undefined {
    const r = this.reason();
    return r.kind === "Cascade" ? r.root : undefined;
  }
  userMetadataJson(): string | undefined {
    return this.record.borrowed().getNullableString(REJ_TX.USER_METADATA);
  }
}

export class RejectedVersion {
  constructor(
    readonly table: string,
    private readonly record: any,
  ) {}
  rowUuid(): RowUuid {
    return this.record.borrowed().getUuid(REJ_VERSION.ROW_UUID);
  }
  cellAt(position: number): GValue | undefined {
    return nullableValue(this.record.borrowed().getIdx(REJ_VERSION.USER_CELLS + position));
  }
  cell(table: TableSchema, column: string): GValue | undefined {
    const i = tableColumnPosition(table, column);
    return i === undefined ? undefined : this.cellAt(i);
  }
  deletion(): DeletionEvent | undefined {
    return deletionFromValue(this.record.borrowed().getIdx(REJ_VERSION.DELETION));
  }
}

/** Compact dotted view description captured by the node that created it. */
export type Snapshot = {
  owner: NodeUuid;
  global_base: GlobalTime;
  local_base: TxTime;
  dots: TxId[];
};
export const exclusiveBaseSnapshot = (
  owner: NodeUuid,
  global_base: GlobalTime,
  local_base: TxTime,
  dots: TxId[],
): Snapshot => ({
  owner,
  global_base,
  local_base,
  dots,
});

export type RowRead = { table: string; row_uuid: RowUuid; version: TxId };
export type AbsentRead = { table: string; row_uuid: RowUuid };
export type PredicateRead = {
  table: string;
  shape_id: ShapeId;
  shape: Query;
  binding_id: BindingId;
  binding_values: SortedMap<string, GValue>;
};

export const newDotSet = () => new SortedSet<string>(cmpStr);
