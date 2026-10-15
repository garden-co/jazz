import type { Reader, Writer } from "../../support/postcard.js";
import { cmpStr, cmpBig } from "../../support/collections.js";
import { compareBytes, concatBytes, equalBytes } from "../../support/bytes.js";
import {
  AuthorSubject,
  RowAuthor,
  readUuid,
  writeUuid,
  type RowUuid,
  type SchemaVersionId,
} from "../../types/ids.js";
import type { GlobalTime } from "../../types/time.js";
import { forceSingletonVersionCarriers } from "../../types/debug_env.js";
import {
  APP_COLUMN_PREFIX,
  GrooveRecordError,
  OwnedRecord,
  Value,
  compareBranchKey,
  compareTxId,
  decodePersistedRecordDescriptor,
  defaultBranchKey,
  descriptorKey,
  encodePersistedRecordDescriptor,
  fateEquals,
  readBranchKey,
  readDurabilityTier,
  readFate,
  readTransaction,
  readTxId,
  transactionEquals,
  wireRecordDescriptor,
  writeBranchKey,
  writeDurabilityTier,
  writeFate,
  writeTransaction,
  writeTxId,
  type BranchKey,
  type DeletionEvent,
  type DurabilityTier,
  type Fate,
  type TableSchema,
  type Transaction,
  type TxId,
} from "../../_extern/types-protocol.js";

/** Exact row-version identity used by known-state repair requests. */
export interface RowVersionRef {
  table: string;
  rowUuid: RowUuid;
  txTime: bigint;
  txNodeId: string;
}
export const rowVersionRefNew = (table: string, rowUuid: RowUuid, tx: TxId): RowVersionRef => ({
  table,
  rowUuid,
  txTime: tx.time,
  txNodeId: tx.node,
});
export const rowVersionRefTxId = (r: RowVersionRef): TxId => ({ time: r.txTime, node: r.txNodeId });

export interface PeerPayloadInventory {
  completeTxPayloads: TxId[];
  authorizationProgress?: number;
  openingPending: boolean;
}
export const writePeerPayloadInventory = (w: Writer, v: PeerPayloadInventory) => {
  w.seq(v.completeTxPayloads, writeTxId);
  w.option(v.authorizationProgress, (w, n) => w.varint(n));
  w.bool(v.openingPending);
};
export const readPeerPayloadInventory = (r: Reader): PeerPayloadInventory => ({
  completeTxPayloads: r.seq(readTxId),
  authorizationProgress: r.option((r) => r.varint()),
  openingPending: r.bool(),
});

/** The row image a version's writer made it over (SPEC ch. 4 §4.6). */
export interface RowBase {
  seq?: GlobalTime;
  pending?: TxId;
}
export const rowBaseIsEmpty = (b: RowBase) => b.seq === undefined && b.pending === undefined;
function compareRowBase(a: RowBase, b: RowBase): number {
  const opt = <T>(x: T | undefined, y: T | undefined, c: (x: T, y: T) => number) =>
    x === undefined ? (y === undefined ? 0 : -1) : y === undefined ? 1 : c(x, y);
  return opt(a.seq, b.seq, cmpBig) || opt(a.pending, b.pending, compareTxId);
}
const writeRowBase = (w: Writer, b: RowBase) => {
  w.option(b.seq, (w, s) => w.varint64(s));
  w.option(b.pending, writeTxId);
};
const readRowBase = (r: Reader): RowBase => ({
  seq: r.option((r) => r.varint64()),
  pending: r.option(readTxId),
});

// groove::define_record! WireRowRecord field layout.
const FIELD_ROW_UUID_IDX = 0;
const FIELD_CREATED_BY_IDX = 1;
const FIELD_CREATED_AT_IDX = 2;
const FIELD_UPDATED_BY_IDX = 3;
const FIELD_UPDATED_AT_IDX = 4;
const FIELD__DELETION_IDX = 5;
const USER_CELLS = 6;

/** Explicit immutable-version row encoding (JVRR v2 envelope). */
namespace versionRecordWireRow {
  const MAGIC = Uint8Array.of(0x4a, 0x56, 0x52, 0x52, 0x02); // "JVRR\x02"
  const MAX_DESCRIPTOR_PROOFS = 256;
  const MAX_DESCRIPTOR_PROOF_BYTES = 64 * 1024;
  const MAX_DESCRIPTOR_PROOF_CACHE_BYTES = 1024 * 1024;

  interface DescriptorProof {
    source: any;
    canonical: any;
    encoded: Uint8Array;
    encodedKey: string;
  }

  const bytesKey = (b: Uint8Array) => String.fromCharCode(...b);

  /** Bounded FIFO of descriptor proofs indexed by source descriptor and encoded bytes. */
  class DescriptorProofCache {
    private entries: [number, DescriptorProof][] = [];
    private bySourceIdx = new Map<string, number>();
    private byEncodedIdx = new Map<string, number>();
    private nextId = 0;
    private bytes = 0;

    private get(id: number): DescriptorProof | undefined {
      if (!this.entries.length) return undefined;
      const index = id - this.entries[0][0];
      return index >= 0 ? this.entries[index]?.[1] : undefined;
    }

    bySource(descriptor: any): DescriptorProof | undefined {
      const id = this.bySourceIdx.get(descriptorKey(descriptor));
      return id === undefined ? undefined : this.get(id);
    }

    byEncoded(encoded: Uint8Array): DescriptorProof | undefined {
      const newest = this.entries[this.entries.length - 1];
      if (newest && equalBytes(newest[1].encoded, encoded)) return newest[1];
      const id = this.byEncodedIdx.get(bytesKey(encoded));
      return id === undefined ? undefined : this.get(id);
    }

    remember(proof: DescriptorProof): void {
      if (proof.encoded.length > MAX_DESCRIPTOR_PROOF_BYTES) return;
      while (
        this.entries.length >= MAX_DESCRIPTOR_PROOFS ||
        this.bytes + proof.encoded.length > MAX_DESCRIPTOR_PROOF_CACHE_BYTES
      ) {
        const [id, evicted] = this.entries.shift()!;
        this.bytes -= evicted.encoded.length;
        const sk = descriptorKey(evicted.source);
        if (this.bySourceIdx.get(sk) === id) this.bySourceIdx.delete(sk);
        if (this.byEncodedIdx.get(evicted.encodedKey) === id)
          this.byEncodedIdx.delete(evicted.encodedKey);
      }
      const id = this.nextId++;
      this.bytes += proof.encoded.length;
      this.bySourceIdx.set(descriptorKey(proof.source), id);
      this.byEncodedIdx.set(proof.encodedKey, id);
      this.entries.push([id, proof]);
    }
  }

  const proofs = new DescriptorProofCache();

  function descriptorForEncode(descriptor: any): DescriptorProof {
    const hit = proofs.bySource(descriptor);
    if (hit) return hit;
    const encoded = encodePersistedRecordDescriptor(descriptor);
    const canonical = decodePersistedRecordDescriptor(encoded);
    const proof = { source: descriptor, canonical, encoded, encodedKey: bytesKey(encoded) };
    proofs.remember(proof);
    return proof;
  }

  function descriptorForDecode(encoded: Uint8Array): DescriptorProof {
    const hit = proofs.byEncoded(encoded);
    if (hit) return hit;
    const canonical = decodePersistedRecordDescriptor(encoded);
    const copy = encoded.slice();
    const proof = { source: canonical, canonical, encoded: copy, encodedKey: bytesKey(copy) };
    proofs.remember(proof);
    return proof;
  }

  export function encode(record: any): Uint8Array {
    const descriptor = descriptorForEncode(record.descriptor()).encoded;
    const length = new Uint8Array(4);
    new DataView(length.buffer).setUint32(0, descriptor.length, true);
    return concatBytes(MAGIC, length, descriptor, record.raw());
  }

  export function decode(bytes: Uint8Array): any {
    const invalid = () => GrooveRecordError.NonCanonicalRecord();
    if (bytes.length < MAGIC.length || !MAGIC.every((b, i) => bytes[i] === b)) throw invalid();
    if (bytes.length < 9) throw invalid();
    const length = new DataView(bytes.buffer, bytes.byteOffset + 5, 4).getUint32(0, true);
    const end = 9 + length;
    if (end > bytes.length) throw invalid();
    const descriptor = descriptorForDecode(bytes.subarray(9, end)).canonical;
    return new OwnedRecord(bytes.slice(end), descriptor);
  }
}

function deletionFromValue(value: any): DeletionEvent | undefined {
  if (value?.kind !== "Nullable") throw new Error("deletion");
  if (value.value === undefined) return undefined;
  if (value.value.kind === "EnumTag" && value.value.value === 0) return "Deleted";
  if (value.value.kind === "EnumTag" && value.value.value === 1) return "Restored";
  throw new Error("deletion");
}

function nullableValue(value: any): any | undefined {
  if (value?.kind !== "Nullable") throw new Error("nullable value expected");
  return value.value;
}

/** One immutable row-version payload carried by a committed transaction. */
export class VersionRecord {
  branchKey: BranchKey = defaultBranchKey();
  authoredColumns?: Set<string>;
  base: RowBase = {};
  counterSigns: Uint8Array = new Uint8Array(0);

  constructor(
    readonly table: string,
    readonly schemaVersion: SchemaVersionId,
    readonly record: any,
  ) {}

  withBranchKey(k: BranchKey): this {
    this.branchKey = k;
    return this;
  }
  withAuthoredColumns(c: Set<string> | undefined): this {
    this.authoredColumns = c;
    return this;
  }
  withBase(b: RowBase): this {
    this.base = b;
    return this;
  }
  withCounterSigns(s: Uint8Array): this {
    this.counterSigns = s;
    return this;
  }

  /** Validate the complete immutable receipt without infallible accessors. */
  validateReceipt(): void {
    const malformed = () =>
      new VersionBundleRunError({ kind: "MalformedVersionRecord", table: this.table });
    const fields: any[] = this.record.descriptor().fields();
    const fixedNames = [
      "row_uuid",
      "created_by",
      "created_at",
      "updated_by",
      "updated_at",
      "_deletion",
    ];
    if (
      fields.length < fixedNames.length ||
      fixedNames.some((name, i) => fields[i].name !== name) ||
      fields
        .slice(USER_CELLS)
        .some((f) => !(typeof f.name === "string" && f.name.startsWith(APP_COLUMN_PREFIX)))
    )
      throw malformed();
    const borrowed = this.record.borrowed();
    try {
      borrowed.getUuid(FIELD_ROW_UUID_IDX);
      for (const index of [FIELD_CREATED_BY_IDX, FIELD_UPDATED_BY_IDX])
        RowAuthor.fromRecord(borrowed.getRecord(index));
      borrowed.getU64(FIELD_CREATED_AT_IDX);
      borrowed.getU64(FIELD_UPDATED_AT_IDX);
      deletionFromValue(borrowed.getIdx(FIELD__DELETION_IDX));
      const values: any[] = [];
      for (let i = 0; i < fields.length; i++) values.push(borrowed.getIdx(i));
      for (let i = USER_CELLS; i < fields.length; i++) nullableValue(values[i]);
      const canonical = this.record.descriptor().create(values);
      if (!equalBytes(canonical, this.record.raw())) throw malformed();
    } catch {
      throw malformed();
    }
  }

  static encode(
    table: TableSchema,
    schemaVersion: SchemaVersionId,
    rowUuid: RowUuid,
    createdBy: AuthorSubject,
    createdAtMs: number,
    updatedBy: AuthorSubject,
    updatedAtMs: number,
    cellsByPosition: (any | undefined)[],
    deletion: DeletionEvent | undefined,
  ): VersionRecord {
    const descriptor = wireRecordDescriptor(table);
    const author = (s: AuthorSubject) => {
      try {
        return RowAuthor.fromPersistedSubject(s).toValue();
      } catch {
        throw GrooveRecordError.NonCanonicalRecord();
      }
    };
    const values = [
      Value.Uuid(rowUuid),
      author(createdBy),
      Value.U64(createdAtMs),
      author(updatedBy),
      Value.U64(updatedAtMs),
      Value.Nullable(
        deletion === undefined ? undefined : Value.EnumTag(deletion === "Deleted" ? 0 : 1),
      ),
      ...table.columns.map((_: unknown, idx: number) => Value.Nullable(cellsByPosition[idx])),
    ];
    return new VersionRecord(
      table.name,
      schemaVersion,
      new OwnedRecord(descriptor.create(values), descriptor),
    );
  }

  static fromCells(
    table: TableSchema,
    schemaVersion: SchemaVersionId,
    rowUuid: RowUuid,
    createdBy: AuthorSubject,
    createdAtMs: number,
    updatedBy: AuthorSubject,
    updatedAtMs: number,
    cells: Map<string, any>,
    deletion: DeletionEvent | undefined,
  ): VersionRecord {
    const positional = table.columns.map((c: any) => cells.get(c.name));
    return VersionRecord.encode(
      table,
      schemaVersion,
      rowUuid,
      createdBy,
      createdAtMs,
      updatedBy,
      updatedAtMs,
      positional,
      deletion,
    );
  }

  rowUuid(): RowUuid {
    return this.record.borrowed().getUuid(FIELD_ROW_UUID_IDX);
  }
  deletesRow(): boolean {
    return (
      this.deletion() === "Deleted" &&
      (!this.authoredColumns || this.authoredColumns.has("_deletion"))
    );
  }
  deletion(): DeletionEvent | undefined {
    return deletionFromValue(this.record.borrowed().getIdx(FIELD__DELETION_IDX));
  }
  createdBy(): AuthorSubject {
    return RowAuthor.fromRecord(
      this.record.borrowed().getRecord(FIELD_CREATED_BY_IDX),
    ).asAuthorSubject();
  }
  createdAtMs(): number {
    return this.record.borrowed().getU64(FIELD_CREATED_AT_IDX);
  }
  updatedBy(): AuthorSubject {
    return RowAuthor.fromRecord(
      this.record.borrowed().getRecord(FIELD_UPDATED_BY_IDX),
    ).asAuthorSubject();
  }
  updatedAtMs(): number {
    return this.record.borrowed().getU64(FIELD_UPDATED_AT_IDX);
  }
  cellAt(position: number): any | undefined {
    return nullableValue(this.record.borrowed().getIdx(USER_CELLS + position));
  }
  optionalCellAt(position: number): any | undefined {
    const field = USER_CELLS + position;
    if (field >= this.record.descriptor().fields().length) return undefined;
    try {
      return nullableValue(this.record.borrowed().getIdx(field));
    } catch {
      return undefined;
    }
  }
  applicationCellCount(): number {
    return Math.max(0, this.record.descriptor().fields().length - USER_CELLS);
  }

  static compare(a: VersionRecord, b: VersionRecord): number {
    const cols = (s?: Set<string>) => (s ? [...s].sort() : undefined);
    const ca = cols(a.authoredColumns);
    const cb = cols(b.authoredColumns);
    return (
      cmpStr(a.table, b.table) ||
      cmpStr(a.schemaVersion, b.schemaVersion) ||
      compareBranchKey(a.branchKey, b.branchKey) ||
      compareBytes(a.record.raw(), b.record.raw()) ||
      (ca === undefined
        ? cb === undefined
          ? 0
          : -1
        : cb === undefined
          ? 1
          : cmpStr(ca.join("\0"), cb.join("\0"))) ||
      compareRowBase(a.base, b.base) ||
      compareBytes(a.counterSigns, b.counterSigns)
    );
  }
}

export function writeVersionRecord(w: Writer, v: VersionRecord): void {
  w.str(v.table);
  writeUuid(w, v.schemaVersion);
  writeBranchKey(w, v.branchKey);
  w.bytes(versionRecordWireRow.encode(v.record));
  w.option(v.authoredColumns && [...v.authoredColumns].sort(), (w, cols) =>
    w.seq(cols, (w, c) => w.str(c)),
  );
  writeRowBase(w, v.base);
  w.bytes(v.counterSigns);
}

export function readVersionRecord(r: Reader): VersionRecord {
  const table = r.str();
  const schema = readUuid(r);
  const branchKey = readBranchKey(r);
  const v = new VersionRecord(table, schema, versionRecordWireRow.decode(r.bytes())).withBranchKey(
    branchKey,
  );
  const cols = r.option((r) => r.seq((r) => r.str()));
  v.authoredColumns = cols && new Set(cols);
  v.base = readRowBase(r);
  v.counterSigns = r.bytes().slice();
  return v;
}

export function validateVersionRecords(versions: VersionRecord[]): void {
  for (const v of versions) v.validateReceipt();
}

export type VersionBundleScope = "CompleteTransaction" | "ViewScoped";
const SCOPES: VersionBundleScope[] = ["CompleteTransaction", "ViewScoped"];
const writeScope = (w: Writer, s: VersionBundleScope) => w.variant(SCOPES.indexOf(s));
const readScope = (r: Reader) => SCOPES[r.variant(2, "VersionBundleScope")];

export interface VersionBundle {
  tx: Transaction;
  versions: VersionRecord[];
  scope: VersionBundleScope;
  fate: Fate;
  globalTime?: GlobalTime;
  durability: DurabilityTier;
}
const writeVersionBundle = (w: Writer, b: VersionBundle) => {
  writeTransaction(w, b.tx);
  w.seq(b.versions, writeVersionRecord);
  writeScope(w, b.scope);
  writeFate(w, b.fate);
  w.option(b.globalTime, (w, t) => w.varint64(t));
  writeDurabilityTier(w, b.durability);
};
const readVersionBundle = (r: Reader): VersionBundle => ({
  tx: readTransaction(r),
  versions: r.seq(readVersionRecord),
  scope: readScope(r),
  fate: readFate(r),
  globalTime: r.option((r) => r.varint64()),
  durability: readDurabilityTier(r),
});

export type VersionCarrier =
  | { kind: "Bundle"; bundle: VersionBundle }
  | { kind: "Run"; run: VersionBundleRun };

export const carrierExpand = (c: VersionCarrier): VersionBundle[] =>
  c.kind === "Bundle" ? [c.bundle] : c.run.expand();
/** JS bundles are shared references, so bundle_refs and expand coincide. */
export const carrierBundleRefs = carrierExpand;

export function writeVersionCarrier(w: Writer, c: VersionCarrier): void {
  if (c.kind === "Bundle") writeVersionBundle(w.variant(0), c.bundle);
  else writeVersionBundleRun(w.variant(1), c.run);
}
export function readVersionCarrier(r: Reader): VersionCarrier {
  return r.variant(2, "VersionCarrier") === 0
    ? { kind: "Bundle", bundle: readVersionBundle(r) }
    : { kind: "Run", run: readVersionBundleRun(r) };
}

export interface VersionBundleRunHeader {
  table?: string;
  tx: Transaction;
  scope: VersionBundleScope;
  bodyCount: number;
  fate: Fate;
  globalTime?: GlobalTime;
  durability: DurabilityTier;
}
export interface VersionBundleRunOverride {
  bodyIndex: number;
  tx?: Transaction;
  scope?: VersionBundleScope;
  fate?: Fate;
  /** `null` encodes an explicit `Some(None)` override. */
  globalTime?: GlobalTime | null;
  durability?: DurabilityTier;
}
const hasOverrides = (o: VersionBundleRunOverride) =>
  o.tx !== undefined ||
  o.scope !== undefined ||
  o.fate !== undefined ||
  o.globalTime !== undefined ||
  o.durability !== undefined;

export class VersionBundleRun {
  constructor(
    readonly header: VersionBundleRunHeader,
    readonly bodies: { versions: VersionRecord[] }[],
    readonly overrides: VersionBundleRunOverride[],
  ) {}

  static fromAdjacentSingletons(bundles: VersionBundle[]): VersionBundleRun {
    const first = bundles[0];
    if (!first) throw new VersionBundleRunError({ kind: "EmptyRun" });
    const overrides: VersionBundleRunOverride[] = [];
    bundles.forEach((b, index) => {
      const o: VersionBundleRunOverride = {
        bodyIndex: index,
        tx: transactionEquals(b.tx, first.tx) ? undefined : b.tx,
        scope: b.scope !== first.scope ? b.scope : undefined,
        fate: fateEquals(b.fate, first.fate) ? undefined : b.fate,
        globalTime: b.globalTime !== first.globalTime ? (b.globalTime ?? null) : undefined,
        durability: b.durability !== first.durability ? b.durability : undefined,
      };
      if (hasOverrides(o)) overrides.push(o);
    });
    const header: VersionBundleRunHeader = {
      table: commonRunTable(bundles),
      tx: first.tx,
      scope: first.scope,
      bodyCount: bundles.length,
      fate: first.fate,
      globalTime: first.globalTime,
      durability: first.durability,
    };
    const run = new VersionBundleRun(
      header,
      bundles.map((b) => ({ versions: b.versions })),
      overrides,
    );
    run.validate();
    return run;
  }

  validate(): void {
    const declared = this.header.bodyCount;
    const actual = this.bodies.length;
    if (declared === 0) throw new VersionBundleRunError({ kind: "EmptyRun" });
    if (declared !== actual)
      throw new VersionBundleRunError({ kind: "BodyCountMismatch", declared, actual });
    const seen = new Set<number>();
    for (const o of this.overrides) {
      const index = o.bodyIndex;
      if (index >= declared)
        throw new VersionBundleRunError({
          kind: "OverrideIndexOutOfRange",
          index,
          bodyCount: declared,
        });
      if (seen.has(index)) throw new VersionBundleRunError({ kind: "DuplicateOverride", index });
      seen.add(index);
    }
    const table = this.header.table;
    if (table !== undefined)
      for (const body of this.bodies)
        for (const v of body.versions)
          if (v.table !== table)
            throw new VersionBundleRunError({
              kind: "TableMismatch",
              expected: table,
              actual: v.table,
            });
  }

  expand(): VersionBundle[] {
    this.validate();
    const overrides = new Map(this.overrides.map((o) => [o.bodyIndex, o]));
    const h = this.header;
    return this.bodies.map((body, index) => {
      const o = overrides.get(index);
      return {
        tx: o?.tx ?? h.tx,
        versions: body.versions,
        scope: o?.scope ?? h.scope,
        fate: o?.fate ?? h.fate,
        globalTime: o?.globalTime === undefined ? h.globalTime : (o.globalTime ?? undefined),
        durability: o?.durability ?? h.durability,
      };
    });
  }
}

function writeVersionBundleRun(w: Writer, run: VersionBundleRun): void {
  const h = run.header;
  w.option(h.table, (w, t) => w.str(t));
  writeTransaction(w, h.tx);
  writeScope(w, h.scope);
  w.varint(h.bodyCount);
  writeFate(w, h.fate);
  w.option(h.globalTime, (w, t) => w.varint64(t));
  writeDurabilityTier(w, h.durability);
  w.seq(run.bodies, (w, b) => w.seq(b.versions, writeVersionRecord));
  w.seq(run.overrides, (w, o) => {
    w.varint(o.bodyIndex);
    w.option(o.tx, writeTransaction);
    w.option(o.scope, writeScope);
    w.option(o.fate, writeFate);
    if (o.globalTime === undefined) w.u8(0);
    else w.u8(1).option(o.globalTime, (w, t) => w.varint64(t));
    w.option(o.durability, writeDurabilityTier);
  });
}

function readVersionBundleRun(r: Reader): VersionBundleRun {
  const header: VersionBundleRunHeader = {
    table: r.option((r) => r.str()),
    tx: readTransaction(r),
    scope: readScope(r),
    bodyCount: r.varint(),
    fate: readFate(r),
    globalTime: r.option((r) => r.varint64()),
    durability: readDurabilityTier(r),
  };
  const bodies = r.seq((r) => ({ versions: r.seq(readVersionRecord) }));
  const overrides = r.seq(
    (r): VersionBundleRunOverride => ({
      bodyIndex: r.varint(),
      tx: r.option(readTransaction),
      scope: r.option(readScope),
      fate: r.option(readFate),
      globalTime: r.option((r) => r.option((r) => r.varint64()) ?? null),
      durability: r.option(readDurabilityTier),
    }),
  );
  return new VersionBundleRun(header, bodies, overrides);
}

export type VersionBundleRunErrorDetail =
  | { kind: "MalformedVersionRecord"; table: string }
  | { kind: "EmptyRun" }
  | { kind: "BodyCountMismatch"; declared: number; actual: number }
  | { kind: "OverrideIndexOutOfRange"; index: number; bodyCount: number }
  | { kind: "DuplicateOverride"; index: number }
  | { kind: "TableMismatch"; expected: string; actual: string };

export class VersionBundleRunError extends Error {
  constructor(readonly detail: VersionBundleRunErrorDetail) {
    super(versionBundleRunErrorMessage(detail));
  }
}
function versionBundleRunErrorMessage(d: VersionBundleRunErrorDetail): string {
  switch (d.kind) {
    case "MalformedVersionRecord":
      return `version for ${d.table} has malformed record bytes`;
    case "EmptyRun":
      return "version-bundle run has no bodies";
    case "BodyCountMismatch":
      return `version-bundle run body_count ${d.declared} did not match ${d.actual} bodies`;
    case "OverrideIndexOutOfRange":
      return `version-bundle run override index ${d.index} out of range for ${d.bodyCount} bodies`;
    case "DuplicateOverride":
      return `version-bundle run has duplicate override for body ${d.index}`;
    case "TableMismatch":
      return `version-bundle run table context ${d.expected} did not match body table ${d.actual}`;
  }
}

export function buildVersionBundleRunsFromSingletons(bundles: VersionBundle[]): VersionBundleRun[] {
  return bundles.length ? [VersionBundleRun.fromAdjacentSingletons(bundles)] : [];
}

export function buildVersionCarriersFromSingletons(bundles: VersionBundle[]): VersionCarrier[] {
  if (forceSingletonVersionCarriers() || bundles.length <= 1)
    return bundles.map((bundle) => ({ kind: "Bundle", bundle }));
  return [{ kind: "Run", run: VersionBundleRun.fromAdjacentSingletons(bundles) }];
}

export const expandVersionCarriers = (carriers: VersionCarrier[]): VersionBundle[] =>
  carriers.flatMap(carrierExpand);

function commonRunTable(bundles: VersionBundle[]): string | undefined {
  let table: string | undefined;
  for (const b of bundles)
    for (const v of b.versions) {
      if (table === undefined) table = v.table;
      else if (table !== v.table) return undefined;
    }
  return table;
}

export function validateVersionCarrierRuns(carriers: VersionCarrier[]): void {
  for (const c of carriers) if (c.kind === "Run") c.run.validate();
}
