// Canonical indirect representation for large logical scalar values.
import {
  type Bytes,
  compareBytes,
  concatBytes,
  equalBytes,
  fromUtf8,
  toHex,
} from "../support/bytes.js";
import { Blake3Hasher, randomBytes } from "../support/hash.js";
import { assert } from "../support/runtime.js";
import { StreamingJsonValidator } from "./large_values/json_syntax.js";
import {
  type ChunkProvider,
  type ChunkRequest,
  ChunkError,
  ChunkStorageError,
  type LocalChunkReader,
} from "./chunks.js";
import {
  type EvaluationInputs,
  evaluationBlocked,
  isEvaluationBlocked,
} from "../_extern/groove-records.js";
import {
  ByteBuf,
  EnumSchema,
  EnumValue,
  OwnedRecord,
  RecordDescriptor,
  RecordError,
  type Value,
  type ValueType,
  appendVariantRecord,
  decodeSingleFieldValue,
  encodeSingleFieldValue,
  encodeVariantRecord,
  enumCase,
  rawBytes,
  rawString,
  splitVariantRecord,
  storedScalar,
} from "./records/index.js";

const FORMAT_V1 = 1;
export const FORMAT_VERSION = FORMAT_V1;

export const INLINE_VALUE_MAX_BYTES = 64 * 1024;
export const V1_FASTCDC_LEAF_MIN_BYTES = 16 * 1024;
export const V1_FASTCDC_LEAF_TARGET_BYTES = 64 * 1024;
export const V1_FASTCDC_LEAF_MAX_BYTES = 256 * 1024;
export const V1_FASTCDC_LEAF_SHORT_MASK = 0x1ffffn;
export const V1_FASTCDC_LEAF_LONG_MASK = 0x7fffn;
export const LEAF_MIN_BYTES = V1_FASTCDC_LEAF_MIN_BYTES;
export const LEAF_TARGET_BYTES = V1_FASTCDC_LEAF_TARGET_BYTES;
export const LEAF_MAX_BYTES = V1_FASTCDC_LEAF_MAX_BYTES;
export const MAX_ENCODED_NODE_BYTES = LEAF_MAX_BYTES + 16 * 1024;
export const V1_GROUPING_BRANCH_MIN_CHILDREN = 4;
export const V1_GROUPING_BRANCH_TARGET_CHILDREN = 16;
export const V1_GROUPING_BRANCH_MAX_CHILDREN = 64;
export const V1_GROUPING_BRANCH_SHORT_MASK = 0x1fn;
export const V1_GROUPING_BRANCH_LONG_MASK = 0x7n;
export const BRANCH_MIN_CHILDREN = V1_GROUPING_BRANCH_MIN_CHILDREN;
export const BRANCH_TARGET_CHILDREN = V1_GROUPING_BRANCH_TARGET_CHILDREN;
export const BRANCH_MAX_CHILDREN = V1_GROUPING_BRANCH_MAX_CHILDREN;
export const MAX_TREE_DEPTH = 32;
export const MAX_LOGICAL_TRAVERSAL_STEPS = 128 * 1024;
export const MAX_PHYSICAL_TRAVERSAL_NODES = MAX_LOGICAL_TRAVERSAL_STEPS;
export const MAX_JSON_NESTING_DEPTH = 128;
export const MAX_EDIT_COUNT = 64;
export const MAX_EDIT_TAIL_BYTES = 256 * 1024;
export const LOCATOR_BYTES = 32;

// ---- Types ----

export type LargeValueKind = "Bytes" | "String" | "Json";
/** 32-byte content hash. */
export type ContentHash = Bytes;
/** Opaque random 256-bit retrieval capability. */
export type Locator = Bytes;
export const randomLocator = (): Locator => randomBytes(LOCATOR_BYTES);
export function locatorFromBytes(bytes: Bytes): Locator {
  if (bytes.length !== LOCATOR_BYTES)
    throw new Error(`chunk locator must be exactly ${LOCATOR_BYTES} bytes, got ${bytes.length}`);
  return bytes;
}

export const physicalStorageValueType = (kind: LargeValueKind): ValueType => storedScalar(kind);

export type NodeRef = { objectHash: ContentHash; locator: Locator };
export const nodeRefKey = (r: NodeRef) => toHex(r.objectHash) + toHex(r.locator);
const nodeRefEq = (a: NodeRef, b: NodeRef) =>
  equalBytes(a.objectHash, b.objectHash) && equalBytes(a.locator, b.locator);

export type NodeMetrics = { byteLength: number; utf16Length: number | null };
const metricsEq = (a: NodeMetrics, b: NodeMetrics) =>
  a.byteLength === b.byteLength && a.utf16Length === b.utf16Length;

export type BranchChild = { nodeRef: NodeRef; metrics: NodeMetrics; logicalHash: ContentHash };

export type ChunkNode =
  | { tag: "Leaf"; format: number; kind: LargeValueKind; bytes: Bytes }
  | { tag: "Branch"; format: number; kind: LargeValueKind; children: BranchChild[] };

export type ReplaceEdit = {
  offset: number;
  deleteLength: number;
  insertBytes: Bytes;
  utf16Offset: number;
  deleteUtf16Length: number;
  insertUtf16Length: number;
};

export type LargeValueRef = {
  kind: LargeValueKind;
  formatVersion: number;
  logicalHash: ContentHash;
  root: NodeRef;
  byteLength: number;
  utf16Length: number | null;
  editTail: ReplaceEdit[];
};

const editEq = (a: ReplaceEdit, b: ReplaceEdit) =>
  a.offset === b.offset &&
  a.deleteLength === b.deleteLength &&
  equalBytes(a.insertBytes, b.insertBytes) &&
  a.utf16Offset === b.utf16Offset &&
  a.deleteUtf16Length === b.deleteUtf16Length &&
  a.insertUtf16Length === b.insertUtf16Length;

export const largeValueRefEq = (a: LargeValueRef, b: LargeValueRef) =>
  a.kind === b.kind &&
  a.formatVersion === b.formatVersion &&
  equalBytes(a.logicalHash, b.logicalHash) &&
  nodeRefEq(a.root, b.root) &&
  a.byteLength === b.byteLength &&
  a.utf16Length === b.utf16Length &&
  a.editTail.length === b.editTail.length &&
  a.editTail.every((e, i) => editEq(e, b.editTail[i]));

const cloneRef = (v: LargeValueRef): LargeValueRef => ({ ...v, editTail: [...v.editTail] });

export type StagedChunk = { nodeRef: NodeRef; encoded: Bytes };
export type PreparedLargeValue = { valueRef: LargeValueRef; stagedChunks: StagedChunk[] };
/** 16-byte opaque staging root identity. */
export type StagedLargeValueId = Bytes;
export type StagedLargeValueAccounting = { encodedBytes: number; nodeCount: number };
export type StagedLargeValue = {
  id: StagedLargeValueId;
  valueRef: LargeValueRef;
  accounting: StagedLargeValueAccounting;
  createdAtMs: number;
};
export type PendingLargeValueUpload = {
  id: StagedLargeValueId;
  descriptor: LargeValueRef | null;
  receiptId: StagedLargeValueId | null;
  accounting: StagedLargeValueAccounting;
  createdAtMs: number;
  chunks: NodeRef[];
};
export type LargeValueUploadProgress =
  | { kind: "Missing"; missing: NodeRef[] }
  | { kind: "Staged"; staged: StagedLargeValue };
export type StreamingPrepareStats = {
  inputBytes: number;
  peakLeafBufferBytes: number;
  peakFrontierNodes: number;
  stagedChunkCount: number;
};
export type StreamingExecutionStats = {
  logicalBytesConsumed: number;
  windowsConsumed: number;
  cooperativeYields: number;
};
export type Range = { start: number; end: number };

// ---- Errors ----

export type LargeValueErrorKind =
  | "InvalidUtf8"
  | "InvalidJson"
  | "UnsupportedFormat"
  | "ObjectHashMismatch"
  | "MalformedNode"
  | "MetricOverflow"
  | "DescriptorMismatch"
  | "InvalidTree"
  | "TraversalWorkLimitExceeded"
  | "PhysicalTraversalNodeLimitExceeded"
  | "MalformedScalar"
  | "RequiresEvaluation";

const MESSAGES: Record<Exclude<LargeValueErrorKind, "UnsupportedFormat">, string> = {
  InvalidUtf8: "large string is not valid UTF-8",
  InvalidJson: "large JSON is not valid JSON",
  ObjectHashMismatch: "chunk object hash does not match its authenticated reference",
  MalformedNode: "malformed large-value node",
  MetricOverflow: "large-value metric overflow",
  DescriptorMismatch: "large-value descriptor metrics or logical hash are dishonest",
  InvalidTree: "large-value tree contains a cycle or exceeds its depth bound",
  TraversalWorkLimitExceeded: "large-value logical traversal exceeded its deterministic work limit",
  PhysicalTraversalNodeLimitExceeded:
    "large-value physical traversal exceeded its distinct-node limit",
  MalformedScalar: "malformed physical scalar encoding",
  RequiresEvaluation: "indirect scalar requires interruptible evaluation",
};

export class LargeValueError extends Error {
  constructor(
    readonly kind: LargeValueErrorKind,
    readonly version = 0,
  ) {
    super(
      kind === "UnsupportedFormat"
        ? `unsupported large-value format version ${version}`
        : MESSAGES[kind],
    );
  }
}
const err = (kind: LargeValueErrorKind, version?: number) => new LargeValueError(kind, version);
const fail = (kind: LargeValueErrorKind): never => {
  throw err(kind);
};
/** Rust `ReachabilityError`: a large-value or chunk error. */
export type ReachabilityError = LargeValueError | ChunkError;

const MAX_SAFE = Number.MAX_SAFE_INTEGER;
function add(a: number, b: number): number {
  const s = a + b;
  if (s > MAX_SAFE) throw err("MetricOverflow");
  return s;
}
function sub(a: number, b: number): number {
  if (b > a) throw err("MetricOverflow");
  return a - b;
}

// ---- UTF-8 helpers ----

/** Length of the valid UTF-8 prefix, and whether the failure is an incomplete trailing sequence. */
function utf8Scan(b: Bytes): { valid: number; incomplete: boolean } {
  let i = 0;
  while (i < b.length) {
    const c = b[i];
    if (c < 0x80) {
      i++;
      continue;
    }
    let width: number;
    let lo = 0x80;
    let hi = 0xbf;
    if (c >= 0xc2 && c <= 0xdf) width = 2;
    else if (c >= 0xe0 && c <= 0xef) {
      width = 3;
      if (c === 0xe0) lo = 0xa0;
      if (c === 0xed) hi = 0x9f;
    } else if (c >= 0xf0 && c <= 0xf4) {
      width = 4;
      if (c === 0xf0) lo = 0x90;
      if (c === 0xf4) hi = 0x8f;
    } else return { valid: i, incomplete: false };
    for (let k = 1; k < width; k++) {
      if (i + k >= b.length) return { valid: i, incomplete: true };
      const x = b[i + k];
      if (x < (k === 1 ? lo : 0x80) || x > (k === 1 ? hi : 0xbf))
        return { valid: i, incomplete: false };
    }
    i += width;
  }
  return { valid: i, incomplete: false };
}
const isUtf8 = (b: Bytes) => utf8Scan(b).valid === b.length;
function text(b: Bytes): string {
  try {
    return fromUtf8(b);
  } catch {
    throw err("InvalidUtf8");
  }
}
const utf16LengthOf = (b: Bytes): number => text(b).length;
const isCharBoundary = (b: Bytes, i: number) => i === 0 || i >= b.length || (b[i] & 0xc0) !== 0x80;

// ---- Format dispatch ----

type LargeValueFormat = "V1";
function formatFromVersion(version: number): LargeValueFormat {
  if (version === FORMAT_V1) return "V1";
  throw err("UnsupportedFormat", version);
}
function formatDecodeNode(_format: LargeValueFormat, encoded: Bytes): ChunkNode {
  const node = decodeCanonicalNodeV1(encoded);
  if (node.format !== FORMAT_V1) throw err("UnsupportedFormat", node.format);
  return node;
}

// ---- Streaming checksum and cursor ----

export class LargeValueCursor {
  offset = 0;
  constructor(
    readonly value: LargeValueRef,
    readonly windowBytes: number,
  ) {
    validateDescriptor(value);
    if (windowBytes === 0) throw err("MalformedScalar");
  }
  remainingBytes(): number {
    return this.value.byteLength - this.offset;
  }
  nextRange(): Range | null {
    if (this.offset === this.value.byteLength) return null;
    return {
      start: this.offset,
      end: Math.min(this.offset + this.windowBytes, this.value.byteLength),
    };
  }
  advanceTo(offset: number): void {
    this.offset = offset;
  }
}

export class StreamingChecksum {
  readonly cursor: LargeValueCursor;
  private readonly hasher = new Blake3Hasher();
  private bytesThisTurn = 0;
  readonly stats: StreamingExecutionStats = {
    logicalBytesConsumed: 0,
    windowsConsumed: 0,
    cooperativeYields: 0,
  };
  constructor(
    value: LargeValueRef,
    windowBytes: number,
    private readonly maxBytesPerTurn: number,
  ) {
    if (maxBytesPerTurn === 0) throw err("MalformedScalar");
    this.cursor = new LargeValueCursor(value, Math.min(windowBytes, maxBytesPerTurn));
  }
  /** Commit one window; true when the caller must cooperatively yield. */
  consumeWindow(bytes: Bytes): boolean {
    const range = this.cursor.nextRange();
    if (!range || bytes.length !== range.end - range.start) throw err("MalformedScalar");
    this.hasher.update(bytes);
    this.cursor.advanceTo(range.end);
    this.stats.logicalBytesConsumed = add(this.stats.logicalBytesConsumed, bytes.length);
    this.stats.windowsConsumed = add(this.stats.windowsConsumed, 1);
    this.bytesThisTurn = add(this.bytesThisTurn, bytes.length);
    return this.bytesThisTurn >= this.maxBytesPerTurn && this.cursor.remainingBytes() !== 0;
  }
  recordYield(): void {
    this.stats.cooperativeYields = add(this.stats.cooperativeYields, 1);
    this.bytesThisTurn = 0;
  }
  finish(): [ContentHash, StreamingExecutionStats] {
    if (this.cursor.remainingBytes() !== 0) throw err("RequiresEvaluation");
    return [this.hasher.finalize(), this.stats];
  }
}

/** Incremental validation of consecutive final-logical windows of an already-chunked value. */
export class LogicalValueValidator {
  private readonly kind: LargeValueKind;
  private utf8Tail: Bytes = new Uint8Array(0);
  private utf16Length = 0;
  private readonly json: StreamingJsonValidator | null;
  constructor(value: LargeValueRef) {
    validateDescriptor(value);
    this.kind = value.kind;
    this.json = value.kind === "Json" ? new StreamingJsonValidator() : null;
  }
  push(bytes: Bytes): void {
    if (this.kind === "Bytes") return;
    const buf = concatBytes(this.utf8Tail, bytes);
    const { valid, incomplete } = utf8Scan(buf);
    if (valid < buf.length && !incomplete) throw err("InvalidUtf8");
    const prefix = buf.subarray(0, valid);
    this.utf16Length = add(this.utf16Length, utf16LengthOf(prefix));
    if (this.json && !this.json.push(prefix)) throw err("InvalidJson");
    this.utf8Tail = buf.slice(valid);
  }
  finish(value: LargeValueRef): void {
    if (this.utf8Tail.length) throw err("InvalidUtf8");
    if (this.json && !this.json.finish()) throw err("InvalidJson");
    if (value.kind !== "Bytes" && value.utf16Length !== this.utf16Length)
      throw err("DescriptorMismatch");
  }
}

/** Validate every node of an upload batch before any reaches a durable backend. */
export function validateStagedChunkBatch(kind: LargeValueKind, chunks: StagedChunk[]): void {
  const locatorHashes = new Map<string, ContentHash>();
  for (const chunk of chunks) {
    const key = toHex(chunk.nodeRef.locator);
    const existing = locatorHashes.get(key);
    locatorHashes.set(key, chunk.nodeRef.objectHash);
    if (existing && !equalBytes(existing, chunk.nodeRef.objectHash))
      throw err("ObjectHashMismatch");
    decodeNode(kind, chunk.nodeRef.objectHash, chunk.encoded);
  }
}

// ---- Streaming construction ----

export function prepareStreaming(
  kind: LargeValueKind,
  reader: Iterable<Bytes>,
  stage: (chunk: StagedChunk) => void,
): [LargeValueRef, StreamingPrepareStats] {
  const builder = new PushStreamingPreparation(kind, stage);
  for (const bytes of reader) if (bytes.length) builder.push(bytes);
  return builder.finish();
}

/** Resumable, bounded-memory construction of a canonical large-value tree. */
export class PushStreamingPreparation {
  private readonly builder: StreamingTreeBuilder;
  private readonly json: StreamingJsonValidator | null;
  constructor(
    kind: LargeValueKind,
    stage: (chunk: StagedChunk) => void,
    locatorFor: (hash: ContentHash) => Locator = randomLocator,
  ) {
    this.builder = new StreamingTreeBuilder(kind, locatorFor, stage);
    this.json = kind === "Json" ? new StreamingJsonValidator() : null;
  }
  push(bytes: Bytes): void {
    if (this.json && !this.json.push(bytes)) throw err("InvalidJson");
    this.builder.feed(bytes);
  }
  finish(): [LargeValueRef, StreamingPrepareStats] {
    if (this.json && !this.json.finish()) throw err("InvalidJson");
    return this.builder.finish();
  }
}

type StreamingLevel = { pending: BuiltNode[]; hash: bigint; total: number };

class StreamingTreeBuilder {
  private leaf: Bytes = new Uint8Array(0);
  private leafScan = 0;
  private leafHash = 0n;
  private levels: StreamingLevel[] = [];
  private stats: StreamingPrepareStats = {
    inputBytes: 0,
    peakLeafBufferBytes: 0,
    peakFrontierNodes: 0,
    stagedChunkCount: 0,
  };
  constructor(
    private readonly kind: LargeValueKind,
    private readonly locatorFor: (hash: ContentHash) => Locator,
    private readonly stage: (chunk: StagedChunk) => void,
  ) {}

  feed(bytes: Bytes): void {
    this.stats.inputBytes = add(this.stats.inputBytes, bytes.length);
    this.leaf = concatBytes(this.leaf, bytes);
    this.stats.peakLeafBufferBytes = Math.max(this.stats.peakLeafBufferBytes, this.leaf.length);
    for (;;) {
      let boundary: number | null = null;
      const limit = Math.min(this.leaf.length, LEAF_MAX_BYTES);
      while (this.leafScan < limit) {
        this.leafHash = roll(this.leafHash, this.leaf[this.leafScan]);
        this.leafScan += 1;
        if (leafCut(this.leafHash, this.leafScan) || this.leafScan === LEAF_MAX_BYTES) {
          boundary = this.leafScan;
          break;
        }
      }
      if (boundary === null) break;
      let end = boundary;
      if (this.kind !== "Bytes") {
        const { valid, incomplete } = utf8Scan(this.leaf.subarray(0, end));
        if (valid < end) {
          if (!incomplete) throw err("InvalidUtf8");
          end = valid;
        }
        if (end === 0) break;
      }
      const leaf = this.leaf.slice(0, end);
      this.leaf = this.leaf.slice(end);
      this.leafScan = 0;
      this.leafHash = 0n;
      this.emitLeaf(leaf);
    }
  }

  private emitLeaf(bytes: Bytes): void {
    const m = metrics(this.kind, bytes);
    this.addNode(0, this.stageNodeTo(leafNode(this.kind, bytes), m));
  }

  private stageNodeTo(node: ChunkNode, m: NodeMetrics): BuiltNode {
    if (node.kind !== this.kind || !metricsEq(nodeMetrics(this.kind, node), m))
      throw err("DescriptorMismatch");
    const [built, chunk] = sealNode(node, m, (hash) => [this.locatorFor(hash), true]);
    if (chunk) {
      this.stage(chunk);
      this.stats.stagedChunkCount = add(this.stats.stagedChunkCount, 1);
    }
    return built;
  }

  private addNode(level: number, node: BuiltNode): void {
    for (;;) {
      if (level >= MAX_TREE_DEPTH) throw err("MalformedNode");
      while (this.levels.length <= level) this.levels.push({ pending: [], hash: 0n, total: 0 });
      const state = this.levels[level];
      state.hash = rollChild(state.hash, this.kind, node);
      state.pending.push(node);
      state.total += 1;
      const count = state.pending.length;
      const boundary = branchCut(state.hash, count);
      this.stats.peakFrontierNodes = Math.max(
        this.stats.peakFrontierNodes,
        this.levels.reduce((n, l) => n + l.pending.length, 0),
      );
      if (!boundary && count < BRANCH_MAX_CHILDREN) return;
      node = this.flushGroup(level);
      level += 1;
    }
  }

  private flushGroup(level: number): BuiltNode {
    const group = this.levels[level].pending;
    this.levels[level].pending = [];
    this.levels[level].hash = 0n;
    const [chunkNode, m] = branchOver(this.kind, group);
    return this.stageNodeTo(chunkNode, m);
  }

  finish(): [LargeValueRef, StreamingPrepareStats] {
    if (this.leaf.length || !this.levels.length) {
      const leaf = this.leaf;
      this.leaf = new Uint8Array(0);
      this.emitLeaf(leaf);
    }
    for (let level = 0; ; level++) {
      if (level >= this.levels.length) throw err("MalformedNode");
      if (this.levels[level].total === 1) {
        const root = this.levels[level].pending.pop() ?? fail("MalformedNode");
        return [intoValueRef(root, this.kind), this.stats];
      }
      if (this.levels[level].pending.length) this.addNode(level + 1, this.flushGroup(level));
    }
  }
}

/** Replays a tail into successive localized splices; completed splices survive suspension. */
export class ConsolidationContinuation {
  private current: LargeValueRef | null = null;
  private nextEdit = 0;
  private stagedChunks: StagedChunk[] = [];
  constructor(private readonly source: LargeValueRef) {
    validateDescriptorShape(source);
  }

  step(inputs: EvaluationInputs): PreparedLargeValue | null {
    const source = this.source;
    if (!this.current) {
      const node = loadAuthenticatedNodeAttempt(
        source.formatVersion,
        source.kind,
        source.root,
        source.logicalHash,
        inputs,
      );
      const m = nodeMetrics(source.kind, node);
      this.current = {
        ...source,
        byteLength: m.byteLength,
        utf16Length: m.utf16Length,
        editTail: [],
      };
    }
    while (this.nextEdit < source.editTail.length) {
      const edit = source.editTail[this.nextEdit];
      const outcome = replaceTailWithBoundsAttempt(
        this.current!,
        edit.offset,
        edit.deleteLength,
        edit.insertBytes,
        inputs,
        false,
      );
      if (outcome.kind !== "Updated") throw err("MalformedScalar");
      const prepared = consolidateSingleEditAttempt(outcome.value, inputs, randomLocator);
      for (const chunk of prepared.stagedChunks)
        inputs.installChunk(
          {
            objectHash: chunk.nodeRef.objectHash,
            locator: chunk.nodeRef.locator,
          } satisfies ChunkRequest,
          chunk.encoded.slice(),
        );
      this.stagedChunks.push(...prepared.stagedChunks);
      this.current = prepared.valueRef;
      this.nextEdit += 1;
    }
    const current = this.current ?? fail("MalformedScalar");
    if (current.byteLength !== source.byteLength || current.utf16Length !== source.utf16Length)
      throw err("DescriptorMismatch");
    const stagedChunks = this.stagedChunks;
    this.stagedChunks = [];
    return { valueRef: current, stagedChunks };
  }
}

// ---- Tail edits ----

export type TailOutcome = { kind: "Updated" | "ConsolidationRequired"; value: LargeValueRef };
export type TailAppendOutcome = TailOutcome;
export type TailEditOutcome = TailOutcome;

function pushTailEdit(
  value: LargeValueRef,
  updated: LargeValueRef,
  edit: ReplaceEdit,
  enforceTailBounds: boolean,
): TailOutcome {
  let nextTailBytes = 0;
  for (const e of [...value.editTail, edit])
    nextTailBytes = add(add(nextTailBytes, e.insertBytes.length), 24);
  const overBounds = value.editTail.length >= MAX_EDIT_COUNT || nextTailBytes > MAX_EDIT_TAIL_BYTES;
  updated.editTail.push(edit);
  return {
    kind: enforceTailBounds && overBounds ? "ConsolidationRequired" : "Updated",
    value: updated,
  };
}

/** Physical arm stored inside a logical bytes/string/JSON cell. */
export type StoredScalar =
  | { kind: "Primitive"; bytes: Bytes }
  | { kind: "Chunked"; value: LargeValueRef };

/** Append bytes without reading the immutable base tree. */
export function appendTail(value: LargeValueRef, insertBytes: Bytes): TailAppendOutcome {
  validateDescriptor(value);
  if (value.kind === "String" && !isUtf8(insertBytes)) throw err("InvalidUtf8");
  if (value.kind === "Json") throw err("InvalidJson");
  const updated = cloneRef(value);
  updated.byteLength = add(updated.byteLength, insertBytes.length);
  if (updated.utf16Length !== null)
    updated.utf16Length = add(updated.utf16Length, utf16LengthOf(insertBytes));
  const edit: ReplaceEdit = {
    offset: value.byteLength,
    deleteLength: 0,
    insertBytes,
    utf16Offset: value.utf16Length ?? 0,
    deleteUtf16Length: 0,
    insertUtf16Length:
      updated.utf16Length !== null &&
      value.utf16Length !== null &&
      updated.utf16Length >= value.utf16Length
        ? updated.utf16Length - value.utf16Length
        : 0,
  };
  return pushTailEdit(value, updated, edit, true);
}

/** Add one canonical replacement to the bounded tail; text edits read the deleted range. */
export function replaceTailAttempt(
  value: LargeValueRef,
  offset: number,
  deleteLength: number,
  insertBytes: Bytes,
  inputs: EvaluationInputs,
): TailEditOutcome {
  return replaceTailWithBoundsAttempt(value, offset, deleteLength, insertBytes, inputs, true);
}

function replaceTailWithBoundsAttempt(
  value: LargeValueRef,
  offset: number,
  deleteLength: number,
  insertBytes: Bytes,
  inputs: EvaluationInputs,
  enforceTailBounds: boolean,
): TailEditOutcome {
  validateDescriptor(value);
  const end = add(offset, deleteLength);
  if (end > value.byteLength) throw err("MalformedScalar");
  const nextLength = add(sub(value.byteLength, deleteLength), insertBytes.length);
  let nextUtf16: number | null = null;
  let utf16Offset = 0;
  let deleteUtf16Length = 0;
  let insertUtf16Length = 0;
  switch (value.kind) {
    case "Bytes":
      break;
    case "String": {
      const inserted = utf16LengthOf(insertBytes);
      for (const boundary of [offset, end]) {
        if (boundary < value.byteLength) {
          const byte = byteRangeAttempt(value, { start: boundary, end: boundary + 1 }, inputs);
          if (byte.length && (byte[0] & 0xc0) === 0x80) throw err("InvalidUtf8");
        }
      }
      const deleted = byteRangeAttempt(value, { start: offset, end }, inputs);
      utf16Offset = utf16LengthForByteRangeAttempt(value, { start: 0, end: offset }, inputs);
      deleteUtf16Length = utf16LengthOf(deleted);
      insertUtf16Length = inserted;
      const current = value.utf16Length ?? fail("MalformedScalar");
      nextUtf16 = add(sub(current, deleteUtf16Length), insertUtf16Length);
      break;
    }
    case "Json":
      if (offset !== 0 || deleteLength !== value.byteLength) throw err("InvalidJson");
      validateLogical("Json", insertBytes);
      insertUtf16Length = utf16LengthOf(insertBytes);
      nextUtf16 = insertUtf16Length;
      deleteUtf16Length = value.utf16Length ?? fail("MalformedScalar");
      break;
  }
  const updated = cloneRef(value);
  updated.byteLength = nextLength;
  updated.utf16Length = nextUtf16;
  const edit = {
    offset,
    deleteLength,
    insertBytes,
    utf16Offset,
    deleteUtf16Length,
    insertUtf16Length,
  };
  return pushTailEdit(value, updated, edit, enforceTailBounds);
}

/** Consolidate an append-only tail by rebuilding only the old tree's right spine. */
export function consolidateAppendsAttempt(
  value: LargeValueRef,
  inputs: EvaluationInputs,
  freshLocator: (hash: ContentHash) => Locator,
): PreparedLargeValue {
  validateDescriptorShape(value);
  let cursor = baseLength(value.byteLength, value.editTail, "Bytes");
  const appended: Bytes[] = [];
  for (const edit of value.editTail) {
    if (edit.deleteLength !== 0 || edit.offset !== cursor) throw err("MalformedScalar");
    cursor = add(cursor, edit.insertBytes.length);
    appended.push(edit.insertBytes);
  }
  if (cursor !== value.byteLength) throw err("DescriptorMismatch");

  const lastLeaf = descendAttempt(
    value,
    value.root,
    value.logicalHash,
    [],
    MAX_TREE_DEPTH,
    inputs,
    (children) => (children.length ? children.length - 1 : fail("MalformedNode")),
  );
  const existing = new Map<string, Locator>();
  for (const frame of lastLeaf.path)
    existing.set(toHex(frame.nodeRef.objectHash), frame.nodeRef.locator);
  existing.set(toHex(lastLeaf.nodeRef.objectHash), lastLeaf.nodeRef.locator);
  const depth = lastLeaf.path.length;

  const stager = reusingStager(value.kind, existing, freshLocator);
  let replacement = stager.leaves(concatBytes(lastLeaf.bytes, ...appended));
  for (const frame of [...lastLeaf.path].reverse()) {
    const rebuilt = frame.children.slice(0, -1).map(fromChild);
    replacement = stager.branches([...rebuilt, ...replacement]);
  }
  return finishRebuiltTree(value, replacement, depth, stager);
}

/** Consolidate one replacement by rebuilding the zipper between resynchronized leaves. */
export function consolidateSingleEditAttempt(
  value: LargeValueRef,
  inputs: EvaluationInputs,
  freshLocator: (hash: ContentHash) => Locator,
): PreparedLargeValue {
  validateDescriptorShape(value);
  if (value.editTail.length !== 1) throw err("MalformedScalar");
  const edit = value.editTail[0];
  const baseLen = baseLength(value.byteLength, value.editTail, "Bytes");
  const editEnd = add(edit.offset, edit.deleteLength);
  if (editEnd > baseLen) throw err("MalformedScalar");
  if (edit.offset === baseLen) return consolidateAppendsAttempt(value, inputs, freshLocator);

  const leaves = [locateLeafAttempt(value, edit.offset, baseLen, inputs)];
  let segment: Bytes;
  for (;;) {
    const last = leaves[leaves.length - 1];
    const coveredEnd = add(last.start, last.bytes.length);
    if (coveredEnd >= editEnd) {
      const joined = concatBytes(...leaves.map((l) => l.bytes));
      const localStart = edit.offset - leaves[0].start;
      const localEnd = editEnd - leaves[0].start;
      const candidate = concatBytes(
        joined.subarray(0, localStart),
        edit.insertBytes,
        joined.subarray(localEnd),
      );
      const ranges = leafRanges(value.kind, candidate);
      const lastRange = ranges[ranges.length - 1];
      const resynchronized =
        lastRange !== undefined &&
        lastRange.start > 0 &&
        equalBytes(candidate.subarray(lastRange.start, lastRange.end), last.bytes);
      if (resynchronized || coveredEnd === baseLen) {
        segment = candidate;
        break;
      }
    }
    leaves.push(nextLeafAttempt(value, last, inputs));
  }

  const leftPath = leaves[0].path;
  const rightPath = leaves[leaves.length - 1].path;
  if (leftPath.length !== rightPath.length) throw err("InvalidTree");
  const existing = new Map<string, Locator>();
  for (const leaf of leaves) {
    existing.set(toHex(leaf.nodeRef.objectHash), leaf.nodeRef.locator);
    for (const frame of leaf.path)
      existing.set(toHex(frame.nodeRef.objectHash), frame.nodeRef.locator);
  }

  if (value.byteLength === 0) {
    if (segment.length) throw err("DescriptorMismatch");
    return prepareWithLocator(value.kind, segment, freshLocator);
  }

  const stager = reusingStager(value.kind, existing, freshLocator);
  let replacement = segment.length ? stager.leaves(segment) : [];
  for (let depth = leftPath.length - 1; depth >= 0; depth--) {
    const left = leftPath[depth];
    const right = rightPath[depth];
    const level = [
      ...left.children.slice(0, left.selected).map(fromChild),
      ...replacement,
      ...right.children.slice(right.selected + 1).map(fromChild),
    ];
    let higherPathHasSiblings = false;
    for (let i = 0; i < depth; i++)
      if (leftPath[i].selected > 0 || rightPath[i].selected + 1 < rightPath[i].children.length)
        higherPathHasSiblings = true;
    replacement = level.length === 1 && !higherPathHasSiblings ? level : stager.branches(level);
  }
  return finishRebuiltTree(value, replacement, leftPath.length, stager);
}

function finishRebuiltTree(
  value: LargeValueRef,
  level: BuiltNode[],
  depth: number,
  stager: TreeStager,
): PreparedLargeValue {
  const root = stager.root(level, depth, "InvalidTree");
  if (
    root.metrics.byteLength !== value.byteLength ||
    root.metrics.utf16Length !== value.utf16Length
  )
    throw err("DescriptorMismatch");
  return { valueRef: intoValueRef(root, value.kind), stagedChunks: stager.staged };
}

// ---- Physical traversal ----

type PhysicalTraversalEntry = { nodeRef: NodeRef; ancestors: string[] };
type PhysicalNodeState = {
  expectedLogicalHash: ContentHash;
  expectedMetrics: NodeMetrics | null;
  actualMetrics: NodeMetrics | null;
};

/** Visits each immutable node once while checking shared references and cycles. */
class PhysicalTraversal {
  private readonly rootKey: string;
  private readonly pending: PhysicalTraversalEntry[];
  private readonly nodes = new Map<string, PhysicalNodeState>();
  private readonly edges = new Map<string, string[]>();
  private readonly nodeBudget: PhysicalTraversalNodeBudget;

  static forValue(value: LargeValueRef): PhysicalTraversal {
    const rootMetrics = value.editTail.length
      ? null
      : { byteLength: value.byteLength, utf16Length: value.utf16Length };
    return new PhysicalTraversal(value.root, rootMetrics, value.logicalHash);
  }

  constructor(
    root: NodeRef,
    expectedMetrics: NodeMetrics | null,
    expectedLogicalHash: ContentHash,
    maxNodes = MAX_PHYSICAL_TRAVERSAL_NODES,
  ) {
    this.nodeBudget = new PhysicalTraversalNodeBudget(maxNodes);
    this.nodeBudget.consume();
    this.rootKey = nodeRefKey(root);
    this.pending = [{ nodeRef: root, ancestors: [] }];
    this.nodes.set(this.rootKey, { expectedLogicalHash, expectedMetrics, actualMetrics: null });
  }

  pop(): PhysicalTraversalEntry | undefined {
    return this.pending.pop();
  }

  validateNode(kind: LargeValueKind, nodeRef: NodeRef, node: ChunkNode): void {
    const key = nodeRefKey(nodeRef);
    const state = this.nodes.get(key) ?? fail("InvalidTree");
    if (!equalBytes(nodeLogicalHash(node), state.expectedLogicalHash))
      throw err("DescriptorMismatch");
    const actual = nodeMetrics(kind, node);
    if (state.expectedMetrics && !metricsEq(state.expectedMetrics, actual))
      throw err("DescriptorMismatch");
    state.actualMetrics = actual;
    this.edges.set(
      key,
      node.tag === "Leaf"
        ? []
        : [...new Set(node.children.map((c) => nodeRefKey(c.nodeRef)))].sort(),
    );
  }

  edgeClaim(nodeRef: NodeRef): [ContentHash, NodeMetrics] {
    const state = this.nodes.get(nodeRefKey(nodeRef)) ?? fail("InvalidTree");
    return [state.expectedLogicalHash, state.expectedMetrics ?? fail("InvalidTree")];
  }

  sealReused(nodeRef: NodeRef): void {
    this.edges.set(nodeRefKey(nodeRef), []);
  }

  /** Longest root path to every node; call only after `finish` succeeded. */
  maxDepths(): Map<string, number> {
    const seen = new Set<string>();
    const order: string[] = [];
    const postOrder = (key: string) => {
      if (seen.has(key)) return;
      seen.add(key);
      for (const child of this.edges.get(key) ?? []) postOrder(child);
      order.push(key);
    };
    postOrder(this.rootKey);
    const depths = new Map([[this.rootKey, 0]]);
    for (const key of order.reverse()) {
      const depth = depths.get(key) ?? 0;
      for (const child of this.edges.get(key) ?? [])
        depths.set(child, Math.max(depths.get(child) ?? 0, depth + 1));
    }
    return depths;
  }

  /** Memoized height evaluation rejects cycles and over-deep paths in O(V + E). */
  finish(): void {
    const visiting = new Set<string>();
    const heights = new Map<string, number>();
    const height = (key: string): number => {
      const known = heights.get(key);
      if (known !== undefined) return known;
      if (visiting.has(key)) throw err("InvalidTree");
      visiting.add(key);
      const children = this.edges.get(key) ?? fail("InvalidTree");
      let result = 0;
      for (const child of children) {
        result = Math.max(result, height(child) + 1);
        if (result > MAX_TREE_DEPTH) throw err("InvalidTree");
      }
      visiting.delete(key);
      heights.set(key, result);
      return result;
    };
    height(this.rootKey);
  }

  private discoverChild(parent: PhysicalTraversalEntry, child: BranchChild): void {
    const ancestors = [...parent.ancestors, nodeRefKey(parent.nodeRef)];
    const childKey = nodeRefKey(child.nodeRef);
    if (ancestors.length > MAX_TREE_DEPTH || ancestors.includes(childKey)) throw err("InvalidTree");
    const state = this.nodes.get(childKey);
    if (state) {
      if (
        !equalBytes(state.expectedLogicalHash, child.logicalHash) ||
        (state.expectedMetrics && !metricsEq(state.expectedMetrics, child.metrics)) ||
        (state.actualMetrics && !metricsEq(state.actualMetrics, child.metrics))
      )
        throw err("DescriptorMismatch");
      state.expectedMetrics = child.metrics;
      return;
    }
    this.nodeBudget.consume();
    this.nodes.set(childKey, {
      expectedLogicalHash: child.logicalHash,
      expectedMetrics: child.metrics,
      actualMetrics: null,
    });
    this.pending.push({ nodeRef: child.nodeRef, ancestors });
  }

  admitEncoded(value: LargeValueRef, entry: PhysicalTraversalEntry, encoded: Bytes): void {
    const node = decodeNodeForFormat(
      value.formatVersion,
      value.kind,
      entry.nodeRef.objectHash,
      encoded,
    );
    this.validateNode(value.kind, entry.nodeRef, node);
    if (node.tag === "Branch") this.discoverChildren(entry, node.children);
  }

  discoverChildren(parent: PhysicalTraversalEntry, children: BranchChild[]): void {
    for (let i = children.length - 1; i >= 0; i--) this.discoverChild(parent, children[i]);
  }
}

export class PhysicalTraversalNodeBudget {
  constructor(private remaining = MAX_PHYSICAL_TRAVERSAL_NODES) {}
  consume(): void {
    this.consumeMany(1);
  }
  consumeMany(count: number): void {
    if (count > this.remaining) throw err("PhysicalTraversalNodeLimitExceeded");
    this.remaining -= count;
  }
}

// ---- Logical walks ----

type WalkSpan = { start: number; len: number; metrics: NodeMetrics | null };
const spanEnd = (s: WalkSpan) => add(s.start, s.len);

/** One kind of occurrence-counting read of a value's base tree. */
interface LogicalVisitor {
  wants?(span: WalkSpan): boolean;
  leaf(span: WalkSpan, bytes: Bytes): void;
  branch?(span: WalkSpan, children: BranchChild[]): void;
  childLen(metrics: NodeMetrics): number;
  readonly exactCover?: boolean;
}

/** Depth-first walk in logical order; blocked reads are reported after every other node was tried. */
function walkLogical(
  value: LargeValueRef,
  root: WalkSpan,
  visitor: LogicalVisitor,
  inputs: EvaluationInputs,
): void {
  const pending: [NodeRef, ContentHash, WalkSpan, number][] = [
    [value.root, value.logicalHash, root, 0],
  ];
  let remaining = MAX_LOGICAL_TRAVERSAL_STEPS;
  const charge = (count: number) => {
    if (count > remaining) throw err("TraversalWorkLimitExceeded");
    remaining -= count;
  };
  let blocked = false;
  let next;
  while ((next = pending.pop())) {
    const [nodeRef, expectedHash, span, depth] = next;
    charge(1);
    if (depth > MAX_TREE_DEPTH) throw err("InvalidTree");
    if (visitor.wants && !visitor.wants(span)) continue;
    let node: ChunkNode;
    try {
      node = loadAuthenticatedNodeAttempt(
        value.formatVersion,
        value.kind,
        nodeRef,
        expectedHash,
        inputs,
      );
    } catch (e) {
      if (!isEvaluationBlocked(e)) throw e;
      blocked = true;
      continue;
    }
    if (node.tag === "Leaf") {
      visitor.leaf(span, node.bytes);
      continue;
    }
    visitor.branch?.(span, node.children);
    charge(node.children.length);
    const first = pending.length;
    let childStart = span.start;
    for (const child of node.children) {
      const childSpan = {
        start: childStart,
        len: visitor.childLen(child.metrics),
        metrics: child.metrics,
      };
      childStart = spanEnd(childSpan);
      pending.push([child.nodeRef, child.logicalHash, childSpan, depth + 1]);
    }
    if (visitor.exactCover && childStart !== spanEnd(span)) throw err("DescriptorMismatch");
    const tail = pending.splice(first).reverse();
    pending.push(...tail);
  }
  if (blocked) throw evaluationBlocked();
}

/** Authenticate and visit every immutable node reachable from one descriptor. */
export async function visitReachableChunks(
  value: LargeValueRef,
  provider: ChunkProvider,
  visit: (request: ChunkRequest) => void,
): Promise<number> {
  checkFormat(value.formatVersion);
  const traversal = PhysicalTraversal.forValue(value);
  let visited = 0;
  let entry;
  while ((entry = traversal.pop())) {
    const request: ChunkRequest = {
      objectHash: entry.nodeRef.objectHash,
      locator: entry.nodeRef.locator,
    };
    const encoded = await provider.get(request);
    const node = decodeNodeForFormat(
      value.formatVersion,
      value.kind,
      entry.nodeRef.objectHash,
      encoded,
    );
    traversal.validateNode(value.kind, entry.nodeRef, node);
    visit(request);
    visited = add(visited, 1);
    if (node.tag === "Branch") traversal.discoverChildren(entry, node.children);
  }
  traversal.finish();
  return visited;
}

/** Authenticate the local prefix of a descriptor's tree and return its missing frontier. */
export async function missingUploadFrontier(
  value: LargeValueRef,
  reader: LocalChunkReader,
  limit: number,
): Promise<NodeRef[]> {
  checkFormat(value.formatVersion);
  const traversal = PhysicalTraversal.forValue(value);
  const missing: NodeRef[] = [];
  let entry;
  while ((entry = traversal.pop())) {
    let encoded: Bytes;
    try {
      encoded = await reader.get(entry.nodeRef.locator, entry.nodeRef.objectHash);
    } catch (e) {
      if (e instanceof ChunkStorageError && e.kind === "Unavailable") {
        missing.push(entry.nodeRef);
        if (missing.length >= Math.max(limit, 1)) break;
        continue;
      }
      throw ChunkError.fromStorage(e);
    }
    traversal.admitEncoded(value, entry, encoded);
  }
  if (!missing.length) traversal.finish();
  return missing;
}

/** Verify an upload's complete local tree, proving every node belongs to this upload's journal. */
export async function validateFinalizedUpload(
  value: LargeValueRef,
  reader: LocalChunkReader,
  uploadedChunks: Set<string>,
  descriptorWasBoundBeforeCompletion: boolean,
): Promise<void> {
  validateDescriptor(value);
  const traversal = PhysicalTraversal.forValue(value);
  let entry;
  while ((entry = traversal.pop())) {
    if (!descriptorWasBoundBeforeCompletion && !uploadedChunks.has(nodeRefKey(entry.nodeRef)))
      throw err("DescriptorMismatch");
    traversal.admitEncoded(value, entry, await readForValidation(reader, entry.nodeRef));
  }
  traversal.finish();
}

type ReuseProof = "Proven" | "NotReused" | "Unprovable";
type EdgeClaim = [ContentHash, NodeMetrics];

/**
 * Validate a descriptor Groove derived from `base`: authenticate staged nodes, and prove each
 * reused node is an edge of the base tree. Returns false when reuse cannot be proven.
 */
export async function validateDerivedUpload(
  value: LargeValueRef,
  base: LargeValueRef,
  reader: LocalChunkReader,
  stagedChunks: Set<string>,
): Promise<boolean> {
  const tree = tailBaseDescriptor(value);
  const baseTree = tailBaseDescriptor(base);
  if (value.kind !== base.kind || value.formatVersion !== base.formatVersion)
    throw err("DescriptorMismatch");
  const treeMetrics = (t: LargeValueRef): NodeMetrics => ({
    byteLength: t.byteLength,
    utf16Length: t.utf16Length,
  });
  const baseRoot = decodeNodeForFormat(
    base.formatVersion,
    base.kind,
    base.root.objectHash,
    await readForValidation(reader, base.root),
  );
  if (
    !equalBytes(nodeLogicalHash(baseRoot), baseTree.logicalHash) ||
    !metricsEq(nodeMetrics(base.kind, baseRoot), treeMetrics(baseTree))
  )
    throw err("DescriptorMismatch");
  const traversal = new PhysicalTraversal(value.root, treeMetrics(tree), value.logicalHash);
  const claims = new Map<string, EdgeClaim>();
  let entry;
  while ((entry = traversal.pop())) {
    const key = nodeRefKey(entry.nodeRef);
    if (!stagedChunks.has(key)) {
      claims.set(key, traversal.edgeClaim(entry.nodeRef));
      traversal.sealReused(entry.nodeRef);
      continue;
    }
    traversal.admitEncoded(value, entry, await readForValidation(reader, entry.nodeRef));
  }
  traversal.finish();
  const depths = traversal.maxDepths();

  const pending: [NodeRef, number][] = [];
  switch (
    proveReusedEdge(claims, depths, base.root, [base.logicalHash, treeMetrics(baseTree)], 0)
  ) {
    case "NotReused":
      pending.push([base.root, 0]);
      break;
    case "Unprovable":
      return false;
  }
  const visited = new Set<string>();
  while (claims.size) {
    const next = pending.pop();
    if (!next) return false;
    const [nodeRef, depth] = next;
    const key = nodeRefKey(nodeRef);
    if (visited.has(key)) continue;
    visited.add(key);
    if (visited.size > MAX_PHYSICAL_TRAVERSAL_NODES) return false;
    const node = decodeNodeForFormat(
      base.formatVersion,
      base.kind,
      nodeRef.objectHash,
      await readForValidation(reader, nodeRef),
    );
    if (node.tag !== "Branch") continue;
    for (const child of [...node.children].reverse()) {
      switch (
        proveReusedEdge(
          claims,
          depths,
          child.nodeRef,
          [child.logicalHash, child.metrics],
          depth + 1,
        )
      ) {
        case "NotReused":
          pending.push([child.nodeRef, depth + 1]);
          break;
        case "Unprovable":
          return false;
      }
    }
  }
  return true;
}

function proveReusedEdge(
  claims: Map<string, EdgeClaim>,
  derivedDepths: Map<string, number>,
  nodeRef: NodeRef,
  baseClaim: EdgeClaim,
  baseDepth: number,
): ReuseProof {
  const key = nodeRefKey(nodeRef);
  const expected = claims.get(key);
  if (!expected) return "NotReused";
  if (!equalBytes(expected[0], baseClaim[0]) || !metricsEq(expected[1], baseClaim[1]))
    throw err("DescriptorMismatch");
  const depth = derivedDepths.get(key);
  if (depth === undefined || depth > baseDepth) return "Unprovable";
  claims.delete(key);
  return "Proven";
}

async function readForValidation(reader: LocalChunkReader, nodeRef: NodeRef): Promise<Bytes> {
  try {
    return await reader.get(nodeRef.locator, nodeRef.objectHash);
  } catch (e) {
    throw ChunkError.fromStorage(e);
  }
}

// ---- Tree building ----

type BuiltNode = { nodeRef: NodeRef; metrics: NodeMetrics; structuralHash: ContentHash };

const intoValueRef = (node: BuiltNode, kind: LargeValueKind): LargeValueRef => ({
  kind,
  formatVersion: FORMAT_VERSION,
  logicalHash: node.structuralHash,
  root: node.nodeRef,
  byteLength: node.metrics.byteLength,
  utf16Length: node.metrics.utf16Length,
  editTail: [],
});
const toChild = (n: BuiltNode): BranchChild => ({
  nodeRef: n.nodeRef,
  metrics: n.metrics,
  logicalHash: n.structuralHash,
});
const fromChild = (c: BranchChild): BuiltNode => ({
  nodeRef: c.nodeRef,
  metrics: c.metrics,
  structuralHash: c.logicalHash,
});

function sumMetrics(all: NodeMetrics[]): NodeMetrics {
  if (!all.length) throw err("MalformedNode");
  return all.slice(1).reduce(addMetrics, all[0]);
}

function branchOver(kind: LargeValueKind, group: BuiltNode[]): [ChunkNode, NodeMetrics] {
  const m = sumMetrics(group.map((c) => c.metrics));
  return [{ tag: "Branch", format: FORMAT_VERSION, kind, children: group.map(toChild) }, m];
}

/** Encode `node` and name it; the chunk is returned for staging only when its locator is fresh. */
function sealNode(
  node: ChunkNode,
  m: NodeMetrics,
  locate: (hash: ContentHash) => [Locator, boolean],
): [BuiltNode, StagedChunk | null] {
  const structuralHash = nodeLogicalHash(node);
  const encoded = encodeNode(node);
  const hash = objectHash(encoded);
  const [locator, fresh] = locate(hash);
  const nodeRef = { objectHash: hash, locator };
  return [{ nodeRef, metrics: m, structuralHash }, fresh ? { nodeRef, encoded } : null];
}

type PathFrame = { nodeRef: NodeRef; children: BranchChild[]; selected: number };
type LocatedLeaf = { nodeRef: NodeRef; bytes: Bytes; start: number; path: PathFrame[] };

function prepareWithLocator(
  kind: LargeValueKind,
  logicalBytes: Bytes,
  locatorFor: (hash: ContentHash) => Locator,
): PreparedLargeValue {
  validateLogical(kind, logicalBytes);
  const stager = new TreeStager(kind, (hash) => [locatorFor(hash), true]);
  const root = stager.root(stager.leaves(logicalBytes), 0, "MalformedNode");
  return { valueRef: intoValueRef(root, kind), stagedChunks: stager.staged };
}

/** Construct a canonical tree with fresh random capabilities for every new node. */
export const prepare = (kind: LargeValueKind, logicalBytes: Bytes): PreparedLargeValue =>
  prepareWithLocator(kind, logicalBytes, randomLocator);

/** Rebuild while preserving the locator of every byte-equal node from a previous preparation. */
export function prepareReusing(
  kind: LargeValueKind,
  logicalBytes: Bytes,
  previous: StagedChunk[],
): PreparedLargeValue {
  const existing = new Map(previous.map((c) => [toHex(c.nodeRef.objectHash), c.nodeRef.locator]));
  return prepareWithLocator(
    kind,
    logicalBytes,
    (hash) => existing.get(toHex(hash)) ?? randomLocator(),
  );
}

/** Builds content-defined levels; `locate` names each node and says whether it is new. */
class TreeStager {
  staged: StagedChunk[] = [];
  constructor(
    private readonly kind: LargeValueKind,
    private readonly locate: (hash: ContentHash) => [Locator, boolean],
  ) {}
  node(node: ChunkNode): BuiltNode {
    validateUntypedNodeStructure(node);
    const [built, chunk] = sealNode(node, nodeMetrics(this.kind, node), this.locate);
    if (chunk) this.staged.push(chunk);
    return built;
  }
  leaves(bytes: Bytes): BuiltNode[] {
    return leafRanges(this.kind, bytes).map((r) =>
      this.node(leafNode(this.kind, bytes.slice(r.start, r.end))),
    );
  }
  branches(level: BuiltNode[]): BuiltNode[] {
    return branchRanges(this.kind, level).map((r) =>
      this.node({
        tag: "Branch",
        format: FORMAT_VERSION,
        kind: this.kind,
        children: level.slice(r.start, r.end).map(toChild),
      }),
    );
  }
  root(level: BuiltNode[], depth: number, tooDeep: LargeValueErrorKind): BuiltNode {
    while (level.length > 1) {
      depth += 1;
      if (depth > MAX_TREE_DEPTH) throw err(tooDeep);
      level = this.branches(level);
    }
    return level.pop() ?? fail("MalformedNode");
  }
}

const reusingStager = (
  kind: LargeValueKind,
  existing: Map<string, Locator>,
  fresh: (hash: ContentHash) => Locator,
) =>
  new TreeStager(kind, (hash) => {
    const locator = existing.get(toHex(hash));
    return locator ? [locator, false] : [fresh(hash), true];
  });

const leafNode = (kind: LargeValueKind, bytes: Bytes): ChunkNode => ({
  tag: "Leaf",
  format: FORMAT_VERSION,
  kind,
  bytes,
});

function loadAuthenticatedNodeAttempt(
  formatVersion: number,
  kind: LargeValueKind,
  nodeRef: NodeRef,
  expectedLogicalHash: ContentHash,
  inputs: EvaluationInputs,
): ChunkNode {
  const request: ChunkRequest = { objectHash: nodeRef.objectHash, locator: nodeRef.locator };
  const chunk = inputs.loadedChunk(request);
  const node = chunk.verifiesObjectHash(nodeRef.objectHash)
    ? decodeObjectVerifiedNodeForFormat(formatVersion, kind, chunk.bytes())
    : decodeNodeForFormat(formatVersion, kind, nodeRef.objectHash, chunk.bytes());
  if (!equalBytes(nodeLogicalHash(node), expectedLogicalHash)) throw err("DescriptorMismatch");
  return node;
}

/** Descend to one leaf, letting `choose` pick a child of each branch; blocked reads fail at once. */
function descendAttempt(
  value: LargeValueRef,
  nodeRef: NodeRef,
  expectedHash: ContentHash,
  path: PathFrame[],
  maxDepth: number,
  inputs: EvaluationInputs,
  choose: (children: BranchChild[]) => number,
): LocatedLeaf {
  for (;;) {
    if (path.length > maxDepth) throw err("InvalidTree");
    const node = loadAuthenticatedNodeAttempt(
      value.formatVersion,
      value.kind,
      nodeRef,
      expectedHash,
      inputs,
    );
    if (node.tag === "Leaf") return { nodeRef, bytes: node.bytes, start: 0, path };
    const selected = choose(node.children);
    const child = node.children[selected];
    path.push({ nodeRef, children: node.children, selected });
    nodeRef = child.nodeRef;
    expectedHash = child.logicalHash;
  }
}

function locateLeafAttempt(
  value: LargeValueRef,
  offset: number,
  baseLen: number,
  inputs: EvaluationInputs,
): LocatedLeaf {
  if (offset >= baseLen) throw err("MalformedScalar");
  let start = 0;
  const leaf = descendAttempt(
    value,
    value.root,
    value.logicalHash,
    [],
    MAX_TREE_DEPTH,
    inputs,
    (children) => {
      for (let i = 0; i < children.length; i++) {
        const end = start + children[i].metrics.byteLength;
        if (offset < end) return i;
        start = end;
      }
      return fail("DescriptorMismatch");
    },
  );
  if (offset >= start + leaf.bytes.length) throw err("DescriptorMismatch");
  leaf.start = start;
  return leaf;
}

function nextLeafAttempt(
  value: LargeValueRef,
  current: LocatedLeaf,
  inputs: EvaluationInputs,
): LocatedLeaf {
  const path = current.path.map((f) => ({ ...f }));
  let child: BranchChild;
  for (;;) {
    const frame = path[path.length - 1] ?? fail("DescriptorMismatch");
    if (frame.selected + 1 < frame.children.length) {
      frame.selected += 1;
      child = frame.children[frame.selected];
      break;
    }
    path.pop();
  }
  const start = add(current.start, current.bytes.length);
  const leaf = descendAttempt(
    value,
    child.nodeRef,
    child.logicalHash,
    path,
    Infinity,
    inputs,
    (children) => (children.length ? 0 : fail("MalformedNode")),
  );
  leaf.start = start;
  return leaf;
}

// ---- Node codec ----

export const decodeNode = (
  kind: LargeValueKind,
  expectedHash: ContentHash,
  encoded: Bytes,
): ChunkNode => decodeNodeForFormat(FORMAT_VERSION, kind, expectedHash, encoded);

/** Decode through the exact format selected by the owner descriptor; never probe another codec. */
function decodeNodeForFormat(
  formatVersion: number,
  kind: LargeValueKind,
  expectedHash: ContentHash,
  encoded: Bytes,
): ChunkNode {
  if (encoded.length > MAX_ENCODED_NODE_BYTES) throw err("MalformedNode");
  if (!equalBytes(objectHash(encoded), expectedHash)) throw err("ObjectHashMismatch");
  return decodeObjectVerifiedNodeForFormat(formatVersion, kind, encoded);
}

function decodeObjectVerifiedNodeForFormat(
  formatVersion: number,
  kind: LargeValueKind,
  encoded: Bytes,
): ChunkNode {
  if (encoded.length > MAX_ENCODED_NODE_BYTES) throw err("MalformedNode");
  const descriptorFormat = formatFromVersion(formatVersion);
  if (descriptorFormat !== preflightNodeFormat(encoded)) throw err("DescriptorMismatch");
  const node = formatDecodeNode(descriptorFormat, encoded);
  if (node.kind !== kind) throw err("DescriptorMismatch");
  return node;
}

export const decodeAuthenticatedNode = (expectedHash: ContentHash, encoded: Bytes) =>
  decodeNodeUntypedAuthenticated(expectedHash, encoded);

/** Authenticate and decode a node before a referencing descriptor is available. */
export function decodeNodeUntypedAuthenticated(
  expectedHash: ContentHash,
  encoded: Bytes,
): ChunkNode {
  if (encoded.length > MAX_ENCODED_NODE_BYTES) throw err("MalformedNode");
  if (!equalBytes(objectHash(encoded), expectedHash)) throw err("ObjectHashMismatch");
  return formatDecodeNode(preflightNodeFormat(encoded), encoded);
}

export function encodeNode(node: ChunkNode): Bytes {
  formatFromVersion(node.format);
  return encodeNodeV1(node);
}

function encodeNodeV1(node: ChunkNode): Bytes {
  const schema = chunkNodeSchema(node.kind);
  let value: EnumValue;
  try {
    if (node.tag === "Leaf") {
      value = EnumValue.create(0, schema.case(0).payload, [
        { kind: "U8", v: node.format },
        { kind: "U8", v: kindTag(node.kind) },
        { kind: "Bytes", v: node.bytes },
      ]);
    } else {
      const childSchema = chunkNodeChildSchema(node.kind);
      const children: Value[] = node.children.map((child) => {
        const fields: Value[] = [
          { kind: "Bytes", v: child.nodeRef.objectHash },
          { kind: "Bytes", v: child.nodeRef.locator },
          { kind: "U64", v: BigInt(child.metrics.byteLength) },
          { kind: "Bytes", v: child.logicalHash },
        ];
        const utf16 = child.metrics.utf16Length;
        if ((node.kind === "Bytes") !== (utf16 === null)) throw err("MalformedNode");
        if (utf16 !== null) fields.push({ kind: "U64", v: BigInt(utf16) });
        return { kind: "Record", v: new OwnedRecord(childSchema.create(fields), childSchema) };
      });
      value = EnumValue.create(1, schema.case(1).payload, [
        { kind: "U8", v: node.format },
        { kind: "U8", v: kindTag(node.kind) },
        { kind: "Array", v: children },
      ]);
    }
    return encodeSingleFieldValue({ kind: "Enum", v: value }, { kind: "Enum", schema });
  } catch (e) {
    if (e instanceof LargeValueError) throw e;
    throw err("MalformedNode");
  }
}

const textKind = (kind: LargeValueKind) => (kind === "Bytes" ? "Bytes" : "Text");
function cached<K, T>(build: (k: K) => T, key: (k: K) => unknown = (k) => k): (k: K) => T {
  const m = new Map<unknown, T>();
  return (k) => {
    const id = key(k);
    let v = m.get(id);
    if (v === undefined) m.set(id, (v = build(k)));
    return v;
  };
}

const chunkNodeChildSchema = cached(
  (kind: LargeValueKind) =>
    RecordDescriptor.new([
      ["object_hash", rawBytes()],
      ["locator", rawBytes()],
      ["byte_length", "U64"],
      ["logical_hash", rawBytes()],
      ...(kind !== "Bytes" ? [["utf16_length", "U64"] as [string, ValueType]] : []),
    ]),
  textKind,
);

const chunkNodeSchema = cached(
  (kind: LargeValueKind) =>
    EnumSchema.new("groove.internal.large_value.chunk_node", [
      enumCase(
        "Leaf",
        RecordDescriptor.new([
          ["format", "U8"],
          ["kind", "U8"],
          ["bytes", rawBytes()],
        ]),
      ),
      enumCase(
        "Branch",
        RecordDescriptor.new([
          ["format", "U8"],
          ["kind", "U8"],
          [
            "children",
            { kind: "Array", inner: { kind: "Record", descriptor: chunkNodeChildSchema(kind) } },
          ],
        ]),
      ),
    ]),
  textKind,
);

/** Decode the authenticated payload; exact canonical re-encoding rejects alternates. */
function decodeCanonicalNodeV1(encoded: Bytes): ChunkNode {
  if (encoded.length > MAX_ENCODED_NODE_BYTES) throw err("MalformedNode");
  const [tag, payload] = splitNode(encoded);
  if (tag === 0) {
    if (payload.length < 2) throw err("MalformedNode");
    const format = payload[0];
    const kind = kindFromTagOr(payload[1], "MalformedNode");
    const bytes = payload.subarray(2);
    if (bytes.length > LEAF_MAX_BYTES) throw err("MalformedNode");
    // Canonical leaf: [0, format, kind] header followed by the unchanged raw bytes.
    if (
      encoded.length !== bytes.length + 3 ||
      encoded[0] !== 0 ||
      encoded[1] !== format ||
      encoded[2] !== kindTag(kind)
    )
      throw err("MalformedNode");
    const node: ChunkNode = { tag: "Leaf", format, kind, bytes: bytes.slice() };
    validateUntypedNodeStructure(node);
    return node;
  }
  if (payload.length < 2) throw err("MalformedNode");
  const kind = kindFromTagOr(payload[1], "MalformedNode");
  const schema = chunkNodeSchema(kind);
  preflightNodeBounds(encoded, schema);
  let node: ChunkNode;
  try {
    const value = decodeSingleFieldValue(encoded, { kind: "Enum", schema });
    if (value.kind !== "Enum") throw err("MalformedNode");
    const f = value.v.record.toValues();
    if (
      value.v.tag === 0 &&
      f.length === 3 &&
      f[0].kind === "U8" &&
      f[1].kind === "U8" &&
      f[2].kind === "Bytes"
    ) {
      node = {
        tag: "Leaf",
        format: f[0].v,
        kind: kindFromTagOr(f[1].v, "MalformedNode"),
        bytes: f[2].v,
      };
    } else if (
      value.v.tag === 1 &&
      f.length === 3 &&
      f[0].kind === "U8" &&
      f[1].kind === "U8" &&
      f[2].kind === "Array"
    ) {
      const children = f[2].v.map((child): BranchChild => {
        if (child.kind !== "Record") throw err("MalformedNode");
        const c = child.v.toValues();
        const text = kind !== "Bytes";
        if (c.length !== (text ? 5 : 4) || c[2].kind !== "U64" || (text && c[4].kind !== "U64"))
          throw err("MalformedNode");
        return {
          nodeRef: {
            objectHash: raw32Or(c[0], "MalformedNode"),
            locator: raw32Or(c[1], "MalformedNode"),
          },
          metrics: {
            byteLength: Number(c[2].v),
            utf16Length: text ? Number((c[4] as { v: bigint }).v) : null,
          },
          logicalHash: raw32Or(c[3], "MalformedNode"),
        };
      });
      node = { tag: "Branch", format: f[0].v, kind, children };
    } else throw err("MalformedNode");
  } catch (e) {
    if (e instanceof LargeValueError) throw e;
    throw err("MalformedNode");
  }
  if (!equalBytes(encodeNodeV1(node), encoded)) throw err("MalformedNode");
  validateUntypedNodeStructure(node);
  return node;
}

function splitNode(encoded: Bytes): [number, Bytes] {
  try {
    return splitVariantRecord(encoded);
  } catch {
    throw err("MalformedNode");
  }
}

function preflightNodeBounds(encoded: Bytes, schema: EnumSchema): void {
  const [tag, payload] = splitNode(encoded);
  if (tag !== 1) return;
  let count: number;
  try {
    const span = schema.case(1).payload.fieldSpan(payload, 2);
    const array = payload.subarray(span.start, span.end);
    if (array.length < 4) throw err("MalformedNode");
    count = new DataView(array.buffer, array.byteOffset, 4).getUint32(0, true);
  } catch {
    throw err("MalformedNode");
  }
  if (count > BRANCH_MAX_CHILDREN) throw err("MalformedNode");
}

/** Read only the self-describing selector, before binding leaf bytes or children. */
function preflightNodeFormat(encoded: Bytes): LargeValueFormat {
  const [tag, payload] = splitNode(encoded);
  if (tag !== 0 && tag !== 1) throw err("MalformedNode");
  if (!payload.length) throw err("MalformedNode");
  return formatFromVersion(payload[0]);
}

function validateUntypedNodeStructure(node: ChunkNode): void {
  checkFormat(node.format);
  if (node.tag === "Leaf") {
    if (node.bytes.length > LEAF_MAX_BYTES) throw err("MalformedNode");
    if (node.kind !== "Bytes" && !isUtf8(node.bytes)) throw err("InvalidUtf8");
    return;
  }
  const children = node.children;
  if (
    !children.length ||
    children.length > BRANCH_MAX_CHILDREN ||
    children.some((c) => c.metrics.byteLength === 0)
  )
    throw err("MalformedNode");
  sumMetrics(children.map((c) => c.metrics));
  const text = node.kind !== "Bytes";
  if (children.some((c) => (c.metrics.utf16Length !== null) !== text)) throw err("MalformedNode");
}

// ---- Hashes and metrics ----

export const objectHash = (encoded: Bytes): ContentHash =>
  hashDomain("groove-large-object-v1", encoded);

const nodeLogicalHash = (node: ChunkNode): ContentHash =>
  bindGroupingHash(node.format, node.kind, nodeGroupingHash(node));

/** Format/kind-neutral grouping identity; logical identity binds those dimensions by XOR. */
function nodeGroupingHash(node: ChunkNode): ContentHash {
  if (node.tag === "Leaf") return hashDomain("groove-large-leaf-logical-v1", node.bytes);
  const parts: Bytes[] = [];
  for (const child of node.children) {
    parts.push(
      bindGroupingHash(node.format, node.kind, child.logicalHash),
      u64le(child.metrics.byteLength),
      u64le(child.metrics.utf16Length ?? U64_MASK),
    );
  }
  return hashDomain("groove-large-branch-logical-v1", concatBytes(...parts));
}

function bindGroupingHash(
  format: number,
  kind: LargeValueKind,
  groupingHash: ContentHash,
): ContentHash {
  const mask = hashDomain(
    "groove-large-logical-kind-format-v1",
    Uint8Array.of(format, kindTag(kind)),
  );
  return groupingHash.map((b, i) => b ^ mask[i]);
}

function nodeMetrics(kind: LargeValueKind, node: ChunkNode): NodeMetrics {
  if (node.kind !== kind) throw err("DescriptorMismatch");
  return node.tag === "Leaf"
    ? metrics(kind, node.bytes)
    : sumMetrics(node.children.map((c) => c.metrics));
}

const U64_MASK = (1n << 64n) - 1n;
function u64le(v: number | bigint): Bytes {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, BigInt(v), true);
  return out;
}

function hashDomain(domain: string, bytes: Bytes): ContentHash {
  const d = new TextEncoder().encode(domain);
  return new Blake3Hasher().update(u64le(d.length)).update(d).update(bytes).finalize();
}

function checkFormat(format: number): void {
  if (format !== FORMAT_VERSION) throw err("UnsupportedFormat", format);
}

function validateLogical(kind: LargeValueKind, bytes: Bytes): void {
  if (kind === "Bytes") return;
  const source = text(bytes);
  if (kind === "Json") {
    try {
      JSON.parse(source);
    } catch {
      throw err("InvalidJson");
    }
  }
}

function metrics(kind: LargeValueKind, bytes: Bytes): NodeMetrics {
  return { byteLength: bytes.length, utf16Length: kind === "Bytes" ? null : utf16LengthOf(bytes) };
}

function addMetrics(left: NodeMetrics, right: NodeMetrics): NodeMetrics {
  if ((left.utf16Length === null) !== (right.utf16Length === null)) throw err("MalformedNode");
  return {
    byteLength: add(left.byteLength, right.byteLength),
    utf16Length: left.utf16Length === null ? null : add(left.utf16Length, right.utf16Length!),
  };
}

// ---- Stored scalar envelope ----

export function encodeStoredScalar(kind: LargeValueKind, value: StoredScalar): Bytes {
  return encodeStoredScalarCanonical(kind, value);
}

export function encodePrimitiveStoredScalarInto(
  kind: LargeValueKind,
  bytes: Bytes,
  out: ByteBuf,
): void {
  validateLogical(kind, bytes);
  appendVariantRecord(out, 0, bytes);
}

function encodeStoredScalarCanonical(kind: LargeValueKind, value: StoredScalar): Bytes {
  if (value.kind === "Primitive") {
    validateLogical(kind, value.bytes);
    return encodeVariantRecord(0, value.bytes);
  }
  if (value.value.kind !== kind) throw err("DescriptorMismatch");
  validateDescriptor(value.value);
  return encodeEnumOr(
    "MalformedScalar",
    storedScalarSchema(kind),
    1,
    chunkedValues(value.value),
    storedScalarValueType(kind),
  );
}

function encodeEnumOr(
  code: LargeValueErrorKind,
  schema: EnumSchema,
  tag: number,
  values: Value[],
  vt: ValueType,
): Bytes {
  try {
    return encodeSingleFieldValue(
      { kind: "Enum", v: EnumValue.create(tag, schema.case(tag).payload, values) },
      vt,
    );
  } catch {
    throw err(code);
  }
}

function splitScalar(encoded: Bytes): [number, Bytes] {
  try {
    return splitVariantRecord(encoded);
  } catch {
    throw err("MalformedScalar");
  }
}

/** Decode and canonically validate the internal stored-scalar enum. */
export function decodeStoredScalar(kind: LargeValueKind, encoded: Bytes): StoredScalar {
  const [tag, payload] = splitScalar(encoded);
  if (tag === 0) {
    validateLogical(kind, payload);
    return { kind: "Primitive", bytes: payload.slice() };
  }
  if (tag !== 1) throw err("MalformedScalar");
  preflightDescriptorFormat(payload);
  const decoded: StoredScalar = {
    kind: "Chunked",
    value: decodeDescriptorPayloadV1(kind, payload),
  };
  if (!equalBytes(encodeStoredScalarCanonical(kind, decoded), encoded))
    throw err("MalformedScalar");
  return decoded;
}

export function inlineScalarBytes(kind: LargeValueKind, encoded: Bytes): Bytes {
  const [tag, payload] = splitScalar(encoded);
  if (tag === 0) {
    try {
      validateLogical(kind, payload);
    } catch {
      throw err("MalformedScalar");
    }
    return payload;
  }
  if (tag !== 1) throw err("MalformedScalar");
  decodeStoredScalar(kind, encoded);
  throw err("RequiresEvaluation");
}

const storedScalarSchema = cached((kind: LargeValueKind) =>
  EnumSchema.new(
    kind === "Bytes"
      ? "groove.internal.stored_scalar.bytes"
      : kind === "String"
        ? "groove.internal.stored_scalar.string"
        : "groove.internal.stored_scalar.json",
    [
      enumCase("Primitive", primitivePayloadSchema(kind).descriptor),
      enumCase("Chunked", largeValueRefPayloadSchema(kind).descriptor),
    ],
  ),
);
const storedScalarValueType = cached(
  (kind: LargeValueKind): ValueType => ({ kind: "Enum", schema: storedScalarSchema(kind) }),
);

// ---- Durable engine-owned record schemas ----

const NODE_REF_OBJECT_HASH_FIELD = 1;
const NODE_REF_LOCATOR_FIELD = 2;
const PRIMITIVE_VALUE_FIELD = 1;
const EDIT_OFFSET_FIELD = 1;
const EDIT_DELETE_LENGTH_FIELD = 2;
const EDIT_INSERT_BYTES_FIELD = 3;
const EDIT_UTF16_OFFSET_FIELD = 4;
const EDIT_DELETE_UTF16_LENGTH_FIELD = 5;
const LARGE_VALUE_REF_FORMAT_VERSION_FIELD = 1;
const LARGE_VALUE_REF_LOGICAL_HASH_FIELD = 2;
const LARGE_VALUE_REF_ROOT_FIELD = 3;
const LARGE_VALUE_REF_BYTE_LENGTH_FIELD = 4;
const LARGE_VALUE_REF_EDIT_TAIL_FIELD = 5;
const LARGE_VALUE_REF_UTF16_LENGTH_FIELD = 6;

class DurableLargeValueRecordSchema {
  constructor(
    readonly fieldIds: number[],
    readonly descriptor: RecordDescriptor,
  ) {}
  orderedValues(input: [number, Value][]): Value[] {
    const values = new Map<number, Value>();
    for (const [id, v] of input) {
      if (values.has(id)) throw err("MalformedScalar");
      values.set(id, v);
    }
    const ordered = this.fieldIds.map((id) => {
      const v = values.get(id) ?? fail("MalformedScalar");
      values.delete(id);
      return v;
    });
    if (values.size) throw err("MalformedScalar");
    return ordered;
  }
  decodeValues(values: Value[]): Map<number, Value> {
    if (values.length !== this.fieldIds.length) throw err("MalformedScalar");
    return new Map(this.fieldIds.map((id, i) => [id, values[i]]));
  }
}

/** Fixed engine-owned layout from permanent one-based positional field IDs. */
function durableSchema(fields: [number, string, ValueType][]): DurableLargeValueRecordSchema {
  fields = [...fields].sort((a, b) => a[0] - b[0]);
  assert(fields.length > 0, "large-value record must have at least one field");
  assert(
    fields.every(([id], i) => id === i + 1),
    "large-value record field IDs must be contiguous starting at one",
  );
  return new DurableLargeValueRecordSchema(
    fields.map((f) => f[0]),
    RecordDescriptor.new(
      fields.map(([id, name, vt]) => [`f${String(id).padStart(4, "0")}_${name}`, vt]),
    ),
  );
}

function takeField<T>(
  fields: Map<number, Value>,
  id: number,
  extract: (v: Value) => T | undefined,
): T {
  const v = fields.get(id) ?? fail("MalformedScalar");
  fields.delete(id);
  const out = extract(v);
  if (out === undefined) throw err("MalformedScalar");
  return out;
}
const takeU8 = (f: Map<number, Value>, id: number) =>
  takeField(f, id, (v) => (v.kind === "U8" ? v.v : undefined));
const takeU64 = (f: Map<number, Value>, id: number) =>
  takeField(f, id, (v) => (v.kind === "U64" ? Number(v.v) : undefined));
const takeBytes = (f: Map<number, Value>, id: number) =>
  takeField(f, id, (v) => (v.kind === "Bytes" ? v.v : undefined));
const takeRaw32 = (f: Map<number, Value>, id: number) =>
  raw32Or(
    takeField(f, id, (v) => v),
    "MalformedScalar",
  );

function raw32Or(v: Value, code: LargeValueErrorKind): Bytes {
  if (v.kind !== "Bytes" || v.v.length !== 32) throw err(code);
  return v.v;
}

const nodeRefRecordSchema = cached(() =>
  durableSchema([
    [NODE_REF_OBJECT_HASH_FIELD, "object_hash", rawBytes()],
    [NODE_REF_LOCATOR_FIELD, "locator", rawBytes()],
  ]),
);
export const nodeRefDescriptor = () => nodeRefRecordSchema(undefined).descriptor;

export const nodeRefValue = (nodeRef: NodeRef): Value => ({
  kind: "Record",
  v: new OwnedRecord(encodeNodeRef(nodeRef), nodeRefDescriptor()),
});

export function nodeRefFromValue(value: Value): NodeRef {
  if (value.kind !== "Record") throw err("MalformedScalar");
  return nodeRefFromValues(recordValues(() => value.v.toValues()));
}

function nodeRefFromValues(values: Value[]): NodeRef {
  const fields = nodeRefRecordSchema(undefined).decodeValues(values);
  return {
    objectHash: takeRaw32(fields, NODE_REF_OBJECT_HASH_FIELD),
    locator: takeRaw32(fields, NODE_REF_LOCATOR_FIELD),
  };
}

function recordValues(f: () => Value[]): Value[] {
  try {
    return f();
  } catch {
    throw err("MalformedScalar");
  }
}

/** Encode a chunk identity through the canonical Groove record used by indirect roots. */
export function encodeNodeRef(nodeRef: NodeRef): Bytes {
  const values = nodeRefRecordSchema(undefined).orderedValues([
    [NODE_REF_OBJECT_HASH_FIELD, { kind: "Bytes", v: nodeRef.objectHash }],
    [NODE_REF_LOCATOR_FIELD, { kind: "Bytes", v: nodeRef.locator }],
  ]);
  try {
    return nodeRefDescriptor().create(values);
  } catch {
    throw err("MalformedScalar");
  }
}

export function decodeNodeRef(encoded: Bytes): NodeRef {
  const nodeRef = nodeRefFromValues(
    recordValues(() => nodeRefDescriptor().bind(encoded).toValues()),
  );
  if (!equalBytes(encodeNodeRef(nodeRef), encoded)) throw err("MalformedScalar");
  return nodeRef;
}

const primitivePayloadSchema = cached((kind: LargeValueKind) =>
  durableSchema([[PRIMITIVE_VALUE_FIELD, "value", kind === "Bytes" ? rawBytes() : rawString()]]),
);

const largeValueEditSchema = cached(
  (kind: LargeValueKind) =>
    durableSchema([
      [EDIT_OFFSET_FIELD, "offset", "U64"],
      [EDIT_DELETE_LENGTH_FIELD, "delete_length", "U64"],
      [EDIT_INSERT_BYTES_FIELD, "insert_bytes", rawBytes()],
      ...(kind !== "Bytes"
        ? ([
            [EDIT_UTF16_OFFSET_FIELD, "utf16_offset", "U64"],
            [EDIT_DELETE_UTF16_LENGTH_FIELD, "delete_utf16_length", "U64"],
          ] as [number, string, ValueType][])
        : []),
    ]),
  textKind,
);

const largeValueRefPayloadSchema = cached(
  (kind: LargeValueKind) =>
    durableSchema([
      [LARGE_VALUE_REF_FORMAT_VERSION_FIELD, "format_version", "U8"],
      [LARGE_VALUE_REF_LOGICAL_HASH_FIELD, "logical_hash", rawBytes()],
      [LARGE_VALUE_REF_ROOT_FIELD, "root", { kind: "Record", descriptor: nodeRefDescriptor() }],
      [LARGE_VALUE_REF_BYTE_LENGTH_FIELD, "byte_length", "U64"],
      [
        LARGE_VALUE_REF_EDIT_TAIL_FIELD,
        "edit_tail",
        {
          kind: "Array",
          inner: { kind: "Record", descriptor: largeValueEditSchema(kind).descriptor },
        },
      ],
      ...(kind !== "Bytes"
        ? ([[LARGE_VALUE_REF_UTF16_LENGTH_FIELD, "utf16_length", "U64"]] as [
            number,
            string,
            ValueType,
          ][])
        : []),
    ]),
  textKind,
);

const KINDS: LargeValueKind[] = ["Bytes", "String", "Json"];
const kindTag = (kind: LargeValueKind) => KINDS.indexOf(kind);
const kindFromTagOr = (tag: number, code: LargeValueErrorKind): LargeValueKind =>
  KINDS[tag] ?? fail(code);

const largeValueRefSchema = cached(() =>
  EnumSchema.new(
    "groove.internal.large_value_ref",
    KINDS.map((kind) => enumCase(kind, largeValueRefPayloadSchema(kind).descriptor)),
  ),
);

export function encodeLargeValueRef(value: LargeValueRef): Bytes {
  validateDescriptor(value);
  const schema = largeValueRefSchema(undefined);
  return encodeEnumOr("MalformedScalar", schema, kindTag(value.kind), chunkedValues(value), {
    kind: "Enum",
    schema,
  });
}

export function decodeLargeValueRef(encoded: Bytes): LargeValueRef {
  const [tag, payload] = splitScalar(encoded);
  const kind = kindFromTagOr(tag, "MalformedScalar");
  preflightDescriptorFormat(payload);
  const decoded = decodeDescriptorPayloadV1(kind, payload);
  if (!equalBytes(encodeLargeValueRef(decoded), encoded)) throw err("MalformedScalar");
  return decoded;
}

/** The descriptor payload starts with its fixed-width `format_version:u8` field. */
function preflightDescriptorFormat(payload: Bytes): LargeValueFormat {
  if (!payload.length) throw err("MalformedScalar");
  return formatFromVersion(payload[0]);
}

function decodeDescriptorPayloadV1(kind: LargeValueKind, payload: Bytes): LargeValueRef {
  const schema = largeValueRefPayloadSchema(kind);
  const values = recordValues(() => schema.descriptor.bind(payload).toValues());
  if (
    !equalBytes(
      recordBytes(() => schema.descriptor.create(values)),
      payload,
    )
  )
    throw err("MalformedScalar");
  return decodeChunkedValues(kind, values);
}

function recordBytes(f: () => Bytes): Bytes {
  try {
    return f();
  } catch {
    throw err("MalformedScalar");
  }
}

function chunkedValues(value: LargeValueRef): Value[] {
  const kind = value.kind;
  const editSchema = largeValueEditSchema(kind);
  const edits: Value[] = value.editTail.map((edit) => {
    const fields: [number, Value][] = [
      [EDIT_OFFSET_FIELD, { kind: "U64", v: BigInt(edit.offset) }],
      [EDIT_DELETE_LENGTH_FIELD, { kind: "U64", v: BigInt(edit.deleteLength) }],
      [EDIT_INSERT_BYTES_FIELD, { kind: "Bytes", v: edit.insertBytes }],
    ];
    if (kind !== "Bytes")
      fields.push(
        [EDIT_UTF16_OFFSET_FIELD, { kind: "U64", v: BigInt(edit.utf16Offset) }],
        [EDIT_DELETE_UTF16_LENGTH_FIELD, { kind: "U64", v: BigInt(edit.deleteUtf16Length) }],
      );
    const d = editSchema.descriptor;
    return { kind: "Record", v: new OwnedRecord(d.create(editSchema.orderedValues(fields)), d) };
  });
  const fields: [number, Value][] = [
    [LARGE_VALUE_REF_FORMAT_VERSION_FIELD, { kind: "U8", v: value.formatVersion }],
    [LARGE_VALUE_REF_LOGICAL_HASH_FIELD, { kind: "Bytes", v: value.logicalHash }],
    [LARGE_VALUE_REF_ROOT_FIELD, nodeRefValue(value.root)],
    [LARGE_VALUE_REF_BYTE_LENGTH_FIELD, { kind: "U64", v: BigInt(value.byteLength) }],
    [LARGE_VALUE_REF_EDIT_TAIL_FIELD, { kind: "Array", v: edits }],
  ];
  if (kind !== "Bytes") {
    assert(value.utf16Length !== null, "validated text descriptor has a UTF-16 length");
    fields.push([
      LARGE_VALUE_REF_UTF16_LENGTH_FIELD,
      { kind: "U64", v: BigInt(value.utf16Length) },
    ]);
  }
  return largeValueRefPayloadSchema(kind).orderedValues(fields);
}

function decodeChunkedValues(kind: LargeValueKind, values: Value[]): LargeValueRef {
  const fields = largeValueRefPayloadSchema(kind).decodeValues(values);
  const formatVersion = takeU8(fields, LARGE_VALUE_REF_FORMAT_VERSION_FIELD);
  formatFromVersion(formatVersion);
  const logicalHash = takeRaw32(fields, LARGE_VALUE_REF_LOGICAL_HASH_FIELD);
  const root = takeField(fields, LARGE_VALUE_REF_ROOT_FIELD, (v) => v);
  const byteLength = takeU64(fields, LARGE_VALUE_REF_BYTE_LENGTH_FIELD);
  const utf16Length = kind === "Bytes" ? null : takeU64(fields, LARGE_VALUE_REF_UTF16_LENGTH_FIELD);
  const edits = takeField(fields, LARGE_VALUE_REF_EDIT_TAIL_FIELD, (v) =>
    v.kind === "Array" ? v.v : undefined,
  );
  const editTail = edits.map((editValue): ReplaceEdit => {
    if (editValue.kind !== "Record") throw err("MalformedScalar");
    const f = largeValueEditSchema(kind).decodeValues(recordValues(() => editValue.v.toValues()));
    const insertBytes = takeBytes(f, EDIT_INSERT_BYTES_FIELD);
    const text = kind !== "Bytes";
    const utf16Offset = text ? takeU64(f, EDIT_UTF16_OFFSET_FIELD) : 0;
    const deleteUtf16Length = text ? takeU64(f, EDIT_DELETE_UTF16_LENGTH_FIELD) : 0;
    return {
      offset: takeU64(f, EDIT_OFFSET_FIELD),
      deleteLength: takeU64(f, EDIT_DELETE_LENGTH_FIELD),
      insertBytes,
      utf16Offset,
      deleteUtf16Length,
      insertUtf16Length: text ? utf16LengthOf(insertBytes) : 0,
    };
  });
  const value: LargeValueRef = {
    kind,
    formatVersion,
    logicalHash,
    root: nodeRefFromValue(root),
    byteLength,
    utf16Length,
    editTail,
  };
  validateDescriptorV1(value);
  return value;
}

// ---- Descriptor validation ----

function validateDescriptor(value: LargeValueRef): void {
  formatFromVersion(value.formatVersion);
  validateDescriptorV1(value);
}

function validateDescriptorV1(value: LargeValueRef): void {
  validateDescriptorShapeV1(value);
  if (value.editTail.length > MAX_EDIT_COUNT) throw err("MalformedScalar");
  let tailBytes = 0;
  for (const edit of value.editTail) {
    tailBytes = add(add(tailBytes, edit.insertBytes.length), 24);
    if (tailBytes > MAX_EDIT_TAIL_BYTES) throw err("MalformedScalar");
  }
}

function validateDescriptorShape(value: LargeValueRef): void {
  formatFromVersion(value.formatVersion);
  validateDescriptorShapeV1(value);
}

function validateDescriptorShapeV1(value: LargeValueRef): void {
  let byteLength = value.byteLength;
  let utf16Length = value.utf16Length;
  for (const edit of [...value.editTail].reverse()) {
    const inserted = edit.insertBytes.length;
    if (add(edit.offset, inserted) > byteLength) throw err("MalformedScalar");
    byteLength = add(sub(byteLength, inserted), edit.deleteLength);
    if (value.kind === "Bytes") {
      if (edit.utf16Offset !== 0 || edit.deleteUtf16Length !== 0 || edit.insertUtf16Length !== 0)
        throw err("MalformedScalar");
      continue;
    }
    if (utf16LengthOf(edit.insertBytes) !== edit.insertUtf16Length) throw err("MalformedScalar");
    const current = utf16Length ?? fail("MalformedScalar");
    if (add(edit.utf16Offset, edit.insertUtf16Length) > current) throw err("MalformedScalar");
    utf16Length = add(sub(current, edit.insertUtf16Length), edit.deleteUtf16Length);
  }
  if ((value.kind === "Bytes") !== (value.utf16Length === null)) throw err("MalformedScalar");
}

/** The tail-free descriptor of the base tree, by undoing each edit's length effects. */
function tailBaseDescriptor(value: LargeValueRef): LargeValueRef {
  validateDescriptor(value);
  const replay = cloneRef(value);
  for (const edit of [...value.editTail].reverse()) {
    replay.byteLength = add(sub(replay.byteLength, edit.insertBytes.length), edit.deleteLength);
    replay.utf16Length =
      replay.kind === "Bytes"
        ? null
        : add(
            sub(replay.utf16Length ?? fail("MalformedScalar"), edit.insertUtf16Length),
            edit.deleteUtf16Length,
          );
  }
  replay.editTail = [];
  validateDescriptor(replay);
  return replay;
}

/** Replay an untrusted descriptor's tail against its base tree. */
export function validateEditTailAttempt(value: LargeValueRef, inputs: EvaluationInputs): void {
  let replay = tailBaseDescriptor(value);
  for (const expected of value.editTail) {
    const next = replaceTailWithBoundsAttempt(
      replay,
      expected.offset,
      expected.deleteLength,
      expected.insertBytes,
      inputs,
      false,
    ).value;
    const last = next.editTail[next.editTail.length - 1];
    if (!last || !editEq(last, expected)) throw err("DescriptorMismatch");
    replay = next;
  }
  if (!largeValueRefEq(replay, value)) throw err("DescriptorMismatch");
}

// ---- Materialization ----

/** Materialize one logical value; missing chunks surface as the evaluator's blocked outcome. */
export function materializeAttempt(value: LargeValueRef, inputs: EvaluationInputs): Bytes {
  const kind = value.kind;
  const leaves: Bytes[] = [];
  const visitor: LogicalVisitor = {
    leaf(span, bytes) {
      if (span.metrics) {
        let actual: NodeMetrics | null = null;
        try {
          actual = metrics(kind, bytes);
        } catch {}
        if (!actual || !metricsEq(actual, span.metrics)) throw err("DescriptorMismatch");
      }
      leaves.push(bytes);
    },
    branch(span, children) {
      if (span.metrics && !metricsEq(sumMetrics(children.map((c) => c.metrics)), span.metrics))
        throw err("DescriptorMismatch");
    },
    childLen: () => 0,
  };
  checkFormat(value.formatVersion);
  const rootMetrics = value.editTail.length
    ? null
    : { byteLength: value.byteLength, utf16Length: value.utf16Length };
  walkLogical(value, { start: 0, len: 0, metrics: rootMetrics }, visitor, inputs);
  const bytes = applyEdits(concatBytes(...leaves), value.editTail);
  validateLogical(kind, bytes);
  const actual = metrics(kind, bytes);
  if (actual.byteLength !== value.byteLength || actual.utf16Length !== value.utf16Length)
    throw err("DescriptorMismatch");
  return bytes;
}

function applyEdits(bytes: Bytes, edits: ReplaceEdit[]): Bytes {
  for (const edit of edits) {
    const end = add(edit.offset, edit.deleteLength);
    if (end > bytes.length) throw err("MalformedNode");
    bytes = concatBytes(bytes.subarray(0, edit.offset), edit.insertBytes, bytes.subarray(end));
  }
  return bytes;
}

export const materializeRecordAttempt = (
  descriptor: RecordDescriptor,
  raw: Bytes,
  inputs: EvaluationInputs,
) => materializeRecordBorrowedAttempt(descriptor, raw, null, inputs);

/** Returns `raw` itself when nothing selected is indirect; `fields` limits materialization. */
export function materializeRecordBorrowedAttempt(
  descriptor: RecordDescriptor,
  raw: Bytes,
  fields: number[] | null,
  inputs: EvaluationInputs,
): Bytes {
  const indices = fields ?? descriptor.fields.map((_, i) => i);
  if (!descriptor.fieldsContainIndirectValues(raw, indices)) return raw;
  const values = descriptor.bind(raw).toValues();
  const blocked = { v: false };
  let changed = false;
  values.forEach((value, index) => {
    if (fields && !fields.includes(index)) return;
    const r = materializeValueAttempt(value, inputs, blocked);
    values[index] = r.value;
    changed ||= r.changed;
  });
  if (blocked.v) throw evaluationBlocked();
  return changed ? descriptor.create(values) : raw;
}

/** Materialize only the fields an operator inspects; others stay encoded. */
export function materializeRecordFieldsAttempt(
  descriptor: RecordDescriptor,
  raw: Bytes,
  fieldIndices: number[],
  inputs: EvaluationInputs,
): Bytes {
  if (!descriptor.fieldsContainIndirectValues(raw, fieldIndices)) return raw.slice();
  const values = descriptor.bind(raw).toValues();
  const blocked = { v: false };
  let changed = false;
  for (const index of fieldIndices) {
    if (index >= values.length) throw RecordError.oob(index, descriptor.fields.length);
    const r = materializeValueAttempt(values[index], inputs, blocked);
    values[index] = r.value;
    changed ||= r.changed;
  }
  if (blocked.v) throw evaluationBlocked();
  return changed ? descriptor.create(values) : raw.slice();
}

function materializeRawRecord(
  descriptor: RecordDescriptor,
  raw: Bytes,
  inputs: EvaluationInputs,
  blocked: { v: boolean },
): Bytes | null {
  try {
    return materializeRecordAttempt(descriptor, raw, inputs);
  } catch (e) {
    if (!isEvaluationBlocked(e)) throw e;
    blocked.v = true;
    return null;
  }
}

function materializeValueAttempt(
  value: Value,
  inputs: EvaluationInputs,
  blocked: { v: boolean },
): { value: Value; changed: boolean } {
  switch (value.kind) {
    case "Large": {
      let bytes: Bytes;
      try {
        bytes = materializeAttempt(value.v, inputs);
      } catch (e) {
        if (!isEvaluationBlocked(e)) throw e;
        blocked.v = true;
        return { value, changed: false };
      }
      // JSON is exposed through the string scalar type.
      return {
        value:
          value.v.kind === "Bytes"
            ? { kind: "Bytes", v: bytes }
            : { kind: "String", v: text(bytes) },
        changed: true,
      };
    }
    case "Tuple":
    case "Array": {
      let changed = false;
      const items = value.v.map((item) => {
        const r = materializeValueAttempt(item, inputs, blocked);
        changed ||= r.changed;
        return r.value;
      });
      return { value: { kind: value.kind, v: items } as Value, changed };
    }
    case "Nullable": {
      if (value.v === null) return { value, changed: false };
      const r = materializeValueAttempt(value.v, inputs, blocked);
      return { value: { kind: "Nullable", v: r.value }, changed: r.changed };
    }
    case "Record": {
      const raw = materializeRawRecord(value.v.descriptor, value.v.raw, inputs, blocked);
      if (!raw) return { value, changed: false };
      return {
        value: { kind: "Record", v: new OwnedRecord(raw, value.v.descriptor) },
        changed: !equalBytes(raw, value.v.raw),
      };
    }
    case "Enum": {
      const record = value.v.record;
      const raw = materializeRawRecord(record.descriptor, record.raw, inputs, blocked);
      if (!raw) return { value, changed: false };
      return {
        value: {
          kind: "Enum",
          v: new EnumValue(value.v.tag, new OwnedRecord(raw, record.descriptor)),
        },
        changed: !equalBytes(raw, record.raw),
      };
    }
    default:
      return { value, changed: false };
  }
}

// ---- Range reads ----

type RangePiece = { kind: "Base"; range: Range } | { kind: "Inserted"; bytes: Bytes };
type RangeUnit = "Bytes" | "Utf16";

/** An edit's `[offset, inserted, deleted]` in `unit`. */
const editSpan = (unit: RangeUnit, e: ReplaceEdit): [number, number, number] =>
  unit === "Bytes"
    ? [e.offset, e.insertBytes.length, e.deleteLength]
    : [e.utf16Offset, e.insertUtf16Length, e.deleteUtf16Length];

const childLength = (unit: RangeUnit, m: NodeMetrics): number =>
  unit === "Bytes" ? m.byteLength : (m.utf16Length ?? fail("DescriptorMismatch"));

const sliceUnit = (unit: RangeUnit, bytes: Bytes, r: Range): Bytes =>
  unit === "Bytes" ? bytes.slice(r.start, r.end) : sliceUtf16(bytes, r);

/** Read a byte range of the final value; tail insertions are served directly. */
export const byteRangeAttempt = (
  value: LargeValueRef,
  range: Range,
  inputs: EvaluationInputs,
): Bytes => rangeAttempt(value, range, "Bytes", inputs);

export const utf16RangeAttempt = (
  value: LargeValueRef,
  range: Range,
  inputs: EvaluationInputs,
): Bytes => rangeAttempt(value, range, "Utf16", inputs);

function rangeAttempt(
  value: LargeValueRef,
  range: Range,
  unit: RangeUnit,
  inputs: EvaluationInputs,
): Bytes {
  const length =
    unit === "Bytes" ? value.byteLength : (value.utf16Length ?? fail("MalformedScalar"));
  if (range.start > range.end || range.end > length) throw err("MalformedScalar");
  const output: Bytes[] = [];
  readPieces(value, range, length, unit, (piece, baseLen) =>
    output.push(
      piece.kind === "Inserted"
        ? piece.bytes
        : baseRangeAttempt(value, baseLen, piece.range, unit, inputs),
    ),
  );
  return concatBytes(...output);
}

/** Map a final range onto the base and read each piece; a blocked piece wins only after all were tried. */
function readPieces(
  value: LargeValueRef,
  range: Range,
  length: number,
  unit: RangeUnit,
  read: (piece: RangePiece, baseLen: number) => void,
): void {
  const pieces = mapFinalRangeToBase(range, value.editTail, unit);
  const baseLen = baseLength(length, value.editTail, unit);
  let blocked = false;
  for (const piece of pieces) {
    try {
      read(piece, baseLen);
    } catch (e) {
      if (!isEvaluationBlocked(e)) throw e;
      blocked = true;
    }
  }
  if (blocked) throw evaluationBlocked();
}

/** Resolve a final UTF-16 boundary to its byte offset by binary search over code-point boundaries. */
export function utf16OffsetToByteAttempt(
  value: LargeValueRef,
  offset: number,
  inputs: EvaluationInputs,
): number {
  const total = value.utf16Length ?? fail("MalformedScalar");
  if (offset > total) throw err("MalformedScalar");
  if (offset === 0) return 0;
  if (offset === total) return value.byteLength;
  let low = 0;
  let high = value.byteLength;
  while (low < high) {
    const midpoint = low + Math.floor((high - low) / 2);
    const boundary = utf8BoundaryAtOrBeforeAttempt(value, midpoint, inputs);
    const units = utf16LengthForByteRangeAttempt(value, { start: 0, end: boundary }, inputs);
    if (units < offset) low = utf8NextBoundaryAttempt(value, boundary, inputs);
    else high = boundary;
  }
  if (utf16LengthForByteRangeAttempt(value, { start: 0, end: low }, inputs) !== offset)
    throw err("MalformedScalar");
  return low;
}

function utf8BoundaryAtOrBeforeAttempt(
  value: LargeValueRef,
  offset: number,
  inputs: EvaluationInputs,
): number {
  if (offset === 0 || offset === value.byteLength) return offset;
  const start = Math.max(offset - 3, 0);
  const bytes = byteRangeAttempt(value, { start, end: offset + 1 }, inputs);
  let index = bytes.length - 1;
  if (index < 0) throw err("MalformedScalar");
  while ((bytes[index] & 0xc0) === 0x80) if (--index < 0) throw err("MalformedScalar");
  return start + index;
}

function utf8NextBoundaryAttempt(
  value: LargeValueRef,
  boundary: number,
  inputs: EvaluationInputs,
): number {
  if (boundary >= value.byteLength) return value.byteLength;
  const bytes = byteRangeAttempt(
    value,
    { start: boundary, end: Math.min(boundary + 4, value.byteLength) },
    inputs,
  );
  if (!bytes.length) throw err("MalformedScalar");
  const first = bytes[0];
  const width =
    first <= 0x7f
      ? 1
      : first >= 0xc2 && first <= 0xdf
        ? 2
        : first >= 0xe0 && first <= 0xef
          ? 3
          : first >= 0xf0 && first <= 0xf4
            ? 4
            : fail("InvalidUtf8");
  const next = boundary + width;
  if (next > value.byteLength || width > bytes.length) throw err("InvalidUtf8");
  return next;
}

function sliceUtf16(bytes: Bytes, range: Range): Bytes {
  const s = text(bytes);
  let units = 0;
  let byte = 0;
  let start: number | null = null;
  let end: number | null = null;
  const chars = [...s];
  for (let i = 0; i <= chars.length; i++) {
    if (units === range.start) start = byte;
    if (units === range.end) {
      end = byte;
      break;
    }
    if (i < chars.length) {
      const cp = chars[i].codePointAt(0)!;
      units += cp > 0xffff ? 2 : 1;
      byte += cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
    }
    if ((units > range.start && start === null) || units > range.end) throw err("MalformedScalar");
  }
  if (start === null || end === null) throw err("MalformedScalar");
  return bytes.slice(start, end);
}

/** Count UTF-16 units in a byte range, using subtree metrics for fully covered children. */
function utf16LengthForByteRangeAttempt(
  value: LargeValueRef,
  range: Range,
  inputs: EvaluationInputs,
): number {
  if (value.kind === "Bytes" || range.start > range.end || range.end > value.byteLength)
    throw err("MalformedScalar");
  let total = 0;
  readPieces(value, range, value.byteLength, "Bytes", (piece, baseLen) => {
    const n =
      piece.kind === "Inserted"
        ? utf16LengthOf(piece.bytes)
        : baseUtf16LengthForByteRangeAttempt(value, baseLen, piece.range, inputs);
    total = add(total, n);
  });
  return total;
}

function baseUtf16LengthForByteRangeAttempt(
  value: LargeValueRef,
  baseLen: number,
  range: Range,
  inputs: EvaluationInputs,
): number {
  let count = 0;
  const visitor: LogicalVisitor = {
    wants(span) {
      const end = spanEnd(span);
      if (range.start >= end || range.end <= span.start) return false;
      if (range.start <= span.start && end <= range.end) {
        count = add(count, span.metrics?.utf16Length ?? fail("DescriptorMismatch"));
        return false;
      }
      return true;
    },
    leaf(span, bytes) {
      const a = Math.max(range.start, span.start) - span.start;
      const b = Math.min(range.end, spanEnd(span)) - span.start;
      count = add(count, utf16LengthOf(bytes.subarray(a, b)));
    },
    childLen(m) {
      if (m.utf16Length === null) throw err("DescriptorMismatch");
      return m.byteLength;
    },
  };
  if (range.end > baseLen) throw err("MalformedScalar");
  let baseUtf16 = value.utf16Length ?? fail("MalformedScalar");
  for (const edit of [...value.editTail].reverse())
    baseUtf16 = add(sub(baseUtf16, edit.insertUtf16Length), edit.deleteUtf16Length);
  walkLogical(
    value,
    { start: 0, len: baseLen, metrics: { byteLength: baseLen, utf16Length: baseUtf16 } },
    visitor,
    inputs,
  );
  return count;
}

/** Compare an indirect scalar to inline bytes one bounded window at a time. */
export function compareInlineAttempt(
  value: LargeValueRef,
  inline: Bytes,
  inputs: EvaluationInputs,
): number {
  validateDescriptor(value);
  if (value.kind !== "Bytes" && !isUtf8(inline)) throw err("InvalidUtf8");
  const common = Math.min(value.byteLength, inline.length);
  let offset = 0;
  while (offset < common) {
    const end = Math.min(offset + LEAF_MIN_BYTES, common);
    const ordering = compareBytes(
      byteRangeAttempt(value, { start: offset, end }, inputs),
      inline.subarray(offset, end),
    );
    if (ordering !== 0) return Math.sign(ordering);
    offset = end;
  }
  return Math.sign(value.byteLength - inline.length);
}

// ---- JSON pointer over a source prefix ----

export type JsonPointerPrefix =
  | { kind: "Found"; value: unknown | undefined }
  | { kind: "NeedMore" }
  | { kind: "RequiresFullDocument" };

/** Resolve array-index-rooted pointers from a source prefix; object roots need the full document. */
export function jsonPointerPrefix(source: Bytes, pointer: string): JsonPointerPrefix {
  if (pointer === "") return { kind: "RequiresFullDocument" };
  if (!pointer.startsWith("/")) throw err("InvalidJson");
  const [firstRaw, ...rest] = pointer.slice(1).split("/");
  const first = decodeJsonPointerComponent(firstRaw);
  const targetIndex = arrayIndex(first);
  if (targetIndex === null) return { kind: "RequiresFullDocument" };
  const remaining = rest.map(decodeJsonPointerComponent);
  let position = skipJsonWs(source, 0);
  if (source[position] !== 0x5b) return { kind: "RequiresFullDocument" };
  position += 1;
  for (let index = 0; index <= targetIndex; index++) {
    position = skipJsonWs(source, position);
    if (position >= source.length) return { kind: "NeedMore" };
    if (source[position] === 0x5d) return { kind: "Found", value: undefined };
    const end = new StreamingJsonValidator().rootEnd(source.subarray(position));
    if (end === null) throw err("InvalidJson");
    if (end < 0) return { kind: "NeedMore" };
    if (index === targetIndex) {
      let selected: unknown = JSON.parse(text(source.subarray(position, position + end)));
      for (const component of remaining) {
        if (Array.isArray(selected)) {
          const i = arrayIndex(component);
          selected = i === null ? undefined : selected[i];
        } else if (selected !== null && typeof selected === "object")
          selected = Object.prototype.hasOwnProperty.call(selected, component)
            ? (selected as Record<string, unknown>)[component]
            : undefined;
        else selected = undefined;
        if (selected === undefined) break;
      }
      return { kind: "Found", value: selected };
    }
    position = skipJsonWs(source, position + end);
    if (position >= source.length) return { kind: "NeedMore" };
    if (source[position] === 0x5d) return { kind: "Found", value: undefined };
    if (source[position] !== 0x2c) throw err("InvalidJson");
    position += 1;
  }
  throw err("InvalidJson");
}

function arrayIndex(component: string): number | null {
  if (!/^\d+$/.test(component)) return null;
  const n = Number(component);
  return Number.isSafeInteger(n) && String(n) === component ? n : null;
}

function skipJsonWs(source: Bytes, position: number): number {
  while (position < source.length && [0x20, 0x0a, 0x0d, 0x09].includes(source[position]))
    position++;
  return position;
}

function decodeJsonPointerComponent(component: string): string {
  let decoded = "";
  for (let i = 0; i < component.length; i++) {
    const ch = component[i];
    if (ch !== "~") {
      decoded += ch;
      continue;
    }
    const next = component[++i];
    if (next === "0") decoded += "~";
    else if (next === "1") decoded += "/";
    else throw err("InvalidJson");
  }
  return decoded;
}

// ---- Edit-tail coordinate mapping ----

function baseLength(finalLength: number, edits: ReplaceEdit[], unit: RangeUnit): number {
  let length = finalLength;
  for (const edit of [...edits].reverse()) {
    const [, inserted, deleted] = editSpan(unit, edit);
    length = add(sub(length, inserted), deleted);
  }
  return length;
}

function mapFinalRangeToBase(range: Range, edits: ReplaceEdit[], unit: RangeUnit): RangePiece[] {
  let pieces: RangePiece[] = [{ kind: "Base", range }];
  for (const edit of [...edits].reverse()) {
    const [offset, inserted, deleted] = editSpan(unit, edit);
    const insertedEnd = add(offset, inserted);
    const previous: RangePiece[] = [];
    for (const piece of pieces) {
      if (piece.kind !== "Base") {
        previous.push(piece);
        continue;
      }
      const r = piece.range;
      if (r.start < offset)
        previous.push({ kind: "Base", range: { start: r.start, end: Math.min(r.end, offset) } });
      const overlapStart = Math.max(r.start, offset);
      const overlapEnd = Math.min(r.end, insertedEnd);
      if (overlapStart < overlapEnd)
        previous.push({
          kind: "Inserted",
          bytes: sliceUnit(unit, edit.insertBytes, {
            start: overlapStart - offset,
            end: overlapEnd - offset,
          }),
        });
      if (r.end > insertedEnd) {
        const map = (p: number) => add(sub(p, inserted), deleted);
        previous.push({
          kind: "Base",
          range: { start: map(Math.max(r.start, insertedEnd)), end: map(r.end) },
        });
      }
    }
    pieces = previous;
  }
  return pieces;
}

function baseRangeAttempt(
  value: LargeValueRef,
  baseLen: number,
  range: Range,
  unit: RangeUnit,
  inputs: EvaluationInputs,
): Bytes {
  if (range.end > baseLen) throw err("MalformedScalar");
  if (range.start >= range.end) return new Uint8Array(0);
  const out: Bytes[] = [];
  const visitor: LogicalVisitor = {
    exactCover: true,
    wants: (span) => range.start < spanEnd(span) && range.end > span.start,
    leaf(span, bytes) {
      if (unit === "Bytes" && bytes.length !== span.len) throw err("DescriptorMismatch");
      const start = span.start;
      out.push(
        sliceUnit(unit, bytes, {
          start: Math.max(range.start, start) - start,
          end: Math.min(range.end, spanEnd(span)) - start,
        }),
      );
    },
    childLen: (m) => childLength(unit, m),
  };
  walkLogical(value, { start: 0, len: baseLen, metrics: null }, visitor, inputs);
  return concatBytes(...out);
}

// ---- Content-defined chunking (frozen V1 profile) ----

/** The empty value still has one canonical empty leaf. */
function leafRanges(kind: LargeValueKind, bytes: Bytes): Range[] {
  if (!bytes.length) return [{ start: 0, end: 0 }];
  const isText = kind !== "Bytes";
  if (isText && !isUtf8(bytes)) throw err("InvalidUtf8");
  return cdcRanges(
    bytes.length,
    LEAF_MAX_BYTES,
    (h, i) => roll(h, bytes[i]),
    leafCut,
    (start, end) => {
      if (isText) {
        while (end > start && !isCharBoundary(bytes, end)) end--;
        if (end === start) throw err("MalformedNode");
      }
      return end;
    },
  );
}

const branchRanges = (kind: LargeValueKind, nodes: BuiltNode[]): Range[] =>
  cdcRanges(
    nodes.length,
    BRANCH_MAX_CHILDREN,
    (h, i) => rollChild(h, kind, nodes[i]),
    branchCut,
    (_, end) => end,
  );

/** Split items into content-defined groups of at most `max`; `settle` may pull an end back. */
function cdcRanges(
  count: number,
  max: number,
  rollAt: (hash: bigint, index: number) => bigint,
  cut: (hash: bigint, length: number) => boolean,
  settle: (start: number, end: number) => number,
): Range[] {
  const ranges: Range[] = [];
  let start = 0;
  while (start < count) {
    const hardEnd = Math.min(start + max, count);
    let hash = 0n;
    let end = hardEnd;
    for (let i = start; i < hardEnd; i++) {
      hash = rollAt(hash, i);
      if (cut(hash, i - start + 1)) {
        end = i + 1;
        break;
      }
    }
    end = settle(start, end);
    ranges.push({ start, end });
    start = end;
  }
  return ranges;
}

const roll = (hash: bigint, byte: number): bigint => ((hash << 1n) + gear(byte)) & U64_MASK;

function rollChild(hash: bigint, kind: LargeValueKind, node: BuiltNode): bigint {
  for (const b of bindGroupingHash(FORMAT_VERSION, kind, node.structuralHash)) hash = roll(hash, b);
  return hash;
}

function leafCut(hash: bigint, length: number): boolean {
  const mask = length < LEAF_TARGET_BYTES ? V1_FASTCDC_LEAF_SHORT_MASK : V1_FASTCDC_LEAF_LONG_MASK;
  return length >= LEAF_MIN_BYTES && (hash & mask) === 0n;
}

function branchCut(hash: bigint, count: number): boolean {
  const mask =
    count < BRANCH_TARGET_CHILDREN ? V1_GROUPING_BRANCH_SHORT_MASK : V1_GROUPING_BRANCH_LONG_MASK;
  return count >= BRANCH_MIN_CHILDREN && (hash & mask) === 0n;
}

/** V1 gear table: frozen SplitMix64 parameterization over the byte value. */
let GEAR: bigint[] | undefined;
function gear(byte: number): bigint {
  GEAR ??= Array.from({ length: 256 }, (_, b) => {
    let v = (BigInt(b) + 0x9e3779b97f4a7c15n) & U64_MASK;
    v = ((v ^ (v >> 30n)) * 0xbf58476d1ce4e5b9n) & U64_MASK;
    v = ((v ^ (v >> 27n)) * 0x94d049bb133111ebn) & U64_MASK;
    return v ^ (v >> 31n);
  });
  return GEAR[byte];
}
