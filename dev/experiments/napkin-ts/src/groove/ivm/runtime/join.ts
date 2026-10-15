// Join, anti-join, and arrangement maintenance for runtime evaluation.
import { type Bytes, toHex } from "../../../support/bytes.js";
import {
  AsOf,
  FieldRef,
  IvmRuntimeError,
  compositeKeys,
  consolidateDeltas,
  encodeKeyPart,
  expandsToKeyParts,
  integerValue,
  resolveFieldName,
  resolveFieldRef,
  subTickCmp,
  type PreparedRecordCopy,
  type RecordDelta,
  type RecordDeltas,
  type RecordDescriptor,
  type SubTick,
  type Value,
} from "../../../_extern/groove-ivm-ops.js";

export type JoinKey = Bytes;
const hk = toHex;

/** A key's signed record multiset: shared base plus touched-record overlay. */
class JoinBucket {
  base = new Map<string, [Bytes, number]>();
  /** `null` weight is a tombstone: zero, not the base weight. */
  overlay = new Map<string, [Bytes, number | null]>();
  totalWeight = 0;
  recordCount = 0;

  clone(): JoinBucket {
    const b = new JoinBucket();
    b.base = this.base;
    b.overlay = new Map(this.overlay);
    b.totalWeight = this.totalWeight;
    b.recordCount = this.recordCount;
    return b;
  }

  addWeight(record: Bytes, delta: number): number {
    const k = hk(record);
    const staged = this.overlay.get(k);
    const previous = (staged ? staged[1] : this.base.get(k)?.[1]) ?? 0;
    const next = previous + delta;
    this.overlay.set(k, [record, next !== 0 ? next : null]);
    this.totalWeight += delta;
    if (previous === 0 && next !== 0) this.recordCount++;
    if (previous !== 0 && next === 0) this.recordCount--;
    return next;
  }

  *entries(): IterableIterator<[Bytes, number]> {
    for (const [k, entry] of this.base) if (!this.overlay.has(k)) yield entry;
    for (const [, [record, weight]] of this.overlay) if (weight !== null) yield [record, weight];
  }

  isEmpty(): boolean {
    return this.recordCount === 0;
  }

  commitOverlay(): void {
    if (this.overlay.size === 0) return;
    const base = new Map(this.base);
    for (const [k, [record, weight]] of this.overlay) {
      if (weight !== null) base.set(k, [record, weight]);
      else base.delete(k);
    }
    this.base = base;
    this.overlay = new Map();
  }
}

type JoinIndex = Map<string, [JoinKey, JoinBucket]>;

export function touchedJoinKeys(
  descriptor: RecordDescriptor,
  fields: string[],
  deltas: RecordDelta[],
  comparison: string,
): Bytes[] {
  return keyedJoinDeltas(descriptor, fields, deltas, comparison).map((d) => d.key);
}

/** Operator-local progress contains keys, never retained record buckets. */
class VisibilityState {
  visible = new Set<string>();
  changes = new Map<string, boolean>();
  consumed?: [SubTick, SubTick];

  contains(key: string): boolean {
    return this.changes.get(key) ?? this.visible.has(key);
  }

  set(key: string, visible: boolean): void {
    if (this.contains(key) !== visible) this.changes.set(key, visible);
  }

  commit(): void {
    if (this.changes.size === 0) return;
    const visible = new Set(this.visible);
    for (const [key, present] of this.changes) {
      if (present) visible.add(key);
      else visible.delete(key);
    }
    this.visible = visible;
    this.changes = new Map();
  }
}

export class JoinState {
  evaluatePrepared(
    left: ArrangementTransition,
    right: ArrangementTransition,
    projection: PreparedRecordCopy,
    mode: string,
  ): RecordDelta[] {
    const output: RecordDelta[] = [];
    if (mode === "Replace") {
      appendJoinIndexDeltas(
        output,
        projection,
        left.current.buckets(),
        (k) => right.current.bucket(k),
        "LeftDelta",
        1,
      );
    } else {
      appendJoinIndexDeltas(
        output,
        projection,
        left.changeBuckets(),
        (k) => right.current.bucket(k),
        "LeftDelta",
        1,
      );
      appendJoinIndexDeltas(
        output,
        projection,
        right.changeBuckets(),
        (k) => left.current.bucket(k),
        "RightDelta",
        1,
      );
      // Both current indexes include this transition: remove the repeated cross term.
      const leftChanges = left.changes;
      if (leftChanges) {
        appendJoinIndexDeltas(
          output,
          projection,
          right.changeBuckets(),
          (k) => leftChanges.get(k)?.[1],
          "RightDelta",
          -1,
        );
      }
    }
    return consolidateDeltas(output);
  }
}

/** A consumer borrows one aligned input version and its producer's delta. */
export class ArrangementTransition {
  private constructor(
    readonly current: ArrangementState,
    readonly changes: JoinIndex | undefined,
    readonly frontier: SubTick,
  ) {}

  static at(
    state: any /* AsOf<ArrangementState, SubTick> */,
    expected: SubTick,
  ): ArrangementTransition {
    const current: ArrangementState = state.value;
    const fresh = state.asOf !== undefined && subTickCmp(state.asOf, expected) === 0;
    return new ArrangementTransition(current, fresh ? current.changes : undefined, expected);
  }

  changedKeys(): string[] {
    return this.changes ? [...this.changes.keys()] : [];
  }

  *changeBuckets(): IterableIterator<[string, JoinBucket]> {
    if (this.changes) for (const [k, [, bucket]] of this.changes) yield [k, bucket];
  }

  deltaBucket(key: string): JoinBucket | undefined {
    return this.changes?.get(key)?.[1];
  }
}

function thresholdTransition(
  published: VisibilityState,
  left: ArrangementTransition,
  right: ArrangementTransition,
  replace: boolean,
  semi: boolean,
): RecordDelta[] {
  const frontier: [SubTick, SubTick] = [left.frontier, right.frontier];
  const consumed = published.consumed;
  if (
    !replace &&
    consumed &&
    subTickCmp(consumed[0], frontier[0]) === 0 &&
    subTickCmp(consumed[1], frontier[1]) === 0
  ) {
    return [];
  }
  const affected = new Set<string>();
  if (replace) {
    published.visible = new Set();
    published.changes = new Map();
    for (const k of left.current.index.keys()) affected.add(k);
    for (const k of left.current.overlay.keys()) affected.add(k);
  } else {
    for (const k of left.changedKeys()) affected.add(k);
    for (const k of right.changedKeys()) affected.add(k);
  }
  const output: RecordDelta[] = [];
  for (const key of affected) {
    const before = !replace && published.contains(key);
    const count = right.current.keyCount(key);
    const after = semi ? count > 0 : count === 0;
    if (before && after) appendBucket(output, left.deltaBucket(key), 1);
    else if (!before && after) appendBucket(output, left.current.bucket(key), 1);
    else if (before && !after) {
      // -L_before = -L_after + delta_L, without retaining L_before.
      appendBucket(output, left.current.bucket(key), -1);
      appendBucket(output, left.deltaBucket(key), 1);
    }
    published.set(key, after && left.current.bucket(key) !== undefined);
  }
  published.consumed = frontier;
  return consolidateDeltas(output);
}

/** Two temporary snapshot arrangements through the same read-only kernel. */
export function thresholdSnapshot(
  left: RecordDeltas,
  right: RecordDeltas,
  leftKeys: string[],
  rightKeys: string[],
  comparison: string,
  semi: boolean,
): RecordDelta[] {
  const stamp = { tick: 0, subTick: 0 };
  const l = new AsOf(new ArrangementState());
  const r = new AsOf(new ArrangementState());
  prepareArrangement(
    l,
    left.descriptor,
    leftKeys,
    comparison,
    JoinInput.deltas(left.deltas),
    stamp,
    "Replace",
  );
  prepareArrangement(
    r,
    right.descriptor,
    rightKeys,
    comparison,
    JoinInput.deltas(right.deltas),
    stamp,
    "Replace",
  );
  return thresholdTransition(
    new VisibilityState(),
    ArrangementTransition.at(l, stamp),
    ArrangementTransition.at(r, stamp),
    true,
    semi,
  );
}

/** Published visibility of a semi join or an anti join. */
export class ThresholdJoinState {
  private published = new VisibilityState();
  constructor(readonly semi: boolean) {}

  commitPublishedOverlay(): void {
    this.published.commit();
  }

  evaluate(left: ArrangementTransition, right: ArrangementTransition, mode: string): RecordDelta[] {
    return thresholdTransition(this.published, left, right, mode === "Replace", this.semi);
  }
}

export class JoinInput {
  private constructor(
    readonly records: RecordDelta[],
    readonly snapshot?: RecordDeltas,
  ) {}
  static deltas(records: RecordDelta[]): JoinInput {
    return new JoinInput(records);
  }
  static snapshot(records: RecordDeltas): JoinInput {
    return new JoinInput(records.deltas, records);
  }
}

interface KeyedRecordDelta {
  delta: RecordDelta;
  key: JoinKey;
}

export class ArrangementState {
  /** Immutable base buckets shared between evaluator snapshots. */
  index: JoinIndex = new Map();
  overlay = new Map<string, [JoinKey, JoinBucket | null]>();
  /** One producer-owned transition, shared read-only by all consumers. */
  changes?: JoinIndex;
  /** Identity of the last complete immutable input, not a retained payload. */
  snapshot?: WeakRef<RecordDeltas>;

  *buckets(): IterableIterator<[string, JoinBucket]> {
    for (const [k, [, bucket]] of this.index) if (!this.overlay.has(k)) yield [k, bucket];
    for (const [k, [, bucket]] of this.overlay) if (bucket) yield [k, bucket];
  }

  releaseChanges(): void {
    this.changes = undefined;
  }

  /** Fold only the touched buckets into the shared base at tick commit. */
  commitOverlay(): void {
    this.changes = undefined;
    if (this.overlay.size === 0) return;
    const index = new Map(this.index);
    for (const [k, [key, bucket]] of this.overlay) {
      index.delete(k);
      if (bucket) {
        bucket.commitOverlay();
        index.set(k, [key, bucket]);
      }
    }
    this.index = index;
    this.overlay = new Map();
  }

  rowCount(): number {
    let n = 0;
    for (const [, bucket] of this.buckets()) for (const [, w] of bucket.entries()) if (w !== 0) n++;
    return n;
  }

  encodedBytes(): number {
    let n = 0;
    for (const [k, bucket] of this.buckets()) {
      n += k.length / 2;
      for (const [record] of bucket.entries()) n += record.length;
    }
    return n;
  }

  applyUpdate(deltas: KeyedRecordDelta[], mode: string): void {
    this.snapshot = undefined;
    // An empty batch advances the timestamp but retains no transition.
    this.changes = deltas.length ? buildJoinDeltaIndex(deltas) : undefined;
    if (mode === "Replace") {
      this.index = this.changes ?? new Map();
      this.overlay = new Map();
      return;
    }
    for (const delta of deltas) {
      const k = hk(delta.key);
      let slot = this.overlay.get(k);
      if (!slot) {
        // Clone a base bucket only on first touch; a tombstone stays absent.
        slot = [delta.key, this.index.get(k)?.[1].clone() ?? null];
        this.overlay.set(k, slot);
      }
      const bucket = (slot[1] ??= new JoinBucket());
      bucket.addWeight(delta.delta.record, delta.delta.weight);
      if (bucket.isEmpty()) slot[1] = null;
    }
  }

  keyCount(key: string): number {
    return this.bucket(key)?.totalWeight ?? 0;
  }

  bucket(key: string): JoinBucket | undefined {
    const staged = this.overlay.get(key);
    if (staged) return staged[1] ?? undefined;
    return this.index.get(key)?.[1];
  }

  applyRecordDeltas(
    descriptor: RecordDescriptor,
    fields: string[],
    deltas: RecordDelta[],
    mode: string,
  ): void {
    this.applyUpdate(keyedJoinDeltas(descriptor, fields, deltas, "Exact"), mode);
  }

  recordsForKey(key: Bytes): [Bytes, number][] {
    const bucket = this.bucket(hk(key));
    return bucket ? [...bucket.entries()].filter(([, w]) => w > 0) : [];
  }
}

function reusesSnapshot(
  arrangement: any,
  snapshot: RecordDeltas | undefined,
  subTick: SubTick,
  mode: string,
): boolean {
  return (
    mode === "Replace" &&
    arrangement.asOf !== undefined &&
    subTickCmp(arrangement.asOf, subTick) === 0 &&
    snapshot !== undefined &&
    arrangement.value.snapshot?.deref() === snapshot
  );
}

export function prepareArrangement(
  state: any,
  descriptor: RecordDescriptor,
  fields: string[],
  comparison: string,
  input: JoinInput,
  stamp: SubTick,
  mode: string,
): void {
  if (reusesSnapshot(state, input.snapshot, stamp, mode)) return;
  advanceArrangement(
    state,
    keyedJoinDeltas(descriptor, fields, input.records, comparison),
    stamp,
    mode,
    input.snapshot,
  );
}

function advanceArrangement(
  arrangement: any,
  deltas: KeyedRecordDelta[],
  subTick: SubTick,
  mode: string,
  snapshot: RecordDeltas | undefined,
): void {
  if (reusesSnapshot(arrangement, snapshot, subTick, mode)) return;
  const asOf: SubTick | undefined = arrangement.asOf;
  if (mode === "Accumulate" && asOf && subTickCmp(asOf, subTick) === 0) return;
  // Without exact snapshot identity, Replace callers rebuild even at the same logical time.
  const replaceWithinSameTick =
    mode === "Replace" && asOf !== undefined && asOf.tick === subTick.tick;
  if (!replaceWithinSameTick && asOf && subTickCmp(asOf, subTick) > 0) {
    throw new IvmRuntimeError("OutOfOrderRuntimeState", {
      current: JSON.stringify(asOf),
      next: JSON.stringify(subTick),
    });
  }
  const state: ArrangementState = arrangement.value;
  state.applyUpdate(deltas, mode);
  if (mode === "Replace") state.snapshot = snapshot ? new WeakRef(snapshot) : undefined;
  if (replaceWithinSameTick) arrangement.replaceAsOfAtLeast(subTick);
  else arrangement.markForwardAsOf(subTick);
}

function appendJoinIndexDeltas(
  output: RecordDelta[],
  projection: PreparedRecordCopy,
  changes: Iterable<[string, JoinBucket]>,
  stored: (key: string) => JoinBucket | undefined,
  side: "LeftDelta" | "RightDelta",
  sign: number,
): void {
  for (const [key, changed] of changes) {
    const bucket = stored(key);
    if (!bucket) continue;
    for (const [changedRecord, changedWeight] of changed.entries()) {
      for (const [storedRecord, storedWeight] of bucket.entries()) {
        const weight = sign * changedWeight * storedWeight;
        if (weight === 0) continue;
        const pair =
          side === "LeftDelta" ? [changedRecord, storedRecord] : [storedRecord, changedRecord];
        output.push({ record: projection.project(pair), weight });
      }
    }
  }
}

function buildJoinDeltaIndex(deltas: KeyedRecordDelta[]): JoinIndex {
  const index: JoinIndex = new Map();
  for (const delta of deltas) {
    const k = hk(delta.key);
    let entry = index.get(k);
    if (!entry) index.set(k, (entry = [delta.key, new JoinBucket()]));
    const next = entry[1].addWeight(delta.delta.record, delta.delta.weight);
    if (next === 0 && entry[1].isEmpty()) index.delete(k);
  }
  for (const [, [, bucket]] of index) bucket.commitOverlay();
  return index;
}

function keyedJoinDeltas(
  descriptor: RecordDescriptor,
  fields: string[],
  deltas: RecordDelta[],
  comparison: string,
): KeyedRecordDelta[] {
  const fieldIndices = scalarJoinFieldIndices(descriptor, fields);
  const keyed: KeyedRecordDelta[] = [];
  if (fieldIndices) {
    for (const delta of deltas) {
      const key: number[] = [];
      for (const fieldIdx of fieldIndices)
        encodeJoinKeyPart(key, descriptor.getIdx(delta.record, fieldIdx), comparison);
      keyed.push({ delta, key: Uint8Array.from(key) });
    }
    return keyed;
  }
  for (const delta of deltas) {
    for (const key of joinKeys(descriptor, delta.record, fields, comparison))
      keyed.push({ delta, key });
  }
  return keyed;
}

function scalarJoinFieldIndices(
  descriptor: RecordDescriptor,
  fields: string[],
): number[] | undefined {
  const indices: number[] = [];
  for (const field of fields) {
    const fieldIdx = resolveFieldName(descriptor, field);
    if (fieldIdx === undefined) throw new IvmRuntimeError("GraphFieldNotFound", field);
    const descriptorField = descriptor.fields()[fieldIdx];
    if (!descriptorField) throw new IvmRuntimeError("GraphFieldIndexOutOfBounds", fieldIdx);
    if (expandsToKeyParts(descriptorField.valueType)) return undefined;
    indices.push(fieldIdx);
  }
  return indices;
}

function appendBucket(deltas: RecordDelta[], bucket: JoinBucket | undefined, sign: number): void {
  if (!bucket) return;
  for (const [record, weight] of bucket.entries()) {
    if (sign * weight !== 0) deltas.push({ record, weight: sign * weight });
  }
}

export function joinKeys(
  descriptor: RecordDescriptor,
  record: Bytes,
  fields: string[],
  comparison: string,
): JoinKey[] {
  const values = fields.map((field) => {
    const fieldIdx = resolveFieldName(descriptor, field);
    if (fieldIdx === undefined) throw new IvmRuntimeError("GraphFieldNotFound", field);
    return descriptor.bind(record).getIdx(fieldIdx);
  });
  return compositeKeys([], values, (key, value) => encodeJoinKeyPart(key, value, comparison)).map(
    (k) => Uint8Array.from(k),
  );
}

/** Encode a join key with the requested comparison semantics. */
function encodeJoinKeyPart(key: number[], value: Value, comparison: string): void {
  if (comparison === "Policy") {
    const integer = integerValue(value);
    if (integer !== undefined) {
      key.push(0xfe);
      const be = new DataView(new ArrayBuffer(16));
      be.setBigInt64(0, integer >> 64n);
      be.setBigUint64(8, integer & 0xffffffffffffffffn);
      key.push(...new Uint8Array(be.buffer));
      return;
    }
    if (value.kind === "Nullable" && value.value !== undefined)
      return encodeJoinKeyPart(key, value.value, comparison);
  }
  encodeKeyPart(key, value);
}

export function createJoinRecord(
  leftDescriptor: RecordDescriptor,
  leftRecord: Bytes,
  rightDescriptor: RecordDescriptor,
  rightRecord: Bytes,
  outputDescriptor: RecordDescriptor,
): Bytes {
  const mapping = joinOutputMapping(leftDescriptor, rightDescriptor, outputDescriptor);
  return outputDescriptor.projectRecordRaw(
    [leftDescriptor, rightDescriptor],
    [leftRecord, rightRecord],
    mapping,
  );
}

export function joinOutputMapping(
  leftDescriptor: RecordDescriptor,
  rightDescriptor: RecordDescriptor,
  outputDescriptor: RecordDescriptor,
): [number, number][] {
  return outputDescriptor.fields().map((field: any): [number, number] => {
    const name: string | undefined = field.name;
    if (name === undefined) throw new IvmRuntimeError("GraphFieldNotFound", "<unnamed>");
    if (name.startsWith("left."))
      return [0, resolveFieldRef(leftDescriptor, FieldRef.storedName(name.slice(5)))];
    if (name.startsWith("right."))
      return [1, resolveFieldRef(rightDescriptor, FieldRef.storedName(name.slice(6)))];
    throw new IvmRuntimeError("GraphFieldNotFound", name);
  });
}
