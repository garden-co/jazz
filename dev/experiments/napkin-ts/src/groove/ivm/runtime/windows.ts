// Arg-by, TopBy, and structured CollectBy window maintenance.
import { type Bytes, compareBytes, equalBytes, toHex } from "../../../support/bytes.js";
import { SortedMap, SortedSet, cmpBy } from "../../../support/collections.js";
import {
  BorrowedRecord,
  IvmRuntimeError,
  RecordDescriptor as RecordDescriptorCtor,
  collectByOrderedScalar,
  encodeKeyPart,
  encodeOrderedBytes,
  encodeRecordDescriptor,
  encodeScalarKeyPart,
  isSqlNullValue,
  type CollectByOp,
  type CollectBySlot,
  type RecordDelta,
  type RecordDescriptor,
  type TerminalOperation,
  type TopByOp,
  type Value,
  type ValueType,
} from "../../../_extern/groove-ivm-ops.js";
import { RankIndex } from "./rank_index.js";
import {
  type CollectByGroup,
  type CollectByIncrementalState,
  type CollectByOrderKey,
  cmpOrderKey,
} from "./evaluator.js";

export type WindowedRecord = [Bytes, number];

/** Ordered sort-key variants; variant order first, then value. */
type TopBySortKey =
  | { kind: "Null" }
  | { kind: "Num"; rank: number; value: number | bigint }
  | { kind: "Bool"; value: boolean }
  | { kind: "String"; value: string }
  | { kind: "Bytes"; value: Bytes }
  | { kind: "Uuid"; value: Bytes }
  | { kind: "EnumTag"; value: number };

const KEY_RANK: Record<string, number> = {
  Null: 0,
  Num: 1,
  Bool: 9,
  String: 10,
  Bytes: 11,
  Uuid: 12,
  EnumTag: 13,
};

function cmpSortKey(a: TopBySortKey, b: TopBySortKey): number {
  const ra = a.kind === "Num" ? 1 + a.rank : KEY_RANK[a.kind];
  const rb = b.kind === "Num" ? 1 + b.rank : KEY_RANK[b.kind];
  if (ra !== rb) return ra - rb;
  switch (a.kind) {
    case "Null":
      return 0;
    case "Num": {
      const bv = (b as typeof a).value;
      return a.value < bv ? -1 : a.value > bv ? 1 : 0;
    }
    case "Bool":
      return Number(a.value) - Number((b as typeof a).value);
    case "String": {
      const bv = (b as typeof a).value;
      return a.value < bv ? -1 : a.value > bv ? 1 : 0;
    }
    case "Bytes":
    case "Uuid":
      return compareBytes(a.value, (b as typeof a).value);
    case "EnumTag":
      return a.value - (b as typeof a).value;
  }
}

export interface TopBySortPart {
  key: TopBySortKey;
  direction: "Asc" | "Desc";
}

export function cmpTopBySortParts(a: TopBySortPart[], b: TopBySortPart[]): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const c = cmpSortKey(a[i].key, b[i].key);
    if (c !== 0) return a[i].direction === "Desc" ? -c : c;
  }
  return a.length - b.length;
}

export type ArgByDirection = "Min" | "Max";

export function argByCandidateReplaces(
  candidateKey: Bytes,
  candidateRecord: Bytes,
  winnerKey: Bytes,
  winnerRecord: Bytes,
  direction: ArgByDirection,
): boolean {
  const c = compareBytes(candidateKey, winnerKey);
  if (c < 0) return direction === "Min";
  if (c > 0) return direction === "Max";
  return compareBytes(candidateRecord, winnerRecord) < 0;
}

export interface ArgBySpec {
  groupFieldIndices: number[];
  comparisonFieldIndices: number[];
  direction: ArgByDirection;
}

/** Only the declared key reverses for Max; the record tie-breaker stays ascending. */
export function argByOrderKey(
  descriptor: RecordDescriptor,
  delta: RecordDelta,
  spec: ArgBySpec,
): CollectByOrderKey {
  return [
    [
      {
        key: {
          kind: "Bytes",
          value: encodedRecordKeyPart(descriptor, delta.record, spec.comparisonFieldIndices),
        },
        direction: spec.direction === "Min" ? "Asc" : "Desc",
      },
    ],
    delta.record,
  ];
}

export function argByOrderedWinner(group: CollectByGroup): Bytes | undefined {
  for (const [key, weight] of group) if (weight > 0) return key[1];
  return undefined;
}

/** Bag semantics (INV-QUERY-24/25): offset/limit consume copies, not distinct records. */
export function topByWindowFromOrderedGroup(
  records: CollectByGroup | undefined,
  topBy: TopByOp,
): WindowedRecord[] {
  const window: WindowedRecord[] = [];
  let toSkip: number = topBy.offset;
  let remaining: number | undefined = topBy.limit.kind === "Finite" ? topBy.limit.value : undefined;
  if (!records) return window;
  for (const [[, record], weight] of records) {
    if (remaining === 0) break;
    if (weight <= 0) continue;
    const available = Math.max(0, weight - toSkip);
    toSkip = Math.max(0, toSkip - weight);
    const taken = remaining === undefined ? available : Math.min(available, remaining);
    if (taken > 0) {
      window.push([record, taken]);
      if (remaining !== undefined) remaining -= taken;
    }
  }
  return window;
}

export function recordsBeforeDeltas(
  afterRecords: WindowedRecord[],
  deltas: RecordDelta[],
): WindowedRecord[] {
  const records = new SortedMap<Bytes, number>(compareBytes);
  for (const [record, weight] of afterRecords)
    records.set(record, (records.get(record) ?? 0) + weight);
  for (const delta of deltas)
    records.set(delta.record, (records.get(delta.record) ?? 0) - delta.weight);
  return [...records];
}

function collectByProjectionValueType(inputDesc: RecordDescriptor, field: any): ValueType {
  const descField = inputDesc.fields()[field.fieldIdx];
  if (!descField) throw new IvmRuntimeError("GraphFieldIndexOutOfBounds", field.fieldIdx);
  const ty = descField.valueType;
  return field.unwrapNullable && ty.kind === "Nullable" ? ty.inner : ty;
}

export function collectByOutputValue(outputType: ValueType, value: Value): Value {
  if (outputType.kind === "Nullable" && value.kind !== "Nullable")
    return { kind: "Nullable", value };
  return value;
}

function collectByProjectedValue(record: BorrowedRecord, field: any): Value {
  if (field.fieldIdx >= record.descriptor().fields().length) {
    throw new IvmRuntimeError("GraphFieldIndexOutOfBounds", field.fieldIdx);
  }
  const value = record.getIdx(field.fieldIdx);
  if (!field.unwrapNullable || value.kind !== "Nullable") return value;
  // A present child can carry an application NULL: keep it saturated at NULL.
  return value.value ?? { kind: "Nullable", value: undefined };
}

const keySet = () => new SortedSet<Bytes>(compareBytes);

function insertOp(
  rootDescriptor: RecordDescriptor,
  rootKey: Bytes,
  path: any[],
  index: number,
  key: Bytes,
  value: Bytes,
): TerminalOperation {
  return { rootDescriptor, rootKey, path, edit: { kind: "Insert", index, key, value } };
}

function removeOp(
  rootDescriptor: RecordDescriptor,
  rootKey: Bytes,
  path: any[],
  key: Bytes,
): TerminalOperation {
  return { rootDescriptor, rootKey, path, edit: { kind: "Remove", key } };
}

export function updateUnboundedCollectByTerminalState(
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
  collectBy: CollectByOp,
  directTreeSlot: CollectBySlot | undefined,
  state: CollectByIncrementalState,
  rawDeltas: RecordDelta[],
  emit: boolean,
): TerminalOperation[] {
  if (collectBy.mode === "Root") {
    return updateCollectByRootTerminalState(
      inputDesc,
      outputDesc,
      collectBy,
      state,
      rawDeltas,
      emit,
    );
  }
  if (!emit) {
    state.groups.clear();
    state.roots.clear();
  }
  // Consolidate exact records; net removals first, then net additions.
  const deltas = canonicalCollectByTerminalDeltas(inputDesc, collectBy, directTreeSlot, rawDeltas);
  // Only a group named by this batch can enter or leave the terminal.
  const touchedGroupKeys = keySet();
  for (const delta of deltas)
    touchedGroupKeys.add(
      encodedRecordKeyPart(inputDesc, delta.record, collectBy.groupFieldIndices),
    );
  const rootGroupsBefore = directTreeSlot
    ? undefined
    : new SortedSet(
        compareBytes,
        touchedGroupKeys.toArray().filter((k) => state.groups.get(k) !== undefined),
      );
  const operations: TerminalOperation[] = [];
  for (const delta of deltas) {
    const groupKey = encodedRecordKeyPart(inputDesc, delta.record, collectBy.groupFieldIndices);
    const presenceField = directTreeSlot?.presenceFieldIndex;
    if (
      presenceField !== undefined &&
      !new BorrowedRecord(delta.record, inputDesc).getBool(presenceField)
    ) {
      const stateKey: CollectByOrderKey = [
        collectBySortKey(inputDesc, delta.record, collectBy),
        delta.record,
      ];
      const beforeWeight = state.roots.get(stateKey) ?? 0;
      const afterWeight = beforeWeight + delta.weight;
      if (afterWeight === 0) state.roots.delete(stateKey);
      else state.roots.set(stateKey, afterWeight);
      if (!emit || beforeWeight > 0 === afterWeight > 0) continue;
      if (afterWeight > 0) {
        let index = 0;
        for (const [, w] of state.roots.range(undefined, stateKey)) if (w > 0) index++;
        const record = collectByTreeParentFromRecords(inputDesc, outputDesc, collectBy, [
          [delta.record, 1],
        ]);
        if (!record)
          throw new IvmRuntimeError(
            "InvalidCollectBy",
            "root anchor did not render a terminal row",
          );
        operations.push(insertOp(outputDesc, groupKey, [], index, groupKey, record));
      } else {
        operations.push(removeOp(outputDesc, groupKey, [], groupKey));
      }
      continue;
    }
    const sortFieldIndices = directTreeSlot?.sortFieldIndices ?? collectBy.sortFieldIndices;
    const sortDirections = directTreeSlot?.sortDirections ?? collectBy.sortDirections;
    const stateKey: CollectByOrderKey = [
      collectBySortKeyForFields(inputDesc, delta.record, sortFieldIndices, sortDirections),
      delta.record,
    ];
    const group = state.groups.getOrDefault(groupKey);
    const beforeWeight = group.get(stateKey) ?? 0;
    const afterWeight = beforeWeight + delta.weight;
    group.set(stateKey, afterWeight);
    if (!emit || beforeWeight > 0 === afterWeight > 0) continue;
    // A group entering the terminal in this batch is rendered whole below.
    if (rootGroupsBefore && !rootGroupsBefore.has(groupKey)) continue;
    const sourceInput = new BorrowedRecord(delta.record, inputDesc);
    const childFields = directTreeSlot?.childFields ?? collectBy.childFields;
    const childDescriptor = directTreeSlot?.childDescriptor ?? collectBy.childDescriptor;
    const collectionField = directTreeSlot?.collectionField ?? collectBy.collectionField;
    const childValues = childFields.map((field: any, index: number) =>
      collectByOutputValue(
        childDescriptor.fields()[index].valueType,
        collectByProjectedValue(sourceInput, field),
      ),
    );
    const childRecord: Bytes = childDescriptor.create(childValues);
    const childKey = encodedRecordKeyPart(childDescriptor, childRecord, [0]);
    const path = [{ kind: "Collection", name: collectionField }];
    if (afterWeight > 0) {
      operations.push(
        insertOp(outputDesc, groupKey, path, group.countBefore(stateKey), childKey, childRecord),
      );
    } else {
      operations.push(removeOp(outputDesc, groupKey, path, childKey));
    }
  }
  state.groups.removeEmptyTouchedGroups(touchedGroupKeys);
  if (rootGroupsBefore) {
    const rootGroupsAfter = new SortedSet(
      compareBytes,
      touchedGroupKeys.toArray().filter((k) => state.groups.get(k) !== undefined),
    );
    const kept = operations.filter(
      (op) => rootGroupsBefore.has(op.rootKey) && rootGroupsAfter.has(op.rootKey),
    );
    operations.length = 0;
    operations.push(...kept);
    for (const rootKey of rootGroupsBefore) {
      if (!rootGroupsAfter.has(rootKey))
        operations.push(removeOp(outputDesc, rootKey, [], rootKey));
    }
    const insertedRoots = rootGroupsAfter.toArray().filter((k) => !rootGroupsBefore.has(k));
    // Rank against the complete merged group index in one walk.
    const rootRanks = state.groups.countBeforeEach(insertedRoots);
    insertedRoots.forEach((rootKey, i) => {
      const group = state.groups.get(rootKey)!;
      const records: WindowedRecord[] = [...group].map(([[, record], weight]) => [record, weight]);
      const record = collectByParentFromRecords(inputDesc, outputDesc, collectBy, records);
      if (!record)
        throw new IvmRuntimeError(
          "InvalidCollectBy",
          "new collect root did not render a terminal row",
        );
      operations.push(insertOp(outputDesc, rootKey, [], rootRanks[i], rootKey, record));
    });
  }
  return operations;
}

type Keyed = [Bytes, TopBySortPart[], Bytes, number];

function canonicalCollectByTerminalDeltas(
  inputDesc: RecordDescriptor,
  collectBy: CollectByOp,
  directTreeSlot: CollectBySlot | undefined,
  deltas: RecordDelta[],
): RecordDelta[] {
  const sortFieldIndices = directTreeSlot?.sortFieldIndices ?? collectBy.sortFieldIndices;
  const sortDirections = directTreeSlot?.sortDirections ?? collectBy.sortDirections;
  const keyed: Keyed[] = deltas.map((delta) => [
    encodedRecordKeyPart(inputDesc, delta.record, collectBy.groupFieldIndices),
    collectBySortKeyForFields(inputDesc, delta.record, sortFieldIndices, sortDirections),
    delta.record,
    delta.weight,
  ]);
  let canonical = canonicalizeCollectByTerminalWeights(keyed);
  const presence = directTreeSlot?.presenceFieldIndex;
  if (presence !== undefined) {
    // Retire children before their parent, then create the new parent before its children.
    const ordered = canonical.map((entry): [Keyed, boolean] => {
      const isChild = new BorrowedRecord(entry[2], inputDesc).getBool(presence);
      return [entry, entry[3] > 0 ? isChild : !isChild];
    });
    ordered.sort(
      ([l, lp], [r, rp]) =>
        compareBytes(l[0], r[0]) || Number(l[3] > 0) - Number(r[3] > 0) || Number(lp) - Number(rp),
    );
    canonical = ordered.map(([entry]) => entry);
  }
  return canonical.map(([, , record, weight]) => ({ record, weight }));
}

export function canonicalizeCollectByTerminalWeights(keyed: Keyed[]): Keyed[] {
  const cmpKey = (a: Keyed, b: Keyed) =>
    compareBytes(a[0], b[0]) || cmpTopBySortParts(a[1], b[1]) || compareBytes(a[2], b[2]);
  const consolidated = new SortedMap<Keyed, number>(cmpKey);
  for (const entry of keyed) consolidated.set(entry, (consolidated.get(entry) ?? 0) + entry[3]);
  const canonical: Keyed[] = [];
  for (const [[g, s, r], w] of consolidated) if (w !== 0) canonical.push([g, s, r, w]);
  canonical.sort(
    (l, r) =>
      compareBytes(l[0], r[0]) ||
      Number(l[3] > 0) - Number(r[3] > 0) ||
      cmpTopBySortParts(l[1], r[1]),
  );
  return canonical;
}

const groupRecords = (group: CollectByGroup): WindowedRecord[] =>
  [...group].map(([[, record], weight]) => [record, weight]);
const sameKey = (a: CollectByOrderKey | undefined, b: CollectByOrderKey | undefined) =>
  a === b || (a !== undefined && b !== undefined && cmpOrderKey(a, b) === 0);

function updateCollectByRootTerminalState(
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
  collectBy: CollectByOp,
  state: CollectByIncrementalState,
  deltas: RecordDelta[],
  emit: boolean,
): TerminalOperation[] {
  if (!emit) {
    state.groups.clear();
    state.roots.clear();
    state.emittedRootOrder = new RankIndex(cmpOrderKey);
    state.emittedRootKeys = new RankIndex(compareBytes);
  }
  const uniqueGroupSort = collectBySortIdentifiesGroup(inputDesc, collectBy);
  const orderKey = (group: CollectByGroup | undefined) =>
    group ? collectByRootOrderKey(group, uniqueGroupSort) : undefined;
  const before = new SortedMap<Bytes, Bytes | undefined>(compareBytes);
  const beforeOrder = new SortedMap<Bytes, CollectByOrderKey | undefined>(compareBytes);
  for (const delta of deltas) {
    const groupKey = encodedRecordKeyPart(inputDesc, delta.record, collectBy.groupFieldIndices);
    if (emit && !before.has(groupKey)) {
      const group = state.groups.get(groupKey);
      before.set(
        groupKey,
        group
          ? collectByRootFromRecords(inputDesc, outputDesc, collectBy, groupRecords(group))
          : undefined,
      );
    }
    if (!beforeOrder.has(groupKey)) beforeOrder.set(groupKey, orderKey(state.groups.get(groupKey)));
    const stateKey: CollectByOrderKey = [
      collectBySortKey(inputDesc, delta.record, collectBy),
      delta.record,
    ];
    const group = state.groups.getOrDefault(groupKey);
    group.set(stateKey, (group.get(stateKey) ?? 0) + delta.weight);
  }
  state.groups.removeEmptyTouchedGroups(before.keysArray());
  if (!emit) return [];
  // Unchanged total order keys: no public position can change.
  if (
    [...beforeOrder].every(([rootKey, beforeKey]) =>
      sameKey(beforeKey, orderKey(state.groups.get(rootKey))),
    )
  ) {
    return collectByRootPayloadUpdates(inputDesc, outputDesc, collectBy, state, before);
  }
  const operations: TerminalOperation[] = [];
  // Remove first so later positional inserts/moves index the final sequence.
  for (const [rootKey, beforeKey] of beforeOrder) {
    if (
      state.emittedRootKeys.contains(rootKey) &&
      orderKey(state.groups.get(rootKey)) === undefined
    ) {
      operations.push(removeOp(outputDesc, rootKey, [], rootKey));
      if (beforeKey) state.emittedRootOrder.remove(beforeKey);
      state.emittedRootKeys.remove(rootKey);
    }
  }
  // Unprocessed moves keep OLD keys; processed moves have NEW keys.
  const moves: [CollectByOrderKey, CollectByOrderKey, Bytes][] = [];
  for (const [rootKey, beforeKey] of beforeOrder) {
    if (!beforeKey || !before.get(rootKey) || !state.emittedRootKeys.contains(rootKey)) continue;
    const afterKey = orderKey(state.groups.get(rootKey));
    if (afterKey && !sameKey(beforeKey, afterKey)) moves.push([afterKey, beforeKey, rootKey]);
  }
  moves.sort((l, r) => cmpOrderKey(l[0], r[0]));
  for (const [afterKey, beforeKey, rootKey] of moves) {
    const order = state.emittedRootOrder;
    const currentIndex = order.rank(beforeKey);
    order.remove(beforeKey);
    const index = order.rank(afterKey);
    order.insert(afterKey, rootKey);
    if (currentIndex !== index) {
      operations.push({
        rootDescriptor: outputDesc,
        rootKey,
        path: [],
        edit: { kind: "Move", key: rootKey, index },
      });
    }
  }
  const inserts: [CollectByOrderKey, Bytes, Bytes][] = [];
  for (const [rootKey, beforeKey] of beforeOrder) {
    if (beforeKey) continue;
    const group = state.groups.get(rootKey);
    const key = orderKey(group);
    // A freshly reset collector keeps negative maintenance state without a public occurrence.
    if (!key) continue;
    const record =
      group && collectByRootFromRecords(inputDesc, outputDesc, collectBy, groupRecords(group));
    if (!record)
      throw new IvmRuntimeError(
        "InvalidCollectBy",
        "new root collector group did not render a terminal row",
      );
    inserts.push([key, rootKey, record]);
  }
  inserts.sort((l, r) => cmpOrderKey(l[0], r[0]));
  for (const [key, rootKey, record] of inserts) {
    const index = state.emittedRootOrder.rank(key);
    state.emittedRootOrder.insert(key, rootKey);
    state.emittedRootKeys.insert(rootKey, null);
    operations.push(insertOp(outputDesc, rootKey, [], index, rootKey, record));
  }
  operations.push(...collectByRootPayloadUpdates(inputDesc, outputDesc, collectBy, state, before));
  return operations;
}

function collectByRootPayloadUpdates(
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
  collectBy: CollectByOp,
  state: CollectByIncrementalState,
  before: SortedMap<Bytes, Bytes | undefined>,
): TerminalOperation[] {
  const operations: TerminalOperation[] = [];
  for (const [rootKey, beforeRecord] of before) {
    // New roots already emitted an Insert; un-emitted maintenance groups have no occurrence.
    if (!beforeRecord || !state.emittedRootKeys.contains(rootKey)) continue;
    const group = state.groups.get(rootKey);
    const afterRecord =
      group && collectByRootFromRecords(inputDesc, outputDesc, collectBy, groupRecords(group));
    if (!afterRecord || equalBytes(beforeRecord, afterRecord)) continue;
    operations.push({
      rootDescriptor: outputDesc,
      rootKey,
      path: [],
      edit: { kind: "Update", key: rootKey, value: afterRecord },
    });
  }
  return operations;
}

function collectBySortIdentifiesGroup(
  inputDesc: RecordDescriptor,
  collectBy: CollectByOp,
): boolean {
  return collectBy.groupFieldIndices.every((groupField: number) => {
    const consumed = collectBy.sortFieldIndices
      .slice(0, collectBy.sortDirections.length)
      .some((sortField: number) => sortField === groupField);
    const kind = inputDesc.fields()[groupField]?.valueType.kind;
    return consumed && (kind === "Uuid" || kind === "U64");
  });
}

function collectByRootOrderKey(
  group: CollectByGroup,
  uniqueGroupSort: boolean,
): CollectByOrderKey | undefined {
  for (const [[sortKey, record], weight] of group) {
    if (weight > 0) return [sortKey, uniqueGroupSort ? new Uint8Array() : record];
  }
  return undefined;
}

function parentValues(
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
  collectBy: CollectByOp,
  parent: Bytes,
): Value[] {
  const input = new BorrowedRecord(parent, inputDesc);
  return collectBy.parentFields.map((field: any, index: number) =>
    collectByOutputValue(
      outputDesc.fields()[index].valueType,
      collectByProjectedValue(input, field),
    ),
  );
}

export function collectByRootFromRecords(
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
  collectBy: CollectByOp,
  records: WindowedRecord[],
): Bytes | undefined {
  const parent = records.find(([, w]) => w > 0);
  if (!parent) return undefined;
  const values = parentValues(inputDesc, outputDesc, collectBy, parent[0]);
  try {
    return outputDesc.create(values);
  } catch (error) {
    throw new IvmRuntimeError("InvalidCollectBy", `root record render failed: ${error}`);
  }
}

function childValues(
  inputDesc: RecordDescriptor,
  childDescriptor: RecordDescriptor,
  childFields: any[],
  record: Bytes,
): Value[] {
  const source = new BorrowedRecord(record, inputDesc);
  return childFields.map((field, index) =>
    collectByOutputValue(
      childDescriptor.fields()[index].valueType,
      collectByProjectedValue(source, field),
    ),
  );
}

export function collectByParentFromRecords(
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
  collectBy: CollectByOp,
  records: WindowedRecord[],
): Bytes | undefined {
  const parent = records.find(([, w]) => w > 0);
  if (!parent) return undefined;
  const values = parentValues(inputDesc, outputDesc, collectBy, parent[0]);
  const children: Value[] = [];
  for (const [record, copies] of collectByWindowFromRecords(inputDesc, records, collectBy)) {
    const child = {
      kind: "Record",
      value: collectBy.childDescriptor.own(
        collectBy.childDescriptor.create(
          childValues(inputDesc, collectBy.childDescriptor, collectBy.childFields, record),
        ),
      ),
    };
    for (let i = 0; i < copies; i++) children.push(child);
  }
  values.push({ kind: "Array", value: children });
  return outputDesc.create(values);
}

export function collectByTreeParentFromRecords(
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
  collectBy: CollectByOp,
  records: WindowedRecord[],
): Bytes | undefined {
  const parent = records.find(([, w]) => w > 0);
  if (!parent) return undefined;
  const values = parentValues(inputDesc, outputDesc, collectBy, parent[0]);
  values.push(...renderCollectBySlots(inputDesc, records, parent[0], collectBy.slots));
  return outputDesc.create(values);
}

function renderCollectBySlots(
  inputDesc: RecordDescriptor,
  records: WindowedRecord[],
  ownerRecord: Bytes,
  slots: CollectBySlot[],
): Value[] {
  return slots.map((slot): Value => {
    if (slot.limit.kind === "Finite" && slot.limit.value === 0) return { kind: "Array", value: [] };
    const ownerKey = encodedRecordKeyPart(inputDesc, ownerRecord, slot.groupFieldIndices);
    // Collapse a child repeated once per grandchild by its rendered projection.
    const childKeyDescriptor = new RecordDescriptorCtor(
      slot.childFields.map((field: any): [string, ValueType] => [
        field.outputName,
        collectByProjectionValueType(inputDesc, field),
      ]),
    );
    const byChildKey = new SortedMap<Bytes, Bytes>(compareBytes);
    for (const [record, weight] of records) {
      if (
        weight <= 0 ||
        !equalBytes(encodedRecordKeyPart(inputDesc, record, slot.groupFieldIndices), ownerKey)
      )
        continue;
      const source = new BorrowedRecord(record, inputDesc);
      if (slot.presenceFieldIndex !== undefined && !source.getBool(slot.presenceFieldIndex))
        continue;
      const values = slot.childFields.map((field: any) => collectByProjectedValue(source, field));
      byChildKey.getOrInsert(childKeyDescriptor.create(values), () => record);
    }
    let candidates = [...byChildKey.valuesArray()];
    if (slot.referenceArrayFieldIndex !== undefined) {
      const byId = new Map<string, Bytes>();
      let references: Value[] | undefined;
      for (const record of candidates) {
        const source = new BorrowedRecord(record, inputDesc);
        if (references === undefined) {
          let array = source.toValues()[slot.referenceArrayFieldIndex];
          while (array.kind === "Nullable" && array.value !== undefined) array = array.value;
          references = array.kind === "Array" ? array.value : [];
        }
        const id = collectByProjectedValue(source, slot.childFields[0]);
        if (id.kind === "Uuid") byId.set(toHex(id.value), record);
      }
      candidates = (references ?? []).flatMap((v) => {
        const hit = v.kind === "Uuid" ? byId.get(toHex(v.value)) : undefined;
        return hit ? [hit] : [];
      });
    }
    const children: Value[] = collectBySlotWindowFromRecords(inputDesc, candidates, slot).map(
      (record) => {
        const values = childValues(inputDesc, slot.childDescriptor, slot.childFields, record);
        values.push(...renderCollectBySlots(inputDesc, records, record, slot.slots));
        return {
          kind: "Record",
          value: slot.childDescriptor.own(slot.childDescriptor.create(values)),
        };
      },
    );
    return { kind: "Array", value: children };
  });
}

function collectBySlotWindowFromRecords(
  descriptor: RecordDescriptor,
  records: Bytes[],
  slot: CollectBySlot,
): Bytes[] {
  const ranked = records.map((record): [TopBySortPart[], Bytes] => [
    collectBySortKeyForFields(descriptor, record, slot.sortFieldIndices, slot.sortDirections),
    record,
  ]);
  if (!slot.referenceOrder)
    ranked.sort((l, r) => cmpTopBySortParts(l[0], r[0]) || compareBytes(l[1], r[1]));
  const take = slot.limit.kind === "Finite" ? slot.limit.value : Infinity;
  return ranked.slice(slot.offset, slot.offset + take).map(([, record]) => record);
}

/** The selected expand window keyed by its ordered contributing source ids. */
export function collectByExpandedWindow(
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
  collectBy: CollectByOp,
  records: WindowedRecord[],
): SortedMap<Bytes, Bytes> {
  const expanded = new SortedMap<Bytes, Bytes>(compareBytes);
  for (const [record, copies] of collectByWindowFromRecords(inputDesc, records, collectBy)) {
    const occurrence = encodedRecordKeyPart(inputDesc, record, collectBy.occurrenceIdFieldIndices);
    // A residual weighted duplicate is malformed; never collapse it into one row.
    if (copies !== 1 || expanded.has(occurrence))
      throw new IvmRuntimeError("DuplicateCollectByOccurrenceId");
    const input = new BorrowedRecord(record, inputDesc);
    const tuple = collectBy.tupleFields.map((field: any) => collectByProjectedValue(input, field));
    expanded.set(occurrence, outputDesc.create(tuple));
  }
  return expanded;
}

function collectByWindowFromRecords(
  descriptor: RecordDescriptor,
  records: WindowedRecord[],
  collectBy: CollectByOp,
): WindowedRecord[] {
  const ranked: [TopBySortPart[], Bytes, number][] = [];
  for (const [record, weight] of records) {
    if (weight > 0) ranked.push([collectBySortKey(descriptor, record, collectBy), record, weight]);
  }
  ranked.sort((l, r) => cmpTopBySortParts(l[0], r[0]) || compareBytes(l[1], r[1]));
  const window: WindowedRecord[] = [];
  let toSkip: number = collectBy.offset;
  let remaining: number | undefined =
    collectBy.limit.kind === "Finite" ? collectBy.limit.value : undefined;
  for (const [, record, weight] of ranked) {
    if (remaining === 0) break;
    const available = Math.max(0, weight - toSkip);
    toSkip = Math.max(0, toSkip - weight);
    const taken = remaining === undefined ? available : Math.min(available, remaining);
    if (taken > 0) {
      window.push([record, taken]);
      if (remaining !== undefined) remaining -= taken;
    }
  }
  return window;
}

function collectBySortKey(
  descriptor: RecordDescriptor,
  record: Bytes,
  collectBy: CollectByOp,
): TopBySortPart[] {
  return collectBySortKeyForFields(
    descriptor,
    record,
    collectBy.sortFieldIndices,
    collectBy.sortDirections,
  );
}

function collectBySortKeyForFields(
  descriptor: RecordDescriptor,
  record: Bytes,
  sortFieldIndices: number[],
  sortDirections: ("Asc" | "Desc")[],
): TopBySortPart[] {
  const input = new BorrowedRecord(record, descriptor);
  const n = Math.min(sortFieldIndices.length, sortDirections.length);
  const parts: TopBySortPart[] = [];
  for (let i = 0; i < n; i++) {
    const fieldIdx = sortFieldIndices[i];
    const field = descriptor.fields()[fieldIdx];
    if (!field) throw new IvmRuntimeError("GraphFieldIndexOutOfBounds", fieldIdx);
    parts.push({
      key: topBySortValue(field.valueType, input.getIdx(fieldIdx)),
      direction: sortDirections[i],
    });
  }
  return parts;
}

const NUM_RANK: Record<string, number> = {
  U8: 0,
  U16: 1,
  U32: 2,
  U64: 3,
  U48: 4,
  I32: 5,
  I64: 6,
  F64: 7,
};

function topBySortValue(valueType: ValueType, value: Value): TopBySortKey {
  if (!collectByOrderedScalar(valueType)) {
    throw new IvmRuntimeError("InvalidTopBy", "sort field value must be an orderable scalar");
  }
  if (isSqlNullValue(value)) return { kind: "Null" };
  const ty = valueType.nonNullable();
  while (value.kind === "Nullable" && value.value !== undefined) value = value.value;
  const mismatch = () =>
    new IvmRuntimeError("InvalidTopBy", "sort field value must match its orderable scalar type");
  if (ty.kind !== value.kind && !(ty.kind === "EnumTag" && value.kind === "EnumTag"))
    throw mismatch();
  switch (value.kind) {
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U48":
    case "I32":
    case "I64":
      return { kind: "Num", rank: NUM_RANK[value.kind], value: value.value };
    case "F64": {
      if (Number.isNaN(value.value)) throw mismatch();
      // Total order over finite and infinite floats, with -0 == +0.
      const view = new DataView(new ArrayBuffer(8));
      view.setFloat64(0, value.value === 0 ? 0 : value.value);
      const bits = view.getBigUint64(0);
      const ordered =
        (bits & (1n << 63n)) === 0n ? bits ^ (1n << 63n) : ~bits & 0xffffffffffffffffn;
      return { kind: "Num", rank: NUM_RANK.F64, value: ordered };
    }
    case "Bool":
      return { kind: "Bool", value: value.value };
    case "String":
      return { kind: "String", value: value.value };
    case "Bytes":
      return { kind: "Bytes", value: value.value };
    case "Uuid":
      return { kind: "Uuid", value: value.value };
    case "EnumTag":
      return { kind: "EnumTag", value: value.value };
    default:
      throw mismatch();
  }
}

export function topBySortKey(
  descriptor: RecordDescriptor,
  record: Bytes,
  topBy: TopByOp,
): TopBySortPart[] {
  return collectBySortKeyForFields(
    descriptor,
    record,
    topBy.sortFieldIndices,
    topBy.sortDirections,
  );
}

export function diffRecordWindows(
  before: WindowedRecord[],
  after: WindowedRecord[],
): RecordDelta[] {
  const weights = new Map<string, number>();
  const bump = (record: Bytes, d: number) =>
    weights.set(toHex(record), (weights.get(toHex(record)) ?? 0) + d);
  for (const [record, copies] of before) bump(record, -copies);
  for (const [record, copies] of after) bump(record, copies);
  const deltas: RecordDelta[] = [];
  const emit = (record: Bytes, negative: boolean) => {
    const k = toHex(record);
    const weight = weights.get(k);
    if (weight !== undefined && (negative ? weight < 0 : weight > 0)) {
      weights.delete(k);
      deltas.push({ record, weight });
    }
  };
  for (const [record] of before) emit(record, true);
  for (const [record] of after) emit(record, false);
  return deltas;
}

/** Unbounded zero-offset membership: compare first/final weights of touched records. */
export function updateUnboundedTopByGroupTouching(
  descriptor: RecordDescriptor,
  topBy: TopByOp,
  group: CollectByGroup,
  input: RecordDelta[],
  touchedKeys: CollectByOrderKey[] | undefined,
): RecordDelta[] {
  const touched = new SortedMap<CollectByOrderKey, [number, number]>(cmpOrderKey);
  for (const delta of input) {
    const key: CollectByOrderKey = [topBySortKey(descriptor, delta.record, topBy), delta.record];
    const weights = touched.getOrInsert(key, () => {
      const before = group.get(key) ?? 0;
      return [before, before];
    });
    weights[1] += delta.weight;
  }
  const removed: RecordDelta[] = [];
  const added: RecordDelta[] = [];
  for (const [key, [before, after]] of touched) {
    // Negative internal bag weights are retained but never selected.
    const weight = Math.max(after, 0) - Math.max(before, 0);
    if (weight !== 0) (weight < 0 ? removed : added).push({ record: key[1], weight });
    touchedKeys?.push(key);
    group.set(key, after);
  }
  return [...removed, ...added];
}

export function encodedRecordKeyPart(
  descriptor: RecordDescriptor,
  record: Bytes,
  fieldIndices: number[],
): Bytes {
  const key: number[] = [];
  for (const fieldIdx of fieldIndices)
    encodeRuntimePrimaryKeyPart(key, descriptor.getIdx(record, fieldIdx));
  return Uint8Array.from(key);
}

/** Terminal root identity key (#3309): process-local opaque key over every value kind. */
export function encodedIdentityKeyPart(
  descriptor: RecordDescriptor,
  record: Bytes,
  fieldIndices: number[],
): Bytes {
  const key: number[] = [];
  for (const fieldIdx of fieldIndices)
    encodeIdentityKeyPart(key, descriptor.getIdx(record, fieldIdx));
  return Uint8Array.from(key);
}

function pushU64(key: number[], v: number | bigint): void {
  const view = new DataView(new ArrayBuffer(8));
  view.setBigUint64(0, BigInt(v));
  key.push(...new Uint8Array(view.buffer));
}

function pushU32(key: number[], v: number): void {
  key.push((v >>> 24) & 255, (v >>> 16) & 255, (v >>> 8) & 255, v & 255);
}

function encodeNested(
  key: number[],
  value: Value,
  recurse: (key: number[], value: Value) => void,
): boolean {
  switch (value.kind) {
    case "Tuple":
      key.push(11);
      for (const v of value.value) recurse(key, v);
      return true;
    case "Nullable":
      key.push(12, value.value === undefined ? 0 : 1);
      if (value.value !== undefined) recurse(key, value.value);
      return true;
    default:
      return false;
  }
}

function encodeIdentityKeyPart(key: number[], value: Value): void {
  if (encodeNested(key, value, encodeIdentityKeyPart)) return;
  switch (value.kind) {
    case "Array":
      key.push(16);
      pushU64(key, value.value.length);
      for (const v of value.value) encodeIdentityKeyPart(key, v);
      return;
    case "Enum": {
      // The column schema fixes each case's payload descriptor.
      key.push(17);
      pushU32(key, value.value.tag());
      const payload = value.value.record();
      const fields = payload.descriptor().fields().length;
      pushU64(key, fields);
      for (let i = 0; i < fields; i++) encodeIdentityKeyPart(key, payload.getIdx(i));
      return;
    }
    case "Large": {
      // Identified by content (hash, lengths, pending edits), not chunk location.
      const large = value.value;
      key.push(
        18,
        large.kind === "String" ? 0 : large.kind === "Bytes" ? 1 : 2,
        large.formatVersion,
      );
      key.push(...large.logicalHash);
      pushU64(key, large.byteLength);
      if (large.utf16Length !== undefined) {
        key.push(1);
        pushU64(key, large.utf16Length);
      } else key.push(0);
      pushU64(key, large.editTail.length);
      for (const edit of large.editTail) {
        pushU64(key, edit.offset);
        pushU64(key, edit.deleteLength);
        pushU64(key, edit.utf16Offset);
        pushU64(key, edit.deleteUtf16Length);
        pushU64(key, edit.insertUtf16Length);
        encodeOrderedBytes(key, edit.insertBytes);
      }
      return;
    }
    default:
      encodeRuntimePrimaryKeyPart(key, value);
  }
}

export function encodedArrangementKeyPart(
  descriptor: RecordDescriptor,
  record: Bytes,
  fieldIndices: number[],
): Bytes {
  const key: number[] = [];
  for (const fieldIdx of fieldIndices) encodeKeyPart(key, descriptor.getIdx(record, fieldIdx));
  return Uint8Array.from(key);
}

function encodeRuntimePrimaryKeyPart(key: number[], value: Value): void {
  if (encodeNested(key, value, encodeRuntimePrimaryKeyPart)) return;
  if (value.kind === "Record") {
    // Whole author records: identity by descriptor and bytes, not intern IDs.
    key.push(15);
    encodeOrderedBytes(key, encodeRecordDescriptor(value.value.descriptor()));
    encodeOrderedBytes(key, value.value.raw());
    return;
  }
  encodeScalarKeyPart(key, value);
}
