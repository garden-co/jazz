// Record descriptors, projection, enum remapping, and operator shape validation.
import {
  type RecordDelta,
  BorrowedRecord,
  DescriptorField,
  EnumValue,
  OwnedRecord,
  PreparedProjection,
  RecordsError,
  encodeSingleFieldValue,
  type RecordDeltas,
  type TerminalDeltas,
  type Value,
  type ValueType,
  type WindowedRecord,
  type TableSchema,
  type IndexSchema,
  IvmRuntimeError,
  NodeState,
  RecordDescriptor,
  collectByOrderedScalar,
  predicateMatches,
  predicateReferencedFields,
  valueTypeContainsRecord,
} from "../../../_extern/groove-ivm-plan.js";
import { compareBytes, concatBytes, equalBytes, toHex } from "../../../support/bytes.js";
import {
  type CollectByField,
  type CollectBySlotBuilder,
  type GraphBuilder,
  type IvmGraph,
  type NodeId,
  type ProjectField,
  FieldRef,
  ProjectField as ProjectFieldCtor,
  MAX_COLLECT_BY_TREE_DEPTH,
  graphPostorder,
} from "../graph.js";
import {
  type AggregateExpr,
  type IndexByOp,
  type LiteralValue,
  type MapProjectOp,
  type StaticScanSpec,
  type VariantProjectionTarget,
  type CollectByProjection,
  type CollectBySlot,
  type ProjectionExpr,
  type RecursiveEnumRemaps,
  type TopByDirection,
  type TopByOp,
  literalToValue,
  literalValueType,
  structEq,
} from "../op_types.js";
import {
  encodeKeyPart,
  encodeRecordFieldKeyPart,
  primaryKeyValueBytes,
  recordFieldKeyParts,
} from "./key_encoding.js";
import { durableIndexKeyPrefix } from "./index_ids.js";

type RawProjectionField =
  | { kind: "Copy"; sourceIdx: number }
  | { kind: "WrapNullable"; sourceIdx: number }
  | { kind: "Nested"; path: [RecordDescriptor, number][] }
  | { kind: "Encoded"; bytes: Uint8Array }
  | { kind: "Error"; error: unknown }
  | { kind: "Evaluate" };

function nodeOf(graph: IvmGraph, id: NodeId) {
  const node = graph.node(id);
  if (!node) throw IvmRuntimeError.GraphNodeNotFound(id);
  return node;
}

/** Keep demand planning and terminal publication on the same classification. */
export function outputIsStructuredCollectBy(graph: IvmGraph, node: NodeId): boolean {
  const op = nodeOf(graph, node).descriptor.operator;
  return op.kind === "CollectBy" && (op.op.mode === "Collect" || op.op.mode === "Root");
}

/** Row identity of a TopBy's output: group fields plus tie fields (or the whole record). */
export function topByIdentityFields(topBy: TopByOp, width: number): number[] {
  const orderLen = topBy.orderFields.length;
  const tie = topBy.sortFieldIndices.slice(Math.min(orderLen, topBy.sortFieldIndices.length));
  const fields = topBy.groupFieldIndices.slice();
  const identity = tie.length ? tie : Array.from({ length: width }, (_, i) => i);
  for (const index of identity) if (!fields.includes(index)) fields.push(index);
  return fields;
}

/** Identity fields of a plain output; the first `groupLen` are the TopBy group fields. */
export interface RootIdentity {
  fields: number[];
  groupLen: number;
  chain: NodeId[];
  filtered: boolean;
  projected: boolean;
}

export function outputConsumesRootPositions(
  graph: IvmGraph,
  output: NodeId,
  ordering: NodeId,
): boolean {
  return (
    !outputIsStructuredCollectBy(graph, output) &&
    rootIdentityFields(graph, output, ordering) !== undefined
  );
}

/** Terminal key fields of a plain output ordered by `ordering` (#3290). */
export function rootIdentityFields(
  graph: IvmGraph,
  output: NodeId,
  ordering: NodeId,
): RootIdentity | undefined {
  const chain: NodeId[] = [];
  let node = output;
  while (node !== ordering) {
    const current = nodeOf(graph, node);
    if (current.descriptor.inputs.length !== 1) return undefined;
    const kind = current.descriptor.operator.kind;
    if (kind !== "MapProject" && kind !== "Filter") return undefined;
    chain.push(node);
    node = current.descriptor.inputs[0];
  }
  const topNode = nodeOf(graph, ordering);
  const width = topNode.descriptor.output.records.fields().length;
  const op = topNode.descriptor.operator;
  if (op.kind !== "TopBy") return undefined;
  const topBy = op.op;
  chain.reverse();
  const filtered = chainFiltersWithinGroups(graph, chain, topBy, width);
  const groupLen = topBy.groupFieldIndices.length;
  const fields = mapThroughProjections(graph, chain, topByIdentityFields(topBy, width));
  if (fields) return { fields, groupLen, chain, filtered, projected: false };
  const groupFields = mapThroughProjections(graph, chain, topBy.groupFieldIndices.slice());
  if (!groupFields) return undefined;
  const outputWidth = nodeOf(graph, output).descriptor.output.records.fields().length;
  for (let i = 0; i < outputWidth; i++) if (!groupFields.includes(i)) groupFields.push(i);
  return { fields: groupFields, groupLen, chain, filtered, projected: true };
}

/** Whether a filter in `chain` can drop some rows of a group but keep others. */
function chainFiltersWithinGroups(
  graph: IvmGraph,
  chain: NodeId[],
  topBy: TopByOp,
  width: number,
): boolean {
  // For each field of the current record, the TopBy field it copies.
  let sources: (number | undefined)[] = Array.from({ length: width }, (_, i) => i);
  for (const id of chain) {
    const current = nodeOf(graph, id);
    const inputOutput = nodeOf(graph, current.descriptor.inputs[0]).descriptor.output.records;
    const op = current.descriptor.operator;
    if (op.kind === "Filter") {
      const referenced = new Set<string>();
      predicateReferencedFields(op.op.predicate, referenced);
      const withinGroup = [...referenced].every((field) => {
        const index = resolveFieldName(inputOutput, field);
        const source = index === undefined ? undefined : sources[index];
        return source !== undefined && topBy.groupFieldIndices.includes(source);
      });
      if (!withinGroup) return true;
    } else if (op.kind === "MapProject") {
      sources = op.op.expressions.map((e) => {
        if (e.expression.kind !== "Field") return undefined;
        const index = tryResolveFieldRef(inputOutput, e.expression.source);
        return index === undefined ? undefined : sources[index];
      });
    } else throw IvmRuntimeError.UnsupportedOperator();
  }
  return false;
}

/** Positions of TopBy output `fields` in the output of `chain`. */
function mapThroughProjections(
  graph: IvmGraph,
  chain: NodeId[],
  fields: number[],
): number[] | undefined {
  for (const id of chain) {
    const current = nodeOf(graph, id);
    const op = current.descriptor.operator;
    if (op.kind !== "MapProject") continue;
    const inputOutput = nodeOf(graph, current.descriptor.inputs[0]).descriptor.output.records;
    const mapped: number[] = [];
    for (const field of fields) {
      const position = op.op.expressions.findIndex(
        (e) =>
          e.expression.kind === "Field" &&
          tryResolveFieldRef(inputOutput, e.expression.source) === field,
      );
      if (position < 0) return undefined;
      mapped.push(position);
    }
    fields = mapped;
  }
  return fields;
}

export type WindowReach = "Yes" | "No" | "Unknown";

/** Runs a TopBy window record through `chain`. */
export function windowRecordReachesOutput(
  graph: IvmGraph,
  chain: NodeId[],
  descriptor: RecordDescriptor,
  record: Uint8Array,
): WindowReach {
  let current: RecordDeltas = { descriptor, deltas: [{ record, weight: 1 }] };
  let unknown = false;
  for (const id of chain) {
    const node = nodeOf(graph, id);
    const delta = current.deltas[0];
    if (!delta) return "No";
    const op = node.descriptor.operator;
    if (op.kind === "Filter") {
      const referenced = new Set<string>();
      predicateReferencedFields(op.op.predicate, referenced);
      const indices = [...referenced].map((field) => {
        const idx = resolveFieldName(current.descriptor, field);
        if (idx === undefined) throw IvmRuntimeError.GraphFieldNotFound(field);
        return idx;
      });
      if (current.descriptor.fieldsContainIndirectValues(delta.record, indices)) unknown = true;
      else if (
        !predicateMatches(op.op.predicate, current.descriptor.bind(delta.record), op.op.comparison)
      )
        return "No";
    } else if (op.kind === "MapProject") {
      current = NodeState.updateMapProject(
        op.op,
        node.descriptor.output.records,
        current,
        undefined,
        false,
      );
    } else throw IvmRuntimeError.UnsupportedOperator();
  }
  return !current.deltas.length ? "No" : unknown ? "Unknown" : "Yes";
}

/** A TopBy window record carried through the projections of `chain`. */
export function projectWindowRecord(
  graph: IvmGraph,
  chain: NodeId[],
  descriptor: RecordDescriptor,
  record: Uint8Array,
): [RecordDescriptor, Uint8Array] | undefined {
  let current: RecordDeltas = { descriptor, deltas: [{ record: record.slice(), weight: 1 }] };
  for (const id of chain) {
    const node = nodeOf(graph, id);
    const op = node.descriptor.operator;
    if (op.kind === "Filter") continue;
    if (op.kind !== "MapProject") throw IvmRuntimeError.UnsupportedOperator();
    current = NodeState.updateMapProject(
      op.op,
      node.descriptor.output.records,
      current,
      undefined,
      false,
    );
  }
  const first = current.deltas[0];
  return first && [current.descriptor, first.record];
}

/** Encodes a window record's terminal root key. */
export type RootKeyOf = (record: Uint8Array) => Uint8Array;

/** Root ordering for one group's window (#3290, #3505). */
export function applyGroupWindowOrdering(
  before: WindowedRecord[],
  after: WindowedRecord[],
  keyOf: RootKeyOf,
  rootDescriptor: RecordDescriptor,
  terminal: TerminalDeltas,
): void {
  const records = (w: WindowedRecord[]) => new Set(w.map(([r]) => toHex(r)));
  const beforeRecords = records(before);
  const afterRecords = records(after);
  const changed = (w: WindowedRecord[], other: Set<string>) =>
    w.flatMap(([r], index): [number, Uint8Array][] =>
      other.has(toHex(r)) ? [] : [[index, keyOf(r)]],
    );
  const changes: GroupChanges = {
    removed: changed(before, afterRecords),
    added: changed(after, beforeRecords),
  };
  if (applyChangedGroupOrdering(changes, rootDescriptor, terminal)) return;
  applyGroupWindowOrderingByScan(before, after, keyOf, rootDescriptor, terminal);
}

/** One group's rows whose presence differs between the windows, ascending by index. */
export interface GroupChanges {
  removed: [number, Uint8Array][];
  added: [number, Uint8Array][];
}

interface FloatingRoot {
  key: string;
  gap: number;
  settled: boolean;
}

/** Places the changed rows of one group with O(changed²) work (#3505). */
export function applyChangedGroupOrdering(
  changes: GroupChanges,
  rootDescriptor: RecordDescriptor,
  terminal: TerminalDeltas,
): boolean {
  const target = new Map<string, number>();
  for (const [slot, [, key]] of changes.added.entries()) {
    const k = toHex(key);
    if (target.has(k)) return false;
    target.set(k, slot);
  }
  const departed = new Set<string>();
  for (const [, key] of changes.removed) {
    const k = toHex(key);
    if (departed.has(k)) return false;
    departed.add(k);
  }
  if (!target.size && !departed.size) return true;
  const changed = (key: Uint8Array) => target.has(toHex(key)) || departed.has(toHex(key));
  for (const op of terminal.operations) {
    if (
      !op.path.length &&
      changed(op.rootKey) &&
      op.edit.kind === "Insert" &&
      !target.has(toHex(op.edit.key))
    )
      return false;
  }
  const added = changes.added;
  const addedKeys = added.map(([, k]) => toHex(k));
  const gapOf = (slot: number) => added[slot][0] - slot;
  const floating: FloatingRoot[] = changes.removed.map(([index, key], rank) => ({
    key: toHex(key),
    gap: index - rank,
    settled: false,
  }));
  const settled = new Array<boolean>(added.length).fill(false);
  const position = (key: string) => floating.findIndex((r) => r.key === key);
  // The list slot directly after after-row `slot`'s nearest settled predecessor.
  const placement = (slot: number): [number, number] => {
    const gap = gapOf(slot);
    for (let previous = slot - 1; previous >= 0; previous--) {
      if (gapOf(previous) !== gap) break;
      if (settled[previous]) return [position(addedKeys[previous]) + 1, gap];
    }
    let at = 0;
    while (at < floating.length && floating[at].gap < gap) at++;
    return [at, gap];
  };
  for (const op of terminal.operations) {
    if (op.path.length || !changed(op.rootKey)) continue;
    const edit = op.edit;
    if (edit.kind === "Insert") {
      const k = toHex(edit.key);
      const slot = target.get(k)!;
      const existing = position(k);
      if (existing >= 0) floating.splice(existing, 1);
      settled[slot] = false;
      const [at, gap] = placement(slot);
      floating.splice(at, 0, { key: addedKeys[slot], gap, settled: true });
      settled[slot] = true;
      edit.index = gap + at;
    } else if (edit.kind === "Remove") {
      const k = toHex(edit.key);
      const existing = position(k);
      if (existing >= 0) floating.splice(existing, 1);
      const slot = target.get(k);
      if (slot !== undefined) settled[slot] = false;
    }
  }
  for (let slot = 0; slot < added.length; slot++) {
    if (settled[slot]) continue;
    const key = addedKeys[slot];
    const current = position(key);
    if (current < 0) continue;
    const gap = gapOf(slot);
    const [afterPredecessor] = placement(slot);
    const inPlace =
      floating[current].gap === gap &&
      current >= afterPredecessor &&
      floating.slice(afterPredecessor, current).every((r) => !r.settled);
    if (inPlace) {
      floating[current].settled = true;
      settled[slot] = true;
      continue;
    }
    floating.splice(current, 1);
    const [at, g] = placement(slot);
    floating.splice(at, 0, { key, gap: g, settled: true });
    settled[slot] = true;
    const rawKey = added[slot][1];
    terminal.operations.push({
      rootDescriptor,
      rootKey: rawKey,
      path: [],
      edit: { kind: "Move", key: rawKey, index: g + at },
    });
  }
  return true;
}

/** The pre-#3505 placement: walks both complete windows. */
function applyGroupWindowOrderingByScan(
  before: WindowedRecord[],
  after: WindowedRecord[],
  keyOf: RootKeyOf,
  rootDescriptor: RecordDescriptor,
  terminal: TerminalDeltas,
): void {
  // A root is "u:<record>" when byte-identical in both windows, else "k:<key>".
  const records = (w: WindowedRecord[]) => new Set(w.map(([r]) => toHex(r)));
  const beforeRecords = records(before);
  const afterRecords = records(after);
  const root = (record: Uint8Array, other: Set<string>) =>
    other.has(toHex(record)) ? `u:${toHex(record)}` : `k:${toHex(keyOf(record))}`;
  const current = before.map(([r]) => root(r, afterRecords));
  const desired = after.map(([r]) => root(r, beforeRecords));
  const groupKeys = new Set(
    [...current, ...desired].filter((r) => r.startsWith("k:")).map((r) => r.slice(2)),
  );
  for (const op of terminal.operations) {
    if (op.path.length || !groupKeys.has(toHex(op.rootKey))) continue;
    const edit = op.edit;
    if (edit.kind === "Insert") {
      const r = `k:${toHex(edit.key)}`;
      const actual = desired.indexOf(r);
      if (actual >= 0) edit.index = actual;
      const existing = current.indexOf(r);
      if (existing >= 0) current.splice(existing, 1);
      current.splice(Math.min(edit.index, current.length), 0, r);
    } else if (edit.kind === "Remove") {
      const existing = current.indexOf(`k:${toHex(edit.key)}`);
      if (existing >= 0) current.splice(existing, 1);
    }
  }
  desired.forEach((r, afterIndex) => {
    if (current[afterIndex] === r) return;
    const existing = current.indexOf(r);
    if (existing < 0) return;
    const [moved] = current.splice(existing, 1);
    current.splice(Math.min(afterIndex, current.length), 0, moved);
    const key = r.startsWith("k:") ? keyFromHex(r.slice(2)) : keyOf(after[afterIndex][0]);
    terminal.operations.push({
      rootDescriptor,
      rootKey: key,
      path: [],
      edit: { kind: "Move", key, index: afterIndex },
    });
  });
}

const keyFromHex = (hex: string) => Uint8Array.from(hex.match(/../g) ?? [], (h) => parseInt(h, 16));

function fieldTypeAt(input: RecordDescriptor, index: number): ValueType {
  const field = input.fields()[index];
  if (!field) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(index);
  return field.valueType;
}

export function projectDescriptor(
  input: RecordDescriptor,
  fields: ProjectField[],
): RecordDescriptor {
  return RecordDescriptor.newWithFields(
    fields.map((pf) => {
      const e = pf.expression;
      let valueType: ValueType;
      switch (e.kind) {
        case "RecordField": {
          valueType = fieldTypeAt(input, resolveFieldRef(input, e.source));
          for (const name of e.path) {
            if (valueType.kind !== "Record") throw IvmRuntimeError.UnsupportedOperator();
            const idx = valueType.descriptor.fieldIndex(name);
            if (idx === undefined) throw IvmRuntimeError.UnsupportedOperator();
            valueType = valueType.descriptor.fields()[idx].valueType;
          }
          break;
        }
        case "Field":
        case "EnumTagRemap":
        case "EnumRemap":
          valueType = fieldTypeAt(input, resolveFieldRef(input, e.source));
          break;
        case "Literal": {
          const t = literalValueType(e.value);
          if (!t) throw IvmRuntimeError.UnsupportedOperator();
          valueType = t;
          break;
        }
        case "TypedLiteral":
        case "TemplateArgument":
        case "Null":
          valueType = e.valueType;
          break;
        case "Nullable":
          valueType = {
            kind: "Nullable",
            inner: fieldTypeAt(input, resolveFieldRef(input, e.source)),
          };
          break;
        case "NullableFlat": {
          const inner = fieldTypeAt(input, resolveFieldRef(input, e.source));
          valueType = inner.kind === "Nullable" ? inner : { kind: "Nullable", inner };
          break;
        }
        case "RecursiveEnumRemap":
          valueType = e.target;
      }
      return { name: pf.outputName, identity: pf.outputIdentity, valueType } as DescriptorField;
    }),
  );
}

const invalidCollect = (m: string) => IvmRuntimeError.InvalidCollectBy(m);
const depthError = () =>
  invalidCollect(
    `tree collect depth exceeds MAX_COLLECT_BY_TREE_DEPTH (${MAX_COLLECT_BY_TREE_DEPTH})`,
  );
const arrayOfRecord = (descriptor: RecordDescriptor): ValueType => ({
  kind: "Array",
  inner: { kind: "Record", descriptor },
});

function uniqueNamed(
  fields: CollectByField[],
  input: RecordDescriptor,
  names: Set<string>,
  message: string,
  out: [string, ValueType][],
) {
  for (const field of fields) {
    if (names.has(field.outputName)) throw invalidCollect(message);
    names.add(field.outputName);
    out.push([
      field.outputName,
      collectByFieldValueType(input, resolveFieldRef(input, field.field), field),
    ]);
  }
}

export function collectByDescriptor(
  input: RecordDescriptor,
  parentFields: CollectByField[],
  childFields: CollectByField[],
  collectionField: string,
): RecordDescriptor {
  if (!parentFields.length || !childFields.length || !collectionField)
    throw invalidCollect("parent projection, child projection, and collection slot are required");
  const output: [string, ValueType][] = [];
  const names = new Set<string>();
  for (const field of parentFields) {
    if (names.has(field.outputName) || field.outputName === collectionField)
      throw invalidCollect("output field names must be unique");
    names.add(field.outputName);
    output.push([
      field.outputName,
      collectByFieldValueType(input, resolveFieldRef(input, field.field), field),
    ]);
  }
  const child: [string, ValueType][] = [];
  uniqueNamed(childFields, input, new Set(), "child output field names must be unique", child);
  output.push([collectionField, arrayOfRecord(RecordDescriptor.new(child))]);
  return RecordDescriptor.new(output);
}

export function collectByRootDescriptor(
  input: RecordDescriptor,
  parentFields: CollectByField[],
): RecordDescriptor {
  if (!parentFields.length) throw invalidCollect("root collect requires a parent projection");
  return RecordDescriptor.new(
    parentFields.map((f) => [
      f.outputName,
      collectByFieldValueType(input, resolveFieldRef(input, f.field), f),
    ]),
  );
}

export function collectByTreeDescriptor(
  input: RecordDescriptor,
  parentFields: CollectByField[],
  slots: CollectBySlotBuilder[],
): RecordDescriptor {
  if (!parentFields.length || !slots.length)
    throw invalidCollect(
      "tree collect requires a parent projection and at least one collection slot",
    );
  const names = new Set<string>();
  const output: [string, ValueType][] = [];
  uniqueNamed(parentFields, input, names, "output field names must be unique", output);
  for (const slot of slots) {
    if (names.has(slot.collectionField)) throw invalidCollect("output field names must be unique");
    names.add(slot.collectionField);
    output.push([slot.collectionField, arrayOfRecord(collectBySlotDescriptor(input, slot, 1))]);
  }
  return RecordDescriptor.new(output);
}

function collectBySlotDescriptor(
  input: RecordDescriptor,
  slot: CollectBySlotBuilder,
  depth: number,
): RecordDescriptor {
  if (depth > MAX_COLLECT_BY_TREE_DEPTH) throw depthError();
  if (
    !slot.groupCols.length ||
    !slot.childFields.length ||
    !slot.collectionField ||
    !slot.orderCols.length ||
    !slot.tieCols.length
  )
    throw invalidCollect(
      "each tree collection slot requires group, child, name, order, and tie fields",
    );
  const names = new Set<string>();
  const output: [string, ValueType][] = [];
  uniqueNamed(slot.childFields, input, names, "child output field names must be unique", output);
  for (const nested of slot.slots) {
    if (names.has(nested.collectionField))
      throw invalidCollect("child output field names must be unique");
    names.add(nested.collectionField);
    output.push([
      nested.collectionField,
      arrayOfRecord(collectBySlotDescriptor(input, nested, depth + 1)),
    ]);
  }
  return RecordDescriptor.new(output);
}

export function collectBySlots(
  input: RecordDescriptor,
  builders: CollectBySlotBuilder[],
  ownerIndices: number[],
  depth: number,
): CollectBySlot[] {
  if (depth > MAX_COLLECT_BY_TREE_DEPTH) throw depthError();
  const names = new Set<string>();
  return builders.map((builder, slotIndex): CollectBySlot => {
    if (names.has(builder.collectionField))
      throw invalidCollect("sibling collection slot names must be unique");
    names.add(builder.collectionField);
    const resolve = (f: FieldRef) => resolveFieldRef(input, f);
    const groupFieldIndices = builder.groupCols.map(resolve);
    if (!groupFieldIndices.length || groupFieldIndices.some((f) => !ownerIndices.includes(f)))
      throw invalidCollect("a slot group must be available on its owning record");
    validateCollectByKeyTypes(input, groupFieldIndices);
    const childFields = collectByProjections(input, builder.childFields);
    const orderFieldIndices = builder.orderCols.map((o) => resolve(o.field));
    const tieFieldIndices = builder.tieCols.map(resolve);
    const presenceFieldIndex = builder.presenceCol && resolve(builder.presenceCol);
    if (
      presenceFieldIndex !== undefined &&
      input.fields()[presenceFieldIndex].valueType.kind !== "Bool"
    )
      throw invalidCollect("a tree collection presence field must be boolean");
    if (!orderFieldIndices.length || !tieFieldIndices.length)
      throw invalidCollect("order and tie fields must both be complete and non-empty");
    validateCollectByKeyTypes(input, orderFieldIndices);
    validateCollectByKeyTypes(input, tieFieldIndices);
    const referenceArrayFieldIndex =
      builder.referenceArrayCol && resolve(builder.referenceArrayCol);
    if (referenceArrayFieldIndex !== undefined) {
      const t = input.fields()[referenceArrayFieldIndex].valueType;
      const nonNull = t.kind === "Nullable" ? t.inner : t;
      if (!(nonNull.kind === "Array" && nonNull.inner.kind === "Uuid"))
        throw invalidCollect("reference sequence must be a UUID array");
    }
    const childDescriptor = collectBySlotDescriptor(input, builder, depth);
    if (builder.referenceOrder && referenceArrayFieldIndex === undefined)
      throw invalidCollect("reference ordering requires a reference array");
    if (
      referenceArrayFieldIndex !== undefined &&
      childDescriptor.fields()[0]?.valueType.kind !== "Uuid"
    )
      throw invalidCollect("a reference collection must project its child UUID first");
    // Nested slots address the raw input record selected for this child;
    // ownerKeyCols is the explicit internal channel for stable grouping keys.
    const owner = childFields.map((f) => f.fieldIdx);
    for (const ownerKey of builder.ownerKeyCols) {
      const idx = resolve(ownerKey);
      if (!groupFieldIndices.includes(idx))
        throw invalidCollect("a slot owner key must also be a grouping field");
      if (!owner.includes(idx)) owner.push(idx);
    }
    return {
      groupFields: groupFieldIndices.map((f) => fieldNameAt(input, f)),
      groupFieldIndices,
      childFields,
      childDescriptor,
      collectionField: builder.collectionField,
      collectionFieldIndex: builder.childFields.length + slotIndex,
      slots: collectBySlots(input, builder.slots, owner, depth + 1),
      orderFields: builder.orderCols.map((o, i) => ({
        field: fieldNameAt(input, orderFieldIndices[i]),
        direction: o.direction,
      })),
      tieFields: tieFieldIndices.map((f) => fieldNameAt(input, f)),
      presenceFieldIndex,
      referenceArrayFieldIndex,
      referenceOrder: builder.referenceOrder,
      sortFieldIndices: [...orderFieldIndices, ...tieFieldIndices],
      sortDirections: [
        ...builder.orderCols.map((o) => o.direction),
        ...tieFieldIndices.map((): TopByDirection => "Asc"),
      ],
      offset: builder.offset,
      limit: builder.limit,
    };
  });
}

export function collectByExpandDescriptor(
  input: RecordDescriptor,
  tupleFields: CollectByField[],
): RecordDescriptor {
  if (!tupleFields.length)
    throw invalidCollect("expand mode requires a non-empty tuple projection");
  const out: [string, ValueType][] = [];
  uniqueNamed(tupleFields, input, new Set(), "expand tuple output field names must be unique", out);
  return RecordDescriptor.new(out);
}

export function collectByProjections(
  input: RecordDescriptor,
  fields: CollectByField[],
): CollectByProjection[] {
  return fields.map((f) => ({
    field: fieldRefName(input, f.field),
    fieldIdx: resolveFieldRef(input, f.field),
    outputName: f.outputName,
    unwrapNullable: f.unwrapNullable,
  }));
}

function collectByFieldValueType(
  input: RecordDescriptor,
  index: number,
  field: CollectByField,
): ValueType {
  const t = input.fields()[index].valueType;
  return field.unwrapNullable && t.kind === "Nullable" ? t.inner : t;
}

export function validateCollectByKeyTypes(input: RecordDescriptor, indices: number[]): void {
  for (const index of indices) {
    const t = fieldTypeAt(input, index);
    if (valueTypeContainsRecord(t) || !collectByOrderedScalar(t))
      throw invalidCollect(
        "group, order, and tie fields must be scalar ordered values without records",
      );
  }
}

export function validateCollectByTerminality(graph: GraphBuilder): void {
  const containsCollect = new Map<GraphBuilder, boolean>();
  for (const node of graphPostorder(graph)) {
    const children: GraphBuilder[] = (() => {
      switch (node.kind) {
        case "TypedTemplate":
        case "Union":
          return node.inputs;
        case "TemplateInput":
          return node.input ? [node.input] : [];
        case "Lookup":
          return [node.keys];
        case "Join":
        case "SemiJoin":
        case "AntiJoin":
          return [node.left, node.right];
        case "Recursive":
          return node.stepWitness
            ? [node.seed, node.step, node.stepWitness]
            : [node.seed, node.step];
        case "RecursiveStepWitness":
          return [node.recursive];
        default:
          return "input" in node ? [node.input] : [];
      }
    })();
    const childrenContain = children.some((c) => containsCollect.get(c) ?? false);
    if (childrenContain) throw IvmRuntimeError.CollectByMustBeTerminal();
    containsCollect.set(
      node,
      node.kind === "CollectBy" || (node.kind === "TypedTemplate" && node.program.terminal),
    );
  }
}

export function projectFieldExpr(
  input: RecordDescriptor,
  field: ProjectField,
): ProjectionExpr["expression"] {
  const e = field.expression;
  if ("source" in e)
    return {
      ...e,
      source: { kind: "Resolved", index: resolveFieldRef(input, e.source) },
    } as typeof e;
  return e;
}

export function projectRecord(
  expressions: ProjectionExpr[],
  mapping: [number, number][],
  outputDesc: RecordDescriptor,
  inputDesc: RecordDescriptor,
  inputRecord: Uint8Array,
): Uint8Array {
  if (projectionUsesRawCopy(expressions, mapping, outputDesc))
    return outputDesc.projectRecordRaw([inputDesc], [inputRecord], mapping);
  return outputDesc.create(
    expressions.map((e, i) => projectFieldValue(e, i, outputDesc, inputDesc, inputRecord)),
  );
}

export function projectFieldValue(
  expr: ProjectionExpr,
  outputIdx: number,
  outputDesc: RecordDescriptor,
  inputDesc: RecordDescriptor,
  inputRecord: Uint8Array,
): Value {
  const input = BorrowedRecord.new(inputRecord, inputDesc);
  const resolved = (field: FieldRef): Value => {
    const sourceIdx = resolveFieldRef(inputDesc, field);
    try {
      return input.getIdx(sourceIdx);
    } catch (e) {
      throw IvmRuntimeError.RecordEncoding(e);
    }
  };
  const e = expr.expression;
  switch (e.kind) {
    case "TemplateArgument":
      throw IvmRuntimeError.UnsupportedOperator();
    case "Field":
      return resolved(e.source);
    case "RecordField": {
      let value = resolved(e.source);
      for (const name of e.path) {
        if (value.kind !== "Record") throw IvmRuntimeError.UnsupportedOperator();
        value = value.record.get(name);
      }
      return value;
    }
    case "Literal":
    case "TypedLiteral":
      return literalToValue(e.value);
    case "Null":
      return { kind: "Nullable" };
    case "Nullable":
      return { kind: "Nullable", value: resolved(e.source) };
    case "NullableFlat": {
      const value = resolved(e.source);
      return value.kind === "Nullable" ? value : { kind: "Nullable", value };
    }
    case "EnumTagRemap":
      return remapEnumTag(resolved(e.source), e.tags);
    case "EnumRemap":
      return remapEnum(resolved(e.source), e.tags);
    case "RecursiveEnumRemap": {
      const sourceIdx = resolveFieldRef(inputDesc, e.source);
      return remapRecursiveEnumValue(
        resolved(e.source),
        inputDesc.fields()[sourceIdx].valueType,
        outputDesc.fields()[outputIdx].valueType,
        e.remaps,
        "root",
      );
    }
  }
}

export function remapEnumTag(value: Value, tags: (number | undefined)[]): Value {
  switch (value.kind) {
    case "EnumTag": {
      const mapped = tags[value.value];
      if (mapped === undefined) throw IvmRuntimeError.EnumTagProjectionAbsent(value.value);
      return { kind: "EnumTag", value: mapped };
    }
    case "Nullable":
      return value.value === undefined
        ? value
        : { kind: "Nullable", value: remapEnumTag(value.value, tags) };
    default:
      throw IvmRuntimeError.EnumTagProjectionNonEnum();
  }
}

export function remapEnum(value: Value, tags: (number | undefined)[]): Value {
  switch (value.kind) {
    case "Enum": {
      const tag = value.value.tag();
      const mapped = tags[tag];
      if (mapped === undefined) throw IvmRuntimeError.EnumProjectionAbsent(tag);
      return { kind: "Enum", value: EnumValue.new(mapped, value.value.intoRecord()) };
    }
    case "Nullable":
      return value.value === undefined
        ? value
        : { kind: "Nullable", value: remapEnum(value.value, tags) };
    default:
      throw IvmRuntimeError.EnumProjectionNonEnum();
  }
}

/**
 * Re-encode an arbitrary user value across two descriptors whose enum registries use different compact tags.
 * A value operation rather than a byte splice: descriptor equality does not imply a physical enum tag means the same.
 */
export function remapRecursiveEnumValue(
  value: Value,
  source: ValueType,
  target: ValueType,
  remaps: RecursiveEnumRemaps,
  path: string,
): Value {
  const mismatch = () => IvmRuntimeError.RecursiveEnumProjectionDescriptorMismatch(path);
  const k = value.kind;
  if (k === "EnumTag" && source.kind === "EnumTag" && target.kind === "EnumTag") {
    const mapped = remaps.scalar.get(path)?.[value.value];
    if (mapped === undefined) throw IvmRuntimeError.EnumTagProjectionAbsent(value.value);
    return { kind: "EnumTag", value: mapped };
  }
  if (k === "Nullable" && source.kind === "Nullable" && target.kind === "Nullable") {
    if (value.value === undefined) return { kind: "Nullable" };
    // Jazz storage adds one nullable carrier around every user cell; that carrier is not an authored enum
    // occurrence. An authored nullable enum does have a `path/nullable` entry, preferred when present.
    const nullablePath = `${path}/nullable`;
    const childPath = remapPathWithEnumOccurrenceBelow(remaps, nullablePath) ? nullablePath : path;
    return {
      kind: "Nullable",
      value: remapRecursiveEnumValue(value.value, source.inner, target.inner, remaps, childPath),
    };
  }
  if (k === "Array" && source.kind === "Array" && target.kind === "Array") {
    return {
      kind: "Array",
      values: value.values.map((v: Value) =>
        remapRecursiveEnumValue(v, source.inner, target.inner, remaps, `${path}/array`),
      ),
    };
  }
  if (
    k === "Tuple" &&
    source.kind === "Tuple" &&
    target.kind === "Tuple" &&
    value.values.length === source.items.length &&
    source.items.length === target.items.length
  ) {
    return {
      kind: "Tuple",
      values: value.values.map((v: Value, i: number) =>
        remapRecursiveEnumValue(v, source.items[i], target.items[i], remaps, `${path}/tuple/${i}`),
      ),
    };
  }
  if (
    k === "Record" &&
    source.kind === "Record" &&
    target.kind === "Record" &&
    source.descriptor.fields().length === target.descriptor.fields().length
  ) {
    const tf = target.descriptor.fields();
    const values = (value.record.toValues() as Value[]).map((v, i) => {
      const s = source.descriptor.fields()[i];
      if (s.name === undefined) throw mismatch();
      return remapRecursiveEnumValue(
        v,
        s.valueType,
        tf[i].valueType,
        remaps,
        `${path}/record/${s.name}`,
      );
    });
    return {
      kind: "Record",
      record: OwnedRecord.new(target.descriptor.create(values), target.descriptor),
    };
  }
  if (k === "Enum" && source.kind === "Enum" && target.kind === "Enum") {
    const sourceTag: number = value.value.tag();
    const targetTag = remaps.payload.get(path)?.[sourceTag];
    if (targetTag === undefined) throw IvmRuntimeError.EnumProjectionAbsent(sourceTag);
    const sourceCase = source.schema.case(sourceTag);
    const targetCase = target.schema.case(targetTag);
    if (sourceCase.payload.fields().length !== targetCase.payload.fields().length) throw mismatch();
    const semanticChildRoot = remaps.payloadChildren.get(path)?.[sourceTag];
    // Older callers without nested payload occurrences keep the historic local-tag spelling.
    const childRoot = semanticChildRoot ?? `${path}/case/${sourceTag}`;
    const tf = targetCase.payload.fields();
    const values = (value.value.record().toValues() as Value[]).map((v, i) => {
      const s = sourceCase.payload.fields()[i];
      if (s.name === undefined) throw mismatch();
      return remapRecursiveEnumValue(
        v,
        s.valueType,
        tf[i].valueType,
        remaps,
        semanticChildRoot !== undefined
          ? `${childRoot}/record/${s.name}`
          : `${childRoot}/${s.name}`,
      );
    });
    return {
      kind: "Enum",
      value: EnumValue.new(
        targetTag,
        OwnedRecord.new(targetCase.payload.create(values), targetCase.payload),
      ),
    };
  }
  if (structEq(source, target)) return value;
  throw mismatch();
}

function remapPathWithEnumOccurrenceBelow(remaps: RecursiveEnumRemaps, path: string): boolean {
  const below = `${path}/`;
  return [remaps.scalar, remaps.payload, remaps.payloadChildren].some((m) =>
    [...m.keys()].some((c) => c === path || c.startsWith(below)),
  );
}

export function projectionUsesRawCopy(
  expressions: ProjectionExpr[],
  mapping: [number, number][],
  outputDesc: RecordDescriptor,
): boolean {
  const n = outputDesc.fields().length;
  // Legacy/validation-only path: normal lowering always fills expressions.
  if (!expressions.length) return mapping.length === n;
  return (
    expressions.length === n &&
    mapping.length === n &&
    expressions.every((e) => e.expression.kind === "Field")
  );
}

export function rawProjectionFields(
  project: MapProjectOp,
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
): PreparedProjection | undefined {
  const out = outputDesc.fields();
  const validateCopy = (sourceType: ValueType, outputIdx: number) => {
    if (!structEq(sourceType, out[outputIdx].valueType))
      throw IvmRuntimeError.RecordEncoding(RecordsError.TypeMismatch(out[outputIdx].valueType));
  };
  if (!project.expressions.length) {
    if (project.mapping.length !== out.length) return undefined;
    const fields = project.mapping.map(([source, sourceIdx], outputIdx) => {
      if (source !== 0) throw IvmRuntimeError.UnsupportedOperator();
      const field = inputDesc.fields()[sourceIdx];
      if (!field) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(sourceIdx);
      validateCopy(field.valueType, outputIdx);
      return { kind: "Copy", sourceIdx };
    });
    return PreparedProjection.new(inputDesc, outputDesc, fields);
  }
  if (project.expressions.length !== out.length) return undefined;
  const fields = project.expressions.map((expr, outputIdx): RawProjectionField => {
    const e = expr.expression;
    switch (e.kind) {
      case "TemplateArgument":
        throw IvmRuntimeError.UnsupportedOperator();
      case "Field": {
        const sourceIdx = resolveFieldRef(inputDesc, e.source);
        validateCopy(inputDesc.fields()[sourceIdx].valueType, outputIdx);
        return { kind: "Copy", sourceIdx };
      }
      case "Nullable":
      case "NullableFlat": {
        const sourceIdx = resolveFieldRef(inputDesc, e.source);
        const sourceType: ValueType = inputDesc.fields()[sourceIdx].valueType;
        if (e.kind === "NullableFlat" && sourceType.kind === "Nullable") {
          validateCopy(sourceType, outputIdx);
          return { kind: "Copy", sourceIdx };
        }
        validateCopy({ kind: "Nullable", inner: sourceType }, outputIdx);
        return { kind: "WrapNullable", sourceIdx };
      }
      case "RecordField": {
        const sourceIdx = resolveFieldRef(inputDesc, e.source);
        const steps: [RecordDescriptor, number][] = [[inputDesc, sourceIdx]];
        let valueType: ValueType = inputDesc.fields()[sourceIdx].valueType;
        for (const name of e.path) {
          if (valueType.kind !== "Record") throw IvmRuntimeError.UnsupportedOperator();
          const index = valueType.descriptor.fieldIndex(name);
          if (index === undefined) throw IvmRuntimeError.UnsupportedOperator();
          steps.push([valueType.descriptor, index]);
          valueType = valueType.descriptor.fields()[index].valueType;
        }
        validateCopy(valueType, outputIdx);
        return { kind: "Nested", path: steps };
      }
      case "Null":
        return preparedConstantField(outputDesc, outputIdx, { kind: "Nullable" });
      case "Literal":
      case "TypedLiteral":
        return preparedConstantField(outputDesc, outputIdx, literalToValue(e.value));
      case "EnumTagRemap":
      case "EnumRemap":
      case "RecursiveEnumRemap":
        return { kind: "Evaluate" };
    }
  });
  return PreparedProjection.new(inputDesc, outputDesc, fields);
}

function preparedConstantField(
  output: RecordDescriptor,
  index: number,
  value: Value,
): RawProjectionField {
  try {
    return {
      kind: "Encoded",
      bytes: encodeSingleFieldValue(value, output.fields()[index].valueType),
    };
  } catch (error) {
    return { kind: "Error", error };
  }
}

export function encodeProjectionFieldValue(
  outputDesc: RecordDescriptor,
  fieldIdx: number,
  value: Value,
): Uint8Array {
  const field = outputDesc.fields()[fieldIdx];
  if (!field) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(fieldIdx);
  return encodeSingleFieldValue(value, field.valueType);
}

const exactNameIndex = (descriptor: RecordDescriptor, name: string): number | undefined => {
  const i = (descriptor.fields() as DescriptorField[]).findIndex((f) => f.name === name);
  return i < 0 ? undefined : i;
};

export function resolveFieldRef(descriptor: RecordDescriptor, field: FieldRef): number {
  switch (field.kind) {
    case "Name": {
      const i = resolveFieldName(descriptor, field.name);
      if (i === undefined) throw IvmRuntimeError.GraphFieldNotFound(field.name);
      return i;
    }
    case "StoredName": {
      const i = exactNameIndex(descriptor, field.name);
      if (i === undefined) throw IvmRuntimeError.GraphFieldNotFound(field.name);
      return i;
    }
    case "Resolved":
      if (field.index < descriptor.fields().length) return field.index;
      throw IvmRuntimeError.GraphFieldIndexOutOfBounds(field.index);
  }
}

export function tryResolveFieldRef(
  descriptor: RecordDescriptor,
  field: FieldRef,
): number | undefined {
  try {
    return resolveFieldRef(descriptor, field);
  } catch {
    return undefined;
  }
}

export function resolveFieldName(descriptor: RecordDescriptor, name: string): number | undefined {
  return descriptor.fieldIndex(name) ?? exactNameIndex(descriptor, name);
}

export function fieldRefName(descriptor: RecordDescriptor, field: FieldRef): string {
  switch (field.kind) {
    case "Name":
      return field.name;
    case "StoredName": {
      const index = resolveFieldRef(descriptor, field);
      const resolved = descriptor.fields()[index];
      // PlanExpr still carries names. Only emit a spelling that its resolver binds back to this exact field.
      const candidates = [resolved.name, resolved.logicalName()].filter(
        (n) => n !== undefined,
      ) as string[];
      const found = candidates.find((n) => resolveFieldName(descriptor, n) === index);
      if (found === undefined)
        throw IvmRuntimeError.AmbiguousGraphFieldReference(FieldRef.displayName(field));
      return found;
    }
    case "Resolved":
      return fieldNameAt(descriptor, field.index);
  }
}

export function fieldNameAt(descriptor: RecordDescriptor, index: number): string {
  const field = descriptor.fields()[index];
  if (!field) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(index);
  if (field.name === undefined) throw IvmRuntimeError.GraphFieldNotFound(`#${index}`);
  return field.name;
}

const copyField = (f: DescriptorField, valueType = f.valueType): DescriptorField => ({
  name: f.name,
  identity: f.identity,
  valueType,
});

export function unwrapNullableDescriptor(
  input: RecordDescriptor,
  fieldIdx: number,
): RecordDescriptor {
  return RecordDescriptor.newWithFields(
    (input.fields() as DescriptorField[]).map((f, i) =>
      copyField(
        f,
        i === fieldIdx && f.valueType.kind === "Nullable" ? f.valueType.inner : f.valueType,
      ),
    ),
  );
}

export function unnestDescriptor(
  input: RecordDescriptor,
  arrayFieldIdx: number,
  elementField: string,
): RecordDescriptor {
  const arrayField = input.fields()[arrayFieldIdx];
  if (!arrayField) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(arrayFieldIdx);
  if (arrayField.valueType.kind !== "Array") throw IvmRuntimeError.UnsupportedOperator();
  const fields = (input.fields() as DescriptorField[]).map((f) => copyField(f));
  fields.push(DescriptorField.new(elementField, arrayField.valueType.inner));
  return RecordDescriptor.newWithFields(fields);
}

export function variantProjectDescriptor(
  input: RecordDescriptor,
  field: FieldRef,
  kase: string,
): RecordDescriptor {
  const fieldIdx = resolveFieldRef(input, field);
  const enumField = input.fields()[fieldIdx];
  if (!enumField) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(fieldIdx);
  if (enumField.valueType.kind !== "Enum")
    throw IvmRuntimeError.VariantProjectFieldTypeMismatch(fieldRefName(input, field));
  const schema = enumField.valueType.schema;
  return schema.case(schema.tag(kase)).payload;
}

export function aggregateDescriptor(
  input: RecordDescriptor,
  groupCols: FieldRef[],
  aggregates: AggregateExpr[],
): RecordDescriptor {
  const fields: DescriptorField[] = [];
  for (const groupCol of groupCols) {
    const fieldIdx = resolveFieldRef(input, groupCol);
    const field = input.fields()[fieldIdx];
    if (!field) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(fieldIdx);
    // A reference may use a logical spelling to avoid a carrier collision; output keeps the physical name.
    fields.push(
      DescriptorField.new(fieldNameAt(input, fieldIdx), field.valueType).withIdentity(
        field.identity ?? { kind: "Name", name: field.name ?? fieldRefName(input, groupCol) },
      ),
    );
  }
  aggregates.forEach((aggregate, index) => {
    const name = aggregate.outputName ?? `aggregate_${index}`;
    fields.push(
      DescriptorField.new(name, aggregateOutputType(input, aggregate)).withIdentity(
        aggregate.outputIdentity ?? { kind: "Name", name },
      ),
    );
  });
  return RecordDescriptor.newWithFields(fields);
}

function aggregateOutputType(input: RecordDescriptor, aggregate: AggregateExpr): ValueType {
  switch (aggregate.function) {
    case "Count":
      return { kind: "U64" };
    case "Avg":
      return { kind: "Nullable", inner: { kind: "F64" } };
    case "Sum": {
      const t = aggregateExprValueType(input, aggregate);
      if (!["U8", "U16", "U32", "U64", "I32", "I64", "F64"].includes(nonNullableType(t).kind))
        throw IvmRuntimeError.UnsupportedOperator();
      return nullableType(t);
    }
    case "Min":
    case "Max":
      return nullableType(aggregateExprValueType(input, aggregate));
  }
}

function aggregateExprValueType(input: RecordDescriptor, aggregate: AggregateExpr): ValueType {
  const expr = aggregate.expression;
  if (!expr) throw IvmRuntimeError.UnsupportedOperator();
  switch (expr.kind) {
    case "Literal": {
      const t = literalValueType(expr.value);
      if (!t) throw IvmRuntimeError.UnsupportedOperator();
      return t;
    }
    case "Null":
      return expr.valueType;
    default: {
      const name = "name" in expr ? expr.name : expr.field;
      const fieldIdx = resolveFieldName(input, name);
      if (fieldIdx === undefined) throw IvmRuntimeError.GraphFieldNotFound(name);
      const field = input.fields()[fieldIdx];
      if (!field) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(fieldIdx);
      return field.valueType;
    }
  }
}

const nonNullableType = (t: ValueType): ValueType => (t.kind === "Nullable" ? t.inner : t);
/** Used for converting non-nullable column types to nullable column types when aggregating. */
const nullableType = (t: ValueType): ValueType => ({ kind: "Nullable", inner: nonNullableType(t) });

let indexDescriptor: RecordDescriptor | undefined;
export function indexRecordDescriptor(): RecordDescriptor {
  return (indexDescriptor ??= RecordDescriptor.new([
    ["key", { kind: "Bytes" }],
    ["value", { kind: "Bytes" }],
  ]));
}

export function variantProjectionTargetName(target: VariantProjectionTarget): string {
  return target.name;
}

export function schemaIndexInputFields(table: TableSchema, index: IndexSchema): string[] {
  const primaryKey = table.primaryKey;
  if (!primaryKey) throw IvmRuntimeError.MissingPrimaryKey(table.name);
  const fields: string[] = [];
  for (const field of [
    ...index.columns,
    ...primaryKey.columns.map((c: any) => c.column),
  ] as string[]) {
    // The catalogue descriptor has exactly one field per column name.
    if (!table.columns.some((c: any) => c.name === field))
      throw IvmRuntimeError.GraphFieldNotFound(field);
    if (!fields.includes(field)) fields.push(field);
  }
  return fields;
}

export function schemaIndexInputDescriptor(
  table: TableSchema,
  index: IndexSchema,
): RecordDescriptor {
  const catalogue = table.recordSchema();
  return projectDescriptor(
    catalogue,
    schemaIndexInputFields(table, index).map(ProjectFieldCtor.named),
  );
}

export function applyIndexBy(
  indexBy: IndexByOp,
  inputDescriptor: RecordDescriptor,
  inputDeltas: RecordDelta[],
): RecordDelta[] {
  const deltas: RecordDelta[] = [];
  const scalarKeyFields = indexKeyFieldsAreScalar(indexBy, inputDescriptor);
  const push = (key: Uint8Array, value: Uint8Array, weight: number) =>
    deltas.push({
      record: indexRecordDescriptor().create([
        { kind: "Bytes", value: key },
        { kind: "Bytes", value },
      ]),
      weight,
    });
  for (const delta of inputDeltas) {
    const value = indexBy.storeValue
      ? primaryKeyValueBytes(inputDescriptor, delta.raw(), indexBy.valueFields)
      : new Uint8Array();
    const keys = scalarKeyFields
      ? [scalarIndexKey(indexBy, inputDescriptor, delta.raw())]
      : indexKeys(indexBy, inputDescriptor, delta.raw());
    for (const key of keys) {
      if (indexBy.scan && !keyMatchesStaticScan(key, indexBy.scan)) continue;
      push(key, value, delta.weight);
    }
  }
  return deltas;
}

function indexKeyFieldsAreScalar(indexBy: IndexByOp, inputDescriptor: RecordDescriptor): boolean {
  for (const fieldIdx of indexBy.keyFields) {
    const field = inputDescriptor.fields()[fieldIdx];
    if (!field) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(fieldIdx);
    const t: ValueType = field.valueType;
    if (t.kind === "Array" || (t.kind === "Nullable" && t.inner.kind === "Array")) return false;
  }
  return true;
}

function scalarIndexKey(
  indexBy: IndexByOp,
  inputDescriptor: RecordDescriptor,
  record: Uint8Array,
): Uint8Array {
  const key: number[] = [];
  for (const fieldIdx of indexBy.keyFields)
    encodeRecordFieldKeyPart(key, inputDescriptor, record, fieldIdx);
  if (indexBy.appendValueToKey) {
    key.push(0xff, ...primaryKeyValueBytes(inputDescriptor, record, indexBy.valueFields));
  }
  return Uint8Array.from(key);
}

function indexKeys(
  indexBy: IndexByOp,
  inputDescriptor: RecordDescriptor,
  record: Uint8Array,
): Uint8Array[] {
  let keys: Uint8Array[] = [new Uint8Array()];
  for (const fieldIdx of indexBy.keyFields) {
    const parts = recordFieldKeyParts(inputDescriptor, record, fieldIdx);
    if (!parts.length) return [];
    const seen = new Set<string>();
    const next: Uint8Array[] = [];
    for (const key of keys) {
      for (const part of parts) {
        const k = concatBytes(key, part);
        const hex = toHex(k);
        if (!seen.has(hex)) {
          seen.add(hex);
          next.push(k);
        }
      }
    }
    keys = next;
  }
  if (indexBy.appendValueToKey) {
    const value = primaryKeyValueBytes(inputDescriptor, record, indexBy.valueFields);
    // Non-unique indices append the primary key so equal index values remain distinct and ordered for range scans.
    keys = keys.map((k) => concatBytes(k, Uint8Array.of(0xff), value));
  }
  return keys;
}

export type StaticScanBounds =
  | { kind: "Prefix"; prefix: Uint8Array }
  | { kind: "Range"; start: Uint8Array; end: Uint8Array };

export function scanBounds(scan: StaticScanSpec): StaticScanBounds {
  switch (scan.kind) {
    case "Point":
    case "Prefix":
      return { kind: "Prefix", prefix: staticScanKey(scan.values) };
    case "PrefixLimit":
    case "ReversePrefixLimit":
      return { kind: "Prefix", prefix: staticScanKey(scan.prefix) };
    case "Range":
      return { kind: "Range", start: staticScanKey(scan.start), end: staticScanKey(scan.end) };
  }
}

export function scanMaxItems(scan: StaticScanSpec | undefined): number | undefined {
  return scan?.kind === "PrefixLimit" || scan?.kind === "ReversePrefixLimit"
    ? scan.maxItems
    : undefined;
}

export function scanReversed(scan: StaticScanSpec | undefined): boolean {
  return scan?.kind === "ReversePrefixLimit";
}

function staticScanKey(values: LiteralValue[]): Uint8Array {
  const key: number[] = [];
  for (const value of values) encodeKeyPart(key, literalToValue(value));
  return Uint8Array.from(key);
}

export function keyMatchesStaticScan(key: Uint8Array, scan: StaticScanSpec): boolean {
  const bounds = scanBounds(scan);
  if (bounds.kind === "Prefix")
    return (
      key.length >= bounds.prefix.length &&
      equalBytes(key.subarray(0, bounds.prefix.length), bounds.prefix)
    );
  return compareBytes(bounds.start, key) <= 0 && compareBytes(key, bounds.end) < 0;
}

export function persistedIndexScanBounds(
  indexId: number,
  scan: StaticScanSpec | undefined,
): StaticScanBounds {
  // The ordered part encodings are prefix-free and order-preserving, so the logical key follows the id directly.
  const base = durableIndexKeyPrefix(indexId);
  if (!scan) return { kind: "Prefix", prefix: base };
  const bounds = scanBounds(scan);
  return bounds.kind === "Prefix"
    ? { kind: "Prefix", prefix: concatBytes(base, bounds.prefix) }
    : { kind: "Range", start: concatBytes(base, bounds.start), end: concatBytes(base, bounds.end) };
}
