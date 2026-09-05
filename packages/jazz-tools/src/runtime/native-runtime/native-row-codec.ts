import type {
  ColumnDescriptor,
  ColumnType,
  NativeTerminalRootLayout,
  Value,
  WasmRow,
} from "../../drivers/types.js";
import { isProvenanceMagicColumn } from "../../magic-columns.js";
import { decodeCanonicalAuthorSubjectBytes } from "../author-id.js";
import { exactSignedI64 } from "./exact-integer.js";

const textDecoder = new TextDecoder();
const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true });

export type ValueType = {
  tag: number;
  internal?: { tag: number; kind?: number };
  inner?: ValueType;
  members?: ValueType[];
  record?: DescriptorField[];
  enumSchema?: EnumSchema;
};
export type DescriptorField = { name?: string; valueType: ValueType };
export type EnumSchema = {
  registryId?: number;
  name: string;
  variants?: string[];
  cases?: { name: string; payload: DescriptorField[] }[];
};
export type NativeRow = { rowId: Uint8Array; deleted: boolean; raw: Uint8Array };
export type NativeStoredColumnDescriptorField = {
  id: number;
  outputName: string;
  valueType: ValueType;
  kind: "stored-column";
};
export type NativeResultFieldDescriptorField = {
  name: string;
  valueType: ValueType;
  kind: "result-field";
};
export type NativeHiddenMetadataDescriptorField = {
  name: string;
  valueType: ValueType;
  kind: "hidden-metadata";
};
/** Explicit Rust-to-JavaScript provenance for one descriptor field. */
export type NativeRowDescriptorField =
  | NativeStoredColumnDescriptorField
  | NativeResultFieldDescriptorField
  | NativeHiddenMetadataDescriptorField;

/**
 * The producer owns a publication field's public identity. Hidden metadata is
 * carried in the same record only for decoding and never has a public binding.
 */
export function nativeRowDescriptorPublicName(field: NativeRowDescriptorField): string | undefined {
  if (field.kind === "hidden-metadata") return undefined;
  return field.kind === "stored-column" ? field.outputName : field.name;
}
export type NativeRowBatch = {
  table: string;
  descriptor: NativeRowDescriptorField[];
  rows: NativeRow[];
};
export type NativeRemovedRow = { table: string; rowId: Uint8Array };
export type NativeSubscriptionDelta = {
  added: NativeRowBatch[];
  updated: NativeRowBatch[];
  removed: NativeRemovedRow[];
  addedOccurrenceKeys: Uint8Array[];
  updatedOccurrenceKeys: Uint8Array[];
  removedOccurrenceKeys: Uint8Array[];
  addedIndices: number[];
  updatedPreviousIndices: number[];
  updatedIndices: number[];
  removedIndices: number[];
};
export type NativeRelationSubscriptionSnapshot = {
  rootCount: number;
  rows: NativeRowBatch[];
};

type PostcardReaderLike = {
  string(): string;
  u64(): number;
  option<T>(readValue: (reader: PostcardReaderLike) => T): T | undefined;
  bytes(): Uint8Array;
  bool(): boolean;
  readVec<T>(readItem: (reader: PostcardReaderLike) => T): T[];
  done(): boolean;
};

type PostcardWriterLike = {
  u64(value: number): void;
  vec(writeItem: (writer: PostcardWriterLike, index: number) => void, length: number): void;
  some(writeValue: (writer: PostcardWriterLike) => void): void;
  string(value: string): void;
  enumUnit(tag: number): void;
  bytes(value: Uint8Array): void;
  u32Le(value: number): void;
  finish(): Uint8Array;
};

export function readNativeRowBatch(reader: PostcardReaderLike): NativeRowBatch {
  return {
    table: reader.string(),
    descriptor: readNativeRowDescriptor(reader),
    rows: reader.readVec((rowReader) => ({
      rowId: rowReader.bytes(),
      deleted: rowReader.bool(),
      raw: rowReader.bytes(),
    })),
  };
}

export function writeNativeRowDescriptor(
  writer: PostcardWriterLike,
  descriptor: readonly NativeRowDescriptorField[],
): void {
  writer.vec((fieldWriter, index) => {
    const field = descriptor[index]!;
    fieldWriter.u64(field.kind === "stored-column" ? 0 : field.kind === "result-field" ? 1 : 2);
    if (field.kind === "stored-column") {
      fieldWriter.u64(field.id);
      fieldWriter.string(field.outputName);
    } else {
      fieldWriter.string(field.name);
    }
    writeValueType(fieldWriter, field.valueType);
  }, descriptor.length);
}

export function readNativeRowDescriptor(reader: PostcardReaderLike): NativeRowDescriptorField[] {
  return reader.readVec((fieldReader) => {
    const kindTag = fieldReader.u64();
    if (kindTag === 0) {
      return {
        kind: "stored-column",
        id: fieldReader.u64(),
        outputName: fieldReader.string(),
        valueType: readValueType(fieldReader),
      };
    }
    if (kindTag === 1) {
      return {
        kind: "result-field",
        name: fieldReader.string(),
        valueType: readValueType(fieldReader),
      };
    }
    if (kindTag === 2) {
      return {
        kind: "hidden-metadata",
        name: fieldReader.string(),
        valueType: readValueType(fieldReader),
      };
    }
    return invalidNativeRowDescriptorFieldKind(kindTag);
  });
}

function invalidNativeRowDescriptorFieldKind(kindTag: number): never {
  throw new Error(`unknown native row descriptor field kind ${kindTag}`);
}

export function readNativeSubscriptionDelta(reader: PostcardReaderLike): NativeSubscriptionDelta {
  const delta = {
    added: reader.readVec(readNativeRowBatch),
    updated: reader.readVec(readNativeRowBatch),
    removed: reader.readVec(readNativeRemovedRow),
    addedOccurrenceKeys: reader.readVec((keyReader) => readResultKey(keyReader)),
    updatedOccurrenceKeys: reader.readVec((keyReader) => readResultKey(keyReader)),
    removedOccurrenceKeys: reader.readVec((keyReader) => readResultKey(keyReader)),
    addedIndices: reader.readVec((indexReader) => indexReader.u64()),
    updatedPreviousIndices: reader.readVec((indexReader) => indexReader.u64()),
    updatedIndices: reader.readVec((indexReader) => indexReader.u64()),
    removedIndices: reader.readVec((indexReader) => indexReader.u64()),
  };
  const rowCount = (batches: NativeRowBatch[]) =>
    batches.reduce((count, batch) => count + batch.rows.length, 0);
  if (
    delta.addedOccurrenceKeys.length !== rowCount(delta.added) ||
    delta.updatedOccurrenceKeys.length !== rowCount(delta.updated) ||
    delta.removedOccurrenceKeys.length !== delta.removed.length ||
    delta.addedIndices.length !== rowCount(delta.added) ||
    delta.updatedPreviousIndices.length !== rowCount(delta.updated) ||
    delta.updatedIndices.length !== rowCount(delta.updated) ||
    delta.removedIndices.length !== delta.removed.length
  ) {
    throw new Error("subscription occurrence sidecar length mismatch");
  }
  assertReaderDone(reader, "subscription delta");
  return delta;
}

function readResultKey(reader: PostcardReaderLike): Uint8Array {
  const key = reader.bytes();
  if (key[0] !== 1 || !validTypedResultKey(key.subarray(1))) {
    throw new Error("malformed ResultKey v1");
  }
  return key;
}

function validTypedResultKey(bytes: Uint8Array): boolean {
  const readU32 = (offset: number) =>
    ((bytes[offset]! << 24) |
      (bytes[offset + 1]! << 16) |
      (bytes[offset + 2]! << 8) |
      bytes[offset + 3]!) >>>
    0;
  if (bytes.length < 24) return false;
  let cursor = 16;
  const joined = readU32(cursor);
  cursor += 4;
  if (joined > 256 || cursor + joined * 16 + 4 > bytes.length) return false;
  cursor += joined * 16;
  const discriminators = readU32(cursor);
  cursor += 4;
  if (discriminators > joined + 1) return false;
  let previousPosition = -1;
  for (let index = 0; index < discriminators; index++) {
    if (cursor + 8 > bytes.length) return false;
    const position = readU32(cursor);
    const length = readU32(cursor + 4);
    cursor += 8;
    if (
      position > joined ||
      position <= previousPosition ||
      length === 0 ||
      length > 4096 ||
      cursor + length > bytes.length ||
      !isValidUtf8(bytes.subarray(cursor, cursor + length))
    ) {
      return false;
    }
    previousPosition = position;
    cursor += length;
  }
  return cursor === bytes.length;
}

function isValidUtf8(bytes: Uint8Array): boolean {
  try {
    fatalUtf8Decoder.decode(bytes);
    return true;
  } catch {
    return false;
  }
}

export function readNativeRelationSubscriptionSnapshot(
  reader: PostcardReaderLike,
): NativeRelationSubscriptionSnapshot {
  const rootCount = reader.u64();
  const rows = reader.readVec(readNativeRowBatch);
  assertReaderDone(reader, "relation snapshot");
  return { rootCount, rows };
}

function assertReaderDone(reader: PostcardReaderLike, payload: string): void {
  if (!reader.done()) throw new Error(`${payload} has trailing postcard bytes`);
}

export function readNativeRemovedRow(reader: PostcardReaderLike): NativeRemovedRow {
  return {
    table: reader.string(),
    rowId: reader.bytes(),
  };
}

export function writeDescriptor(writer: PostcardWriterLike, descriptor: DescriptorField[]): void {
  writer.vec((field, index) => {
    field.some((nameWriter) => nameWriter.string(descriptor[index].name ?? ""));
    writeValueType(field, descriptor[index].valueType);
  }, descriptor.length);
}

export function readDescriptor(reader: PostcardReaderLike): DescriptorField[] {
  return reader.readVec((fieldReader) => ({
    name: fieldReader.option((nameReader) => nameReader.string()),
    valueType: readValueType(fieldReader),
  }));
}

export function writeValueType(writer: PostcardWriterLike, valueType: ValueType): void {
  writer.enumUnit(valueType.tag);
  if (valueType.tag === 10) {
    if (!valueType.internal) throw new Error("missing physical type for ValueType::Internal");
    writer.enumUnit(valueType.internal.tag);
    if (valueType.internal.tag === 2) {
      if (valueType.internal.kind == null) throw new Error("missing stored-scalar kind");
      writer.enumUnit(valueType.internal.kind);
    }
    return;
  }
  if (valueType.tag === 12) {
    const enumSchema = valueType.enumSchema;
    const variants = enumSchema?.variants;
    if (!enumSchema || !variants) throw new Error("missing enum schema for ValueType::Enum");
    writer.u64(enumSchema.registryId ?? 0);
    writer.string(enumSchema.name);
    writer.vec((variantWriter, index) => variantWriter.string(variants[index]!), variants.length);
    return;
  }
  if (valueType.tag === 13) {
    const members = valueType.members ?? (valueType.inner ? [valueType.inner] : []);
    writer.vec(
      (memberWriter, index) => writeValueType(memberWriter, members[index]!),
      members.length,
    );
    return;
  }
  if (valueType.tag === 14 || valueType.tag === 15) {
    if (!valueType.inner) throw new Error(`missing inner value type for tag ${valueType.tag}`);
    writeValueType(writer, valueType.inner);
    return;
  }
  if (valueType.tag === 16) {
    if (!valueType.record) throw new Error("missing inline record descriptor for tag 16");
    writeDescriptor(writer, valueType.record);
    return;
  }
  if (valueType.tag === 17) {
    const enumSchema = valueType.enumSchema;
    const cases = enumSchema?.cases;
    if (!enumSchema || !cases) throw new Error("missing cases for ValueType::Enum");
    writer.u64(enumSchema.registryId ?? 0);
    writer.string(enumSchema.name);
    writer.vec((caseWriter, index) => {
      const enumCase = cases[index]!;
      caseWriter.string(enumCase.name);
      writeDescriptor(caseWriter, enumCase.payload);
    }, cases.length);
  }
}

export function readValueType(reader: PostcardReaderLike): ValueType {
  const tag = reader.u64();
  if (tag === 10) {
    const internalTag = reader.u64();
    return {
      tag,
      internal: { tag: internalTag, kind: internalTag === 2 ? reader.u64() : undefined },
    };
  }
  if (tag === 12) {
    return {
      tag,
      enumSchema: {
        registryId: reader.u64(),
        name: reader.string(),
        variants: reader.readVec((variantReader) => variantReader.string()),
      },
    };
  }
  if (tag === 14 || tag === 15) {
    return { tag, inner: readValueType(reader) };
  }
  if (tag === 13) {
    const members = reader.readVec(readValueType);
    return { tag, members, inner: members[0] };
  }
  if (tag === 16) {
    return { tag, record: readDescriptor(reader) };
  }
  if (tag === 17) {
    return {
      tag,
      enumSchema: {
        registryId: reader.u64(),
        name: reader.string(),
        cases: reader.readVec((caseReader) => ({
          name: caseReader.string(),
          payload: readDescriptor(caseReader),
        })),
      },
    };
  }
  return { tag };
}

export function createRecord(descriptor: DescriptorField[], values: Uint8Array[]): Uint8Array {
  const layout = recordLayout(descriptor);
  return createRecordWithLayout(layout, values);
}

function createRecordWithLayout(
  layout: {
    fields: FieldLayout[];
    fixed: Extract<FieldLayout, { kind: "fixed" }>[];
    variable: Extract<FieldLayout, { kind: "variable" }>[];
    fixedSize: number;
  },
  values: Uint8Array[],
): Uint8Array {
  const staticChunks = layout.fixed.map((field) => values[field.logicalIndex]);
  const variableChunks = layout.variable.map((field) => values[field.logicalIndex]);
  const fixed = concatBytes(staticChunks);
  const offsets = new OffsetWriter();
  let nextOffset = fixed.length + Math.max(0, variableChunks.length - 1) * 4;
  for (const chunk of variableChunks.slice(0, -1)) {
    nextOffset += chunk.length;
    offsets.u32Le(nextOffset);
  }
  return concatBytes([fixed, offsets.finish(), ...variableChunks]);
}

export function fieldIndex(descriptor: DescriptorField[], name: string): number {
  const index = descriptor.findIndex((field) => field.name === name);
  if (index < 0) {
    throw new Error(
      `missing ${name} field in [${descriptor.map((field) => field.name ?? "<anonymous>").join(", ")}]`,
    );
  }
  return index;
}

export function decodeRecordBool(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): boolean {
  const bytes = decodeRecordBytes(descriptor, raw, logicalIndex);
  if (bytes.length !== 1) throw new Error(`invalid bool size ${bytes.length}`);
  return bytes[0] !== 0;
}

export function decodeRecordString(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): string {
  return new TextDecoder().decode(decodeRecordBytes(descriptor, raw, logicalIndex));
}

export function decodeRecordBytes(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): Uint8Array {
  const value = decodeRecordValue(descriptor, raw, logicalIndex);
  if (value == null) return new Uint8Array();
  return value;
}

export function decodeNativeRowValues(
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): Value[] {
  const descriptor = descriptorFromColumns(columns);
  return columns.map((column, index) => {
    const bytes = decodeRecordValue(descriptor, raw, index);
    if (bytes == null) return { type: "Null" };
    return decodeBytes(column.column_type, bytes);
  });
}

export function decodeNativeRowValuesByColumn(
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): Map<string, Value> {
  const descriptor = descriptorFromColumns(columns);
  const valuesByColumn = new Map<string, Value>();

  for (let i = 0; i < columns.length; i++) {
    const column = columns[i];
    if (!column) continue;
    const bytes = decodeRecordValue(descriptor, raw, i);
    valuesByColumn.set(
      column.name,
      bytes == null ? { type: "Null" } : decodeBytes(column.column_type, bytes),
    );
  }

  return valuesByColumn;
}

export function decodeNativeRow(
  id: string,
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): WasmRow {
  const row = {
    id,
    values: decodeNativeRowValues(columns, raw),
  };
  Object.defineProperty(row, "valuesByColumn", {
    value: decodeNativeRowValuesByColumn(columns, raw),
    enumerable: false,
    configurable: true,
  });
  return row;
}

const terminalRowKeyColumn: ColumnDescriptor = {
  name: "__jazz_terminal_row_key",
  column_type: { type: "Uuid" },
  nullable: false,
};

/** Decode a Groove terminal record, whose first physical field is its row key. */
export function decodeNativeTerminalRow(
  id: string,
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): WasmRow {
  const terminalColumns = [terminalRowKeyColumn, ...columns];
  const decoded = decodeNativeTerminalRowValues(terminalColumns, raw);
  const embeddedKey = decoded[0];
  if (embeddedKey?.type !== "Uuid" || embeddedKey.value !== id) {
    throw new Error(
      `terminal record key ${embeddedKey?.type === "Uuid" ? embeddedKey.value : "<non-uuid>"} does not match addressed key ${id}`,
    );
  }
  const values = decoded.slice(1);
  const valuesByColumn = new Map(columns.map((column, index) => [column.name, values[index]!]));
  const row = { id, values };
  Object.defineProperty(row, "valuesByColumn", {
    value: valuesByColumn,
    enumerable: false,
    configurable: true,
  });
  return row;
}

/**
 * Decode a terminal root from the descriptor emitted with its edit.  Terminal
 * payloads are not all CurrentRow carriers: ordinary roots may be logical
 * non-nullable records while hop/gather roots retain nullable carriers.  The
 * producer descriptor is therefore the sole authority for byte layout.
 */
export function decodeNativeTerminalRowWithDescriptor(
  id: string,
  descriptor: DescriptorField[],
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): WasmRow {
  assertTerminalRootDescriptorCompatible(descriptor, columns);
  assertRecordLayoutIsComplete(descriptor, raw);
  const key = decodeRecordValue(descriptor, raw, 0);
  if (key == null || formatUuid(key) !== id) {
    throw new Error("terminal record key does not match addressed key");
  }
  const values = columns.map((column, index) => {
    const bytes = decodeRecordValue(descriptor, raw, index + 1);
    return bytes == null
      ? ({ type: "Null" } satisfies Value)
      : decodeTerminalColumnBytes(column, bytes, descriptor[index + 1]?.valueType);
  });
  const valuesByColumn = new Map(columns.map((column, index) => [column.name, values[index]!]));
  const row = { id, values };
  Object.defineProperty(row, "valuesByColumn", {
    value: valuesByColumn,
    enumerable: false,
    configurable: true,
  });
  return row;
}

/**
 * Compile the hot terminal-root decoder from an immutable producer-owned
 * layout. The descriptor's field identities and slots are checked once; each
 * operation thereafter carries only the layout ID and packed edit bytes.
 */
export function compileNativeTerminalRootDecoder(
  layout: NativeTerminalRootLayout,
  descriptor: DescriptorField[],
  columns: readonly ColumnDescriptor[],
): (id: string, raw: Uint8Array) => WasmRow {
  assertTerminalRootLayoutCompatible(descriptor, columns, layout);
  const fieldsByName = new Map(layout.publicFields.map((field) => [field.name, field]));
  const slots = columns.map((column) => fieldsByName.get(column.name)!.slot);
  return (id, raw) => {
    assertRecordLayoutIsComplete(descriptor, raw);
    const key = decodeRecordValue(descriptor, raw, layout.rootKeySlot);
    if (key == null || formatUuid(key) !== id) {
      throw new Error("terminal record key does not match addressed key");
    }
    const values = columns.map((column, index) => {
      const slot = slots[index]!;
      const bytes = decodeRecordValue(descriptor, raw, slot);
      return bytes == null
        ? ({ type: "Null" } satisfies Value)
        : decodeTerminalColumnBytes(column, bytes, descriptor[slot]?.valueType);
    });
    const valuesByColumn = new Map(columns.map((column, index) => [column.name, values[index]!]));
    const row = { id, values };
    Object.defineProperty(row, "valuesByColumn", {
      value: valuesByColumn,
      enumerable: false,
      configurable: true,
    });
    return row;
  };
}

function assertTerminalRootLayoutCompatible(
  descriptor: readonly DescriptorField[],
  columns: readonly ColumnDescriptor[],
  layout: NativeTerminalRootLayout,
): void {
  if (layout.carrier !== "CurrentRow" && layout.carrier !== "Logical") {
    throw new Error(`unsupported terminal root carrier ${String(layout.carrier)}`);
  }
  const root = descriptor[layout.rootKeySlot];
  if (
    !Number.isSafeInteger(layout.rootKeySlot) ||
    root?.name !== layout.rootKeyFieldName ||
    root.valueType.tag !== 11 ||
    !isKnownValueType(root.valueType)
  ) {
    throw new Error("terminal root layout key slot does not match its descriptor");
  }
  if (layout.publicFields.length !== columns.length) {
    throw new Error("terminal root layout does not match the public projection");
  }
  const columnsByName = new Map(columns.map((column) => [column.name, column]));
  const seenNames = new Set<string>();
  const seenSlots = new Set<number>([layout.rootKeySlot]);
  for (let index = 0; index < layout.publicFields.length; index++) {
    const field = layout.publicFields[index]!;
    const column = columnsByName.get(field.name);
    const descriptorField = descriptor[field.slot];
    if (
      !column ||
      seenNames.has(field.name) ||
      !Number.isSafeInteger(field.slot) ||
      seenSlots.has(field.slot) ||
      descriptorField?.name !== field.descriptorFieldName ||
      !terminalLayoutValueTypeMatchesColumn(
        descriptorField?.valueType,
        column,
        field.carrier ?? layout.carrier,
      )
    ) {
      throw new Error(
        `terminal root layout does not match the public projection at ${index}: ` +
          `${field.name}/${field.descriptorFieldName}@${field.slot} vs ${column?.name ?? "<missing>"}/` +
          `${descriptorField?.name ?? "<missing>"} (descriptor tag ${descriptorField?.valueType.tag ?? "?"}, ` +
          `nullable ${String(column?.nullable)}, sparse ${String(column?.sparse)})`,
      );
    }
    seenNames.add(field.name);
    seenSlots.add(field.slot);
  }
}

function terminalLayoutValueTypeMatchesColumn(
  valueType: ValueType | undefined,
  column: ColumnDescriptor,
  carrier: NativeTerminalRootLayout["carrier"],
): boolean {
  // `sparse` describes the TS wildcard/storage carrier, not the declared
  // public value. Rust collector descriptors have already removed it.
  const logicalColumn = logicalStorageColumns([column])[0]!;
  // Provenance lives in fixed CurrentRow system fields, not nullable _app_
  // carriers. Author subjects are already canonical text at the native/public
  // boundary; timestamps retain their native scalar storage type.
  if (isProvenanceMagicColumn(column.name)) {
    return terminalValueTypeMatchesColumn(valueType, logicalColumn, false);
  }
  if (carrier === "Logical") {
    return terminalValueTypeMatchesColumn(valueType, logicalColumn, false);
  }
  return (
    valueType?.tag === 15 &&
    valueType.inner !== undefined &&
    terminalValueTypeMatchesColumn(valueType.inner, logicalColumn, false)
  );
}

/**
 * Verify that an operation-supplied terminal descriptor can describe this
 * public projection.  This is intentionally stricter than layout matching:
 * a same-width scalar of the wrong type can otherwise be decoded as a valid,
 * but corrupt, public value.
 */
export function assertTerminalRootDescriptorCompatible(
  descriptor: DescriptorField[],
  columns: readonly ColumnDescriptor[],
): void {
  const publicColumns = logicalStorageColumns(columns);
  const matchesLogical = matchesNamedTerminalLayout(
    descriptor,
    "__jazz_terminal_row_key",
    publicColumns,
    (column) => column.name,
    false,
  );
  const matchesPhysical = matchesNamedTerminalLayout(
    descriptor,
    "__jazz_terminal_row_key",
    columns,
    (column) => column.name,
    false,
  );
  // CurrentRow is a distinct physical layout. Its row key is named row_uuid
  // and its nullable application-cell carriers live in the _app_ namespace.
  // Do not accept a nullable logical descriptor here: doing so would make an
  // arbitrary reordering of same-typed fields indistinguishable from a native
  // CurrentRow record.
  const matchesCurrentRow = matchesNamedTerminalLayout(
    descriptor,
    "row_uuid",
    publicColumns,
    (column) => `_app_${column.name}`,
    true,
  );
  if (!matchesLogical && !matchesPhysical && !matchesCurrentRow) {
    throw new Error("terminal root descriptor does not match the public projection");
  }
}

function matchesNamedTerminalLayout(
  descriptor: readonly DescriptorField[],
  keyName: string,
  columns: readonly ColumnDescriptor[],
  fieldName: (column: ColumnDescriptor) => string,
  forceNullable: boolean,
): boolean {
  return (
    descriptor.length >= columns.length + 1 &&
    descriptor[0]?.name === keyName &&
    descriptor[0]?.valueType.tag === 11 &&
    isKnownValueType(descriptor[0].valueType) &&
    columns.every(
      (column, index) =>
        descriptor[index + 1]?.name === fieldName(column) &&
        terminalValueTypeMatchesColumn(descriptor[index + 1]?.valueType, column, forceNullable),
    )
  );
}

function terminalValueTypeMatchesColumn(
  valueType: ValueType | undefined,
  column: ColumnDescriptor,
  forceNullable: boolean,
): boolean {
  if (!valueType || !isKnownValueType(valueType)) return false;
  if (column.sparse) {
    return (
      valueType.tag === 15 &&
      valueType.inner !== undefined &&
      terminalValueTypeMatchesColumn(
        valueType.inner,
        { ...column, sparse: undefined },
        forceNullable,
      )
    );
  }
  if (forceNullable || column.nullable) {
    return (
      valueType.tag === 15 &&
      valueType.inner !== undefined &&
      terminalValueTypeMatchesColumn(valueType.inner, { ...column, nullable: false }, false)
    );
  }
  switch (column.column_type.type) {
    case "Boolean":
      return valueType.tag === 7;
    case "Integer":
      return valueType.tag === 4;
    case "BigInt":
      return valueType.tag === 5;
    case "Timestamp":
      return valueType.tag === 3;
    case "Double":
      return valueType.tag === 6;
    case "Text":
    case "Json":
    case "Enum":
      return valueType.tag === 8;
    case "EnumPayload": {
      const payloadColumn = column.column_type;
      if (payloadColumn.type !== "EnumPayload") return false;
      const payloadCases = valueType.enumSchema?.cases;
      return (
        valueType.tag === 17 &&
        payloadCases !== undefined &&
        payloadCases.length === payloadColumn.cases.length &&
        payloadCases.every((payloadCase, caseIndex) => {
          const declaredCase = payloadColumn.cases[caseIndex]!;
          return (
            payloadCase.name === declaredCase.name &&
            payloadCase.payload.length === declaredCase.fields.length &&
            payloadCase.payload.every(
              (field, fieldIndex) =>
                field.name === declaredCase.fields[fieldIndex]?.name &&
                terminalValueTypeMatchesColumn(
                  field.valueType,
                  declaredCase.fields[fieldIndex]!,
                  false,
                ),
            )
          );
        })
      );
    }
    case "Uuid":
      return valueType.tag === 11;
    case "Bytea":
      return valueType.tag === 9;
    case "Array":
      return (
        valueType.tag === 14 &&
        valueType.inner !== undefined &&
        terminalValueTypeMatchesColumn(
          valueType.inner,
          { name: column.name, column_type: column.column_type.element, nullable: false },
          false,
        )
      );
    case "Row":
      // Terminal trees retain nested records, while ordinary packed rows use
      // an opaque byte envelope. Both are sanctioned producer representations;
      // any other variable-width type is not.
      return (
        valueType.tag === 9 ||
        (valueType.tag === 16 &&
          valueType.record !== undefined &&
          valueType.record.length === column.column_type.columns.length + 1 &&
          valueType.record[0]?.valueType.tag === 11 &&
          column.column_type.columns.every((nested, index) =>
            terminalValueTypeMatchesColumn(valueType.record?.[index + 1]?.valueType, nested, false),
          ))
      );
  }
}

function decodeTerminalColumnBytes(
  column: ColumnDescriptor,
  bytes: Uint8Array,
  valueType: ValueType | undefined,
): Value {
  if (
    isProvenanceMagicColumn(column.name) &&
    column.column_type.type === "Text" &&
    nonNullableValueType(valueType)?.tag === 8
  ) {
    return { type: "Text", value: decodeProvenanceText(bytes) };
  }
  return decodeTerminalBytes(column.column_type, bytes, column.name);
}

function decodeProvenanceText(bytes: Uint8Array): string {
  return decodeCanonicalAuthorSubjectBytes(bytes);
}

function nonNullableValueType(valueType: ValueType | undefined): ValueType | undefined {
  while (valueType?.tag === 15) valueType = valueType.inner;
  return valueType;
}

function isKnownValueType(valueType: ValueType): boolean {
  switch (valueType.tag) {
    case 0:
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
    case 8:
    case 9:
      return true;
    case 10:
      return (
        valueType.internal !== undefined &&
        (valueType.internal.tag === 0 ||
          valueType.internal.tag === 1 ||
          (valueType.internal.tag === 2 &&
            valueType.internal.kind !== undefined &&
            valueType.internal.kind >= 0 &&
            valueType.internal.kind <= 2))
      );
    case 11:
      return true;
    case 12:
      return valueType.enumSchema?.variants !== undefined;
    case 13:
      return valueType.members !== undefined && valueType.members.every(isKnownValueType);
    case 14:
    case 15:
      return valueType.inner !== undefined && isKnownValueType(valueType.inner);
    case 16:
      return (
        valueType.record !== undefined &&
        valueType.record.every((field) => isKnownValueType(field.valueType))
      );
    case 17:
      return (
        valueType.enumSchema?.cases !== undefined &&
        valueType.enumSchema.cases.every((enumCase) =>
          enumCase.payload.every((field) => isKnownValueType(field.valueType)),
        )
      );
    default:
      return false;
  }
}

function assertRecordLayoutIsComplete(descriptor: DescriptorField[], raw: Uint8Array): void {
  const layout = recordLayout(descriptor);
  const variableCount = layout.variable.length;
  const offsetTableLength = Math.max(0, variableCount - 1) * 4;
  const variableStart = layout.fixedSize + offsetTableLength;
  if (raw.length < variableStart || (variableCount === 0 && raw.length !== layout.fixedSize)) {
    throw new Error("terminal record has trailing or truncated bytes");
  }
  let previous = variableStart;
  for (let index = 0; index < variableCount - 1; index++) {
    const next = readU32Le(raw, layout.fixedSize + index * 4);
    if (next < previous || next > raw.length) {
      throw new Error("terminal record has invalid variable-field offsets");
    }
    previous = next;
  }
}

/**
 * Groove terminal payloads retain `Record` values for nested relation rows.
 * Ordinary packed transport deliberately represents those rows as byte arrays
 * with an id/length envelope instead. Both outer records have the same layout,
 * but decoding a terminal tree through the ordinary path makes the first UUID
 * of a child look like that envelope's flag and length.
 */
function decodeNativeTerminalRowValues(
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): Value[] {
  const descriptor = descriptorFromColumns(columns);
  return columns.map((column, index) => {
    const bytes = decodeRecordValue(descriptor, raw, index);
    if (bytes == null) return { type: "Null" };
    return decodeTerminalColumnBytes(column, bytes, descriptor[index]?.valueType);
  });
}

function decodeTerminalBytes(type: ColumnType, bytes: Uint8Array, columnName?: string): Value {
  switch (type.type) {
    case "Timestamp":
      return { type: "Timestamp", value: decodeNativeTimestamp(bytes, columnName) };
    case "Array":
      return { type: "Array", value: decodeTerminalArray(type.element, bytes) };
    case "Row": {
      if (bytes.byteLength < 16) throw new Error("terminal nested row is missing its physical key");
      const id = formatUuid(bytes.subarray(0, 16));
      return { type: "Row", value: decodeNativeTerminalRow(id, type.columns, bytes) };
    }
    default:
      return decodeBytes(type, bytes);
  }
}

function decodeTerminalArray(elementType: ColumnType, bytes: Uint8Array): Value[] {
  return decodeArrayElements(elementType, bytes, (element) =>
    decodeTerminalBytes(elementType, element),
  );
}

export function encodeNativeRowValues(
  columns: readonly ColumnDescriptor[],
  values: readonly Value[],
): Uint8Array {
  return createNativeRowValueEncoder(columns)(values);
}

export function createNativeRowValueEncoder(
  columns: readonly ColumnDescriptor[],
): (values: readonly Value[]) => Uint8Array {
  const descriptor = descriptorFromColumns(columns);
  const layout = recordLayout(descriptor);
  return (values) => {
    const encoded: Uint8Array[] = [];
    encoded.length = columns.length;
    for (let index = 0; index < columns.length; index += 1) {
      encoded[index] = encodeNativeColumnValue(columns[index], values[index]);
    }
    return createRecordWithLayout(layout, encoded);
  };
}

export function decodeNativeRowObject(
  id: string | undefined,
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): Record<string, unknown> {
  const descriptor = descriptorFromColumns(columns);
  const obj: Record<string, unknown> = {};
  if (id !== undefined) {
    obj.id = id;
  }

  for (let i = 0; i < columns.length; i++) {
    const column = columns[i];
    if (!column) continue;
    const bytes = decodeRecordValue(descriptor, raw, i);
    obj[column.name] =
      bytes == null ? null : decodePlainValue(column.column_type, bytes, column.name);
  }

  return obj;
}

export function decodeRecordValue(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): Uint8Array | null {
  const layout = recordLayout(descriptor);
  return decodeRecordValueWithLayout(descriptor, layout, raw, logicalIndex);
}

export function createRecordValueDecoder(
  descriptor: DescriptorField[],
): (raw: Uint8Array, logicalIndex: number) => Uint8Array | null {
  const layout = recordLayout(descriptor);
  return (raw, logicalIndex) => decodeRecordValueWithLayout(descriptor, layout, raw, logicalIndex);
}

function decodeRecordValueWithLayout(
  descriptor: DescriptorField[],
  layout: {
    fields: FieldLayout[];
    fixed: Extract<FieldLayout, { kind: "fixed" }>[];
    variable: Extract<FieldLayout, { kind: "variable" }>[];
    fixedSize: number;
  },
  raw: Uint8Array,
  logicalIndex: number,
): Uint8Array | null {
  const target = layout.fields[logicalIndex];
  if (!target) throw new Error("field is not present");
  const valueType = descriptor[logicalIndex].valueType;
  if (target.kind === "fixed") {
    const end = target.offset + target.size;
    if (end > raw.length) throw new Error("unexpected end of record");
    const value = raw.subarray(target.offset, target.offset + target.size);
    return unwrapValue(value, valueType);
  }
  const offsetTableStart = layout.fixedSize;
  const variableStart = layout.fixedSize + Math.max(0, layout.variable.length - 1) * 4;
  const start =
    target.variableIndex === 0
      ? variableStart
      : readU32Le(raw, offsetTableStart + (target.variableIndex - 1) * 4);
  const end =
    target.variableIndex === layout.variable.length - 1
      ? raw.length
      : readU32Le(raw, offsetTableStart + target.variableIndex * 4);
  if (start > end || end > raw.length) {
    const field = descriptor[logicalIndex];
    const claimedWidth = end - start;
    const remaining = Math.max(0, raw.length - start);
    throw new Error(
      `invalid offset for ${field?.name ?? `<field ${logicalIndex}>`}: declared type ${formatValueType(valueType)}, tag ${valueType.tag}, claimed width ${claimedWidth}, remaining buffer ${remaining}, current offset ${start}, end offset ${end}, record length ${raw.length}`,
    );
  }
  const value = raw.subarray(start, end);
  return unwrapValue(value, valueType);
}

function unwrapValue(value: Uint8Array, valueType: ValueType): Uint8Array | null {
  if (valueType.tag !== 15) return value;
  const unwrapped = unwrapNullable(value);
  if (unwrapped == null) return null;
  return valueType.inner ? unwrapValue(unwrapped, valueType.inner) : unwrapped;
}

function formatValueType(valueType: ValueType): string {
  if (valueType.tag === 14 || valueType.tag === 15) {
    return `${valueType.tag === 14 ? "Array" : "Nullable"}<${valueType.inner ? formatValueType(valueType.inner) : "?"}>`;
  }
  return valueTypeName(valueType.tag);
}

function valueTypeName(tag: number): string {
  switch (tag) {
    case 0:
      return "U8";
    case 1:
      return "U16";
    case 2:
      return "U32";
    case 3:
      return "U64";
    case 4:
      return "I32";
    case 5:
      return "I64";
    case 6:
      return "F64";
    case 7:
      return "Bool";
    case 8:
      return "String";
    case 9:
      return "Bytes";
    case 10:
      return "Internal";
    case 11:
      return "Uuid";
    case 12:
      return "EnumTag";
    case 13:
      return "Tuple";
    case 14:
      return "Array";
    case 15:
      return "Nullable";
    case 16:
      return "Record";
    case 17:
      return "Enum";
    default:
      return `unknown(${tag})`;
  }
}

function unwrapNullable(value: Uint8Array): Uint8Array | null {
  if (value[0] === 0) return null;
  if (value[0] !== 1) return value;
  return value.subarray(1);
}

function descriptorFromColumns(columns: readonly ColumnDescriptor[]): DescriptorField[] {
  return columns.map((column) => ({
    name: column.name,
    valueType: storageColumnValueType(column),
  }));
}

/**
 * Encode a physical storage value for a declared column.
 *
 * This is the single authority for the value bytes shared by packed rows and
 * mutation-cell records. Callers retain responsibility for their distinct
 * omission/default policies, but a present value must always use this path so
 * nullable and sparse carrier layers remain part of the descriptor contract.
 */
export function encodeNativeColumnValue(
  column: ColumnDescriptor,
  value: Value | undefined,
): Uint8Array {
  const logicalType = storageColumnTypeToValueType(column.column_type);
  const nullableType: ValueType = column.nullable ? { tag: 15, inner: logicalType } : logicalType;

  if (!value) {
    if (column.sparse) return encodeNativeNullValue({ tag: 15, inner: nullableType });
    if (!column.nullable) {
      throw new Error(`missing non-nullable value for ${column.name}`);
    }
    return encodeNativeNullValue(nullableType);
  }
  if (value.type === "Null") {
    if (!column.nullable) {
      throw new Error(`missing non-nullable value for ${column.name}`);
    }
    const encodedNull = encodeNativeNullValue(nullableType);
    return column.sparse ? encodeNativePresentValue(encodedNull, nullableType) : encodedNull;
  }
  let encoded = encodeNativeNonNullValue(column.column_type, value);
  if (column.nullable) encoded = encodeNativePresentValue(encoded, logicalType);
  return column.sparse ? encodeNativePresentValue(encoded, nullableType) : encoded;
}

function encodeNativePresentValue(encoded: Uint8Array, inner: ValueType): Uint8Array {
  if (nativeFixedValueSize(inner) == null) {
    return concatBytes([Uint8Array.of(1), encoded]);
  }
  const output = new Uint8Array(1 + encoded.length);
  output[0] = 1;
  output.set(encoded, 1);
  return output;
}

export function encodeNativeNullValue(valueType: ValueType): Uint8Array {
  const width = nativeFixedValueSize(valueType);
  return width == null ? Uint8Array.of(0) : new Uint8Array(width);
}

function encodeNativeNonNullValue(type: ColumnType, value: Value): Uint8Array {
  switch (type.type) {
    case "Boolean":
      if (value.type !== "Boolean") throw new Error("expected Boolean value");
      return Uint8Array.of(value.value ? 1 : 0);
    case "Integer": {
      const integer = expectSignedI32(value);
      const bytes = new Uint8Array(4);
      new DataView(bytes.buffer).setInt32(0, integer, true);
      return bytes;
    }
    case "Timestamp": {
      if (value.type !== "Timestamp" || !Number.isSafeInteger(value.value)) {
        throw new Error(`expected ${type.type} value`);
      }
      const bytes = new Uint8Array(8);
      new DataView(bytes.buffer).setBigUint64(0, BigInt(value.value), true);
      return bytes;
    }
    case "BigInt": {
      const integer = expectSignedI64(value);
      const bytes = new Uint8Array(8);
      new DataView(bytes.buffer).setBigInt64(0, integer, true);
      return bytes;
    }
    case "Double": {
      if (value.type !== "Double") throw new Error("expected Double value");
      const bytes = new Uint8Array(8);
      new DataView(bytes.buffer).setFloat64(0, value.value, true);
      return bytes;
    }
    case "Text":
    case "Json":
    case "Enum":
      if (value.type !== "Text") throw new Error(`expected ${type.type} value`);
      return concatBytes([Uint8Array.of(2), new TextEncoder().encode(value.value)]);
    case "EnumPayload": {
      if (value.type !== "Enum") throw new Error("expected Enum payload value");
      const entry = type.cases.find((candidate) => candidate.name === value.value.case);
      if (!entry || entry.fields.length !== value.value.values.length) {
        throw new Error("invalid Enum payload case or width");
      }
      const name = new TextEncoder().encode(entry.name);
      const payload = encodeNativeRowValue(entry.fields, { values: value.value.values });
      return concatBytes([encodeU32Le(name.length), name, payload]);
    }
    case "Uuid":
      if (value.type !== "Uuid") throw new Error("expected Uuid value");
      return parseUuid(value.value);
    case "Bytea":
      if (value.type !== "Bytea") throw new Error("expected Bytea value");
      return concatBytes([Uint8Array.of(2), value.value]);
    case "Array":
      if (value.type !== "Array") throw new Error("expected Array value");
      return encodeNativeArrayValue(type.element, value.value);
    case "Row":
      if (value.type !== "Row") throw new Error("expected Row value");
      return encodeNativeRowValue(type.columns, value.value);
  }
}

function encodeNativeRowValue(
  columns: readonly ColumnDescriptor[],
  value: { id?: string; values: Value[]; valuesByColumn?: Map<string, Value> },
): Uint8Array {
  const values = value.valuesByColumn
    ? columns.map(
        (column) =>
          value.valuesByColumn?.get(column.name) ??
          (column.column_type.type === "Array"
            ? ({ type: "Array", value: [] } satisfies Value)
            : ({ type: "Null" } satisfies Value)),
      )
    : value.values;
  const encodedValues = encodeNativeRowValues(columns, values);
  const idBytes = value.id ? parseUuid(value.id) : new Uint8Array();
  return concatBytes([
    Uint8Array.of(value.id ? 1 : 0),
    idBytes,
    encodeU32Le(encodedValues.byteLength),
    encodedValues,
  ]);
}

function encodeNativeArrayValue(elementType: ColumnType, values: readonly Value[]): Uint8Array {
  const encoded = values.map((value) => encodeNativeNonNullValue(elementType, value));
  const elementWidth = nativeFixedValueSize(storageColumnTypeToValueType(elementType));
  if (elementWidth != null) return concatBytes(encoded);

  const offsets = new Uint8Array(Math.max(0, values.length - 1) * 4);
  const view = new DataView(offsets.buffer);
  let nextOffset = 4 + offsets.byteLength;
  encoded.slice(0, -1).forEach((chunk, index) => {
    nextOffset += chunk.length;
    view.setUint32(index * 4, nextOffset, true);
  });
  return concatBytes([encodeU32Le(values.length), offsets, ...encoded]);
}

export function encodeU32Le(value: number): Uint8Array {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, value, true);
  return bytes;
}

function parseUuid(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) {
    throw new Error(`invalid UUID value ${value}`);
  }
  return Uint8Array.from(hex.match(/../g)!.map((byte) => Number.parseInt(byte, 16)));
}

export function storageColumnValueType(column: ColumnDescriptor): ValueType {
  let valueType = storageColumnTypeToValueType(column.column_type, column.name);
  if (column.nullable) valueType = { tag: 15, inner: valueType };
  return column.sparse ? { tag: 15, inner: valueType } : valueType;
}

/** Strip physical sparse-carrier metadata for public packed row transport. */
export function logicalStorageColumns(
  columns: readonly ColumnDescriptor[],
): readonly ColumnDescriptor[] {
  return columns.map((column) => ({
    ...column,
    sparse: undefined,
    column_type:
      column.column_type.type === "Row"
        ? {
            ...column.column_type,
            columns: [...logicalStorageColumns(column.column_type.columns)],
          }
        : column.column_type.type === "Array" && column.column_type.element.type === "Row"
          ? {
              ...column.column_type,
              element: {
                ...column.column_type.element,
                columns: [...logicalStorageColumns(column.column_type.element.columns)],
              },
            }
          : column.column_type,
  }));
}

export function storageColumnTypeToValueType(type: ColumnType, enumName = "enum"): ValueType {
  switch (type.type) {
    case "Boolean":
      return { tag: 7 };
    case "Integer":
      return { tag: 4 };
    case "BigInt":
      return { tag: 5 };
    case "Timestamp":
      return { tag: 3 };
    case "Double":
      return { tag: 6 };
    case "Text":
    case "Json":
    case "Enum":
      return { tag: 8 };
    case "EnumPayload":
      return {
        tag: 17,
        enumSchema: {
          name: enumName,
          cases: type.cases.map((entry) => ({
            name: entry.name,
            payload: entry.fields.map((field) => ({
              name: field.name,
              valueType: storageColumnValueType(field),
            })),
          })),
        },
      };
    case "Bytea":
      return { tag: 9 };
    case "Uuid":
      return { tag: 11 };
    case "Array":
      return { tag: 14, inner: storageColumnTypeToValueType(type.element, enumName) };
    case "Row":
      return { tag: 9 };
  }
}

function decodeBytes(type: ColumnType, bytes: Uint8Array): Value {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  switch (type.type) {
    case "Boolean":
      return { type: "Boolean", value: bytes[0] !== 0 };
    case "Integer":
      return { type: "Integer", value: view.getInt32(0, true) };
    case "BigInt":
      return { type: "BigInt", value: view.getBigInt64(0, true) };
    case "Double":
      return { type: "Double", value: view.getFloat64(0, true) };
    case "Timestamp":
      return { type: "Timestamp", value: Number(view.getBigUint64(0, true)) };
    case "Text":
    case "Json":
    case "Enum":
      return { type: "Text", value: textDecoder.decode(decodeInlineScalar(bytes)) };
    case "EnumPayload": {
      if (bytes.length < 4) throw new Error("invalid Enum payload value");
      const nameLength = view.getUint32(0, true);
      if (bytes.length < 4 + nameLength) throw new Error("invalid Enum payload case");
      const caseName = textDecoder.decode(bytes.subarray(4, 4 + nameLength));
      const entry = type.cases.find((candidate) => candidate.name === caseName);
      if (!entry) throw new Error("unknown Enum payload case");
      return {
        type: "Enum",
        value: {
          case: caseName,
          values: decodeRowValue(entry.fields, bytes.subarray(4 + nameLength)).values,
        },
      };
    }
    case "Uuid":
      return { type: "Uuid", value: formatUuid(bytes) };
    case "Bytea":
      return { type: "Bytea", value: decodeInlineScalar(bytes).slice() };
    case "Array":
      return { type: "Array", value: decodeArray(type.element, bytes) };
    case "Row":
      return { type: "Row", value: decodeRowValue(type.columns, bytes) };
  }
}

function decodeInlineScalar(bytes: Uint8Array): Uint8Array {
  if (bytes[0] !== 2) {
    throw new Error("indirect scalar crossed a logical binding boundary");
  }
  return bytes.subarray(1);
}

function decodeRowValue(
  columns: readonly ColumnDescriptor[],
  bytes: Uint8Array,
): { id?: string; values: Value[]; valuesByColumn?: Map<string, Value> } {
  if (bytes.byteLength < 5) throw new Error("invalid nested row value");
  const hasId = bytes[0] === 1;
  let offset = 1;
  let id: string | undefined;
  if (hasId) {
    id = formatUuid(bytes.subarray(offset, offset + 16));
    offset += 16;
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const len = view.getUint32(offset, true);
  offset += 4;
  const raw = bytes.subarray(offset, offset + len);
  if (raw.byteLength !== len) throw new Error("invalid nested row value length");
  const row: { id?: string; values: Value[]; valuesByColumn?: Map<string, Value> } = {
    id,
    values: decodeNativeRowValues(columns, raw),
  };
  Object.defineProperty(row, "valuesByColumn", {
    value: decodeNativeRowValuesByColumn(columns, raw),
    enumerable: false,
    configurable: true,
  });
  return row;
}

function decodePlainValue(type: ColumnType, bytes: Uint8Array, columnName?: string): unknown {
  const value = decodeBytes(type, bytes);
  switch (type.type) {
    case "Timestamp":
      return timestampToDate(decodeNativeTimestamp(bytes, columnName), columnName);
    case "Json":
      return value.type === "Text" ? JSON.parse(value.value) : null;
    case "Array":
      return decodePlainArray(type.element, bytes);
    case "Text":
    case "Enum":
    case "EnumPayload":
    case "Bytea":
    case "Uuid":
    case "Boolean":
    case "Integer":
    case "BigInt":
    case "Double":
      return "value" in value ? value.value : null;
    case "Row":
      return "value" in value ? value.value : null;
  }
}

function decodePlainArray(elementType: ColumnType, bytes: Uint8Array): unknown[] {
  return decodeArrayElements(elementType, bytes, (element) =>
    decodePlainValue(elementType, element),
  );
}

function decodeArray(elementType: ColumnType, bytes: Uint8Array): Value[] {
  return decodeArrayElements(elementType, bytes, (element) => decodeBytes(elementType, element));
}

function decodeArrayElements<T>(
  elementType: ColumnType,
  bytes: Uint8Array,
  decodeElement: (bytes: Uint8Array) => T,
): T[] {
  const elementWidth = nativeFixedValueSize(storageColumnTypeToValueType(elementType));
  if (elementWidth != null) {
    if (elementWidth === 0) return [];
    if (bytes.length % elementWidth !== 0) {
      throw new Error(`invalid fixed-width array byte length ${bytes.length}`);
    }
    const values: T[] = [];
    for (let offset = 0; offset < bytes.length; offset += elementWidth) {
      values.push(decodeElement(bytes.subarray(offset, offset + elementWidth)));
    }
    return values;
  }

  if (bytes.length < 4) {
    throw new Error("invalid variable-width array byte length");
  }

  const length = readU32Le(bytes, 0);
  const offsetTableEnd = 4 + Math.max(0, length - 1) * 4;
  if (offsetTableEnd > bytes.length) {
    throw new Error("invalid variable-width array offset table");
  }

  const values: T[] = [];
  for (let index = 0; index < length; index += 1) {
    const start = index === 0 ? offsetTableEnd : readU32Le(bytes, 4 + (index - 1) * 4);
    const end = index === length - 1 ? bytes.length : readU32Le(bytes, 4 + index * 4);
    if (start > end || end > bytes.length) {
      throw new Error("invalid variable-width array element offset");
    }
    values.push(decodeElement(bytes.subarray(start, end)));
  }
  return values;
}

export function decodeNativeTimestamp(bytes: Uint8Array, _columnName?: string): number {
  const raw = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigUint64(0, true);
  // All public timestamps use Unix milliseconds. Packed HLCs stay in internal
  // version and transaction-ordering state and never cross this boundary.
  return Number(raw);
}

function timestampToDate(value: number, _columnName?: string): Date {
  return new Date(value);
}

function formatUuid(bytes: Uint8Array): string {
  const hex = Array.from(bytes.subarray(0, 16), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(
    16,
    20,
  )}-${hex.slice(20)}`;
}

export function nativeFixedValueSize(valueType: ValueType): number | undefined {
  switch (valueType.tag) {
    case 0:
    case 7:
    case 12:
      return 1;
    case 1:
      return 2;
    case 2:
    case 4:
      return 4;
    case 3:
    case 5:
    case 6:
      return 8;
    case 11:
      return 16;
    case 13: {
      const members = valueType.members ?? (valueType.inner ? [valueType.inner] : []);
      return members.reduce<number | undefined>((total, member) => {
        if (total == null) return undefined;
        const memberSize = nativeFixedValueSize(member);
        return memberSize == null ? undefined : total + memberSize;
      }, 0);
    }
    case 10:
    case 14:
      return undefined;
    case 15: {
      const innerSize = valueType.inner ? nativeFixedValueSize(valueType.inner) : undefined;
      return innerSize == null ? undefined : innerSize + 1;
    }
    default:
      return undefined;
  }
}

type FieldLayout =
  | {
      kind: "fixed";
      logicalIndex: number;
      offset: number;
      size: number;
    }
  | {
      kind: "variable";
      logicalIndex: number;
      variableIndex: number;
    };

function recordLayout(descriptor: DescriptorField[]): {
  fields: FieldLayout[];
  fixed: Extract<FieldLayout, { kind: "fixed" }>[];
  variable: Extract<FieldLayout, { kind: "variable" }>[];
  fixedSize: number;
} {
  const fields: FieldLayout[] = [];
  fields.length = descriptor.length;
  const fixed: Extract<FieldLayout, { kind: "fixed" }>[] = [];
  const variable: Extract<FieldLayout, { kind: "variable" }>[] = [];
  let fixedOffset = 0;

  for (let logicalIndex = 0; logicalIndex < descriptor.length; logicalIndex += 1) {
    const size = nativeFixedValueSize(descriptor[logicalIndex].valueType);
    if (size == null) continue;
    const layout = { kind: "fixed" as const, logicalIndex, offset: fixedOffset, size };
    fields[logicalIndex] = layout;
    fixed.push(layout);
    fixedOffset += size;
  }

  for (let logicalIndex = 0; logicalIndex < descriptor.length; logicalIndex += 1) {
    if (nativeFixedValueSize(descriptor[logicalIndex].valueType) != null) continue;
    const layout = {
      kind: "variable" as const,
      logicalIndex,
      variableIndex: variable.length,
    };
    fields[logicalIndex] = layout;
    variable.push(layout);
  }

  return { fields, fixed, variable, fixedSize: fixedOffset };
}

function readU32Le(bytes: Uint8Array, offset: number): number {
  return (
    bytes[offset] | (bytes[offset + 1] << 8) | (bytes[offset + 2] << 16) | (bytes[offset + 3] << 24)
  );
}

function expectSignedI32(value: Value): number {
  if (
    value.type !== "Integer" ||
    !Number.isSafeInteger(value.value) ||
    value.value < -0x80000000 ||
    value.value > 0x7fffffff
  ) {
    throw new Error("Integer value must be a signed 32-bit integer");
  }
  return value.value;
}

function expectSignedI64(value: Value): bigint {
  if (value.type !== "BigInt") throw new Error("expected BigInt value");
  return exactSignedI64(value.value, "BigInt value");
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const length = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const out = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

class OffsetWriter {
  readonly #bytes: number[] = [];

  u32Le(value: number): void {
    if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
      throw new Error(`offset must be an unsigned 32-bit integer, got ${value}`);
    }
    this.#bytes.push(
      value & 0xff,
      (value >>> 8) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 24) & 0xff,
    );
  }

  finish(): Uint8Array {
    return new Uint8Array(this.#bytes);
  }
}
