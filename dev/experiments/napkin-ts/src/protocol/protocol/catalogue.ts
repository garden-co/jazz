import type { Reader, Writer } from "../../support/postcard.js";
import { SortedMap, cmpStr } from "../../support/collections.js";
import { concatBytes, equalBytes, fromUtf8, utf8 } from "../../support/bytes.js";
import { uuidFromString, uuidV4, uuidV5 } from "../../support/hash.js";
import {
  NIL_UUID,
  readUuid,
  uuidBytes,
  uuidFromBytes,
  writeUuid,
  type GlobalPhysicalColumnId,
  type GlobalPhysicalTableId,
  type MigrationLensId,
  type SchemaLineagePublicationId,
  type SchemaVersionId,
} from "../../types/ids.js";
import {
  Value,
  decodePublicSchemaJson,
  encodeRecordDescriptor,
  encodeStoredScalar,
  physicalColumnEpochIsCompatible,
  publicSchemaJsonBytes,
  schemaVersionIdOf,
  ValueType,
  type JazzSchema,
} from "../../_extern/types-protocol.js";

export const MIGRATION_LENS_NAMESPACE = uuidFromString("5d13f9cb-8a10-5e0f-9a58-e56630a1dc22");
export const SCHEMA_LINEAGE_PUBLICATION_NAMESPACE = uuidFromString(
  "a1b3ff15-9358-52e0-baa8-f384b1d5db1c",
);

// ---- canonical little-endian codec shared by lenses, claims and read views ----

export class ByteSink {
  private parts: Uint8Array[] = [];
  raw(b: Uint8Array) {
    this.parts.push(b);
  }
  u8(v: number) {
    this.parts.push(Uint8Array.of(v & 0xff));
  }
  private fixed(n: number, f: (d: DataView) => void) {
    const b = new Uint8Array(n);
    f(new DataView(b.buffer));
    this.parts.push(b);
  }
  u16(v: number) {
    this.fixed(2, (d) => d.setUint16(0, v, true));
  }
  u32(v: number) {
    this.fixed(4, (d) => d.setUint32(0, v, true));
  }
  i32(v: number) {
    this.fixed(4, (d) => d.setInt32(0, v, true));
  }
  u64(v: bigint | number) {
    this.fixed(8, (d) => d.setBigUint64(0, BigInt(v), true));
  }
  i64(v: bigint | number) {
    this.fixed(8, (d) => d.setBigInt64(0, BigInt(v), true));
  }
  f64(v: number) {
    this.fixed(8, (d) => d.setFloat64(0, v, true));
  }
  len(n: number) {
    if (n > 0xffffffff) throw new Error("canonical lens component exceeds u32");
    this.u32(n);
  }
  bytes(b: Uint8Array) {
    this.len(b.length);
    this.raw(b);
  }
  finish(): Uint8Array {
    return concatBytes(...this.parts);
  }
}
export const putStr = (b: ByteSink, s: string) => b.bytes(utf8(s));

export function putValue(b: ByteSink, value: any): void {
  const v = value.value;
  switch (value.kind) {
    case "U8":
      b.u8(0);
      b.u8(v);
      break;
    case "U16":
      b.u8(1);
      b.u16(v);
      break;
    case "U32":
      b.u8(2);
      b.u32(v);
      break;
    case "U64":
      b.u8(3);
      b.u64(v);
      break;
    case "I32":
      b.u8(14);
      b.i32(v);
      break;
    case "I64":
      b.u8(13);
      b.i64(v);
      break;
    case "F64":
      b.u8(4);
      b.f64(v);
      break;
    case "Bool":
      b.u8(5);
      b.u8(v ? 1 : 0);
      break;
    case "String":
      b.u8(6);
      putStr(b, v);
      break;
    case "Bytes":
      b.u8(7);
      b.bytes(v);
      break;
    case "Uuid":
      b.u8(8);
      b.raw(uuidBytes(v));
      break;
    case "EnumTag":
      b.u8(9);
      b.u8(v);
      break;
    case "Tuple":
    case "Array":
      b.u8(value.kind === "Tuple" ? 10 : 11);
      b.len(v.length);
      for (const item of v) putValue(b, item);
      break;
    case "Nullable":
      b.u8(12);
      if (v === undefined) b.u8(0);
      else {
        b.u8(1);
        putValue(b, v);
      }
      break;
    case "Record":
      b.u8(16);
      b.bytes(encodeRecordDescriptor(v.descriptor()));
      b.bytes(v.raw());
      break;
    case "Enum":
      throw new Error(
        "union-valued values are an internal Groove representation, not a Jazz protocol value",
      );
    case "U48":
      throw new Error("U48 is an engine-owned stamp width, not a Jazz protocol value");
    case "Large":
      b.u8(15);
      b.bytes(encodeStoredScalar(v.kind, { kind: "Chunked", value: v }));
      break;
  }
}

/** Bounded little-endian decoder for canonical lens bytes. */
class ProtocolCodecCursor {
  static readonly MAX_COUNT = 16_384;
  static readonly MAX_BYTES = 16 * 1024 * 1024;
  static readonly MAX_TOTAL_BYTES = 32 * 1024 * 1024;
  static readonly MAX_TOTAL_ITEMS = 65_536;
  static readonly MAX_DEPTH = 64;
  private offset = 0;
  private budget = ProtocolCodecCursor.MAX_TOTAL_BYTES;
  private items = 0;
  private readonly view: DataView;
  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }
  take(n: number): Uint8Array {
    if (n > this.budget) throw new Error("protocol aggregate byte budget exceeds limit");
    const e = this.offset + n;
    if (e > this.bytes.length) throw new Error("truncated protocol codec");
    const out = this.bytes.subarray(this.offset, e);
    this.offset = e;
    this.budget -= n;
    return out;
  }
  private at(n: number): number {
    const o = this.offset;
    this.take(n);
    return o;
  }
  u8 = () => this.take(1)[0];
  u16 = () => this.view.getUint16(this.at(2), true);
  u32 = () => this.view.getUint32(this.at(4), true);
  u64 = () => this.view.getBigUint64(this.at(8), true);
  i32 = () => this.view.getInt32(this.at(4), true);
  i64 = () => this.view.getBigInt64(this.at(8), true);
  uuid = () => uuidFromBytes(this.take(16));
  count(): number {
    const count = this.u32();
    this.items += count;
    if (count > ProtocolCodecCursor.MAX_COUNT || this.items > ProtocolCodecCursor.MAX_TOTAL_ITEMS)
      throw new Error("protocol collection count exceeds limit");
    return count;
  }
  bytesField(): Uint8Array {
    const n = this.u32();
    if (n > ProtocolCodecCursor.MAX_BYTES) throw new Error("protocol byte length exceeds limit");
    return this.take(n).slice();
  }
  string(): string {
    try {
      return fromUtf8(this.bytesField());
    } catch (e) {
      if (e instanceof TypeError) throw new Error("invalid protocol utf8");
      throw e;
    }
  }
  value(depth = 0): any {
    if (depth >= ProtocolCodecCursor.MAX_DEPTH)
      throw new Error("protocol value nesting exceeds limit");
    switch (this.u8()) {
      case 0:
        return Value.U8(this.u8());
      case 1:
        return Value.U16(this.u16());
      case 2:
        return Value.U32(this.u32());
      case 3:
        return Value.U64(this.u64());
      case 4:
        return Value.F64(this.view.getFloat64(this.at(8), true));
      case 5: {
        const b = this.u8();
        if (b > 1) throw new Error("invalid protocol bool");
        return Value.Bool(b === 1);
      }
      case 6:
        return Value.String(this.string());
      case 7:
        return Value.Bytes(this.bytesField());
      case 8:
        return Value.Uuid(this.uuid());
      case 9:
        return Value.EnumTag(this.u8());
      case 10:
        return Value.Tuple(this.values(depth + 1));
      case 11:
        return Value.Array(this.values(depth + 1));
      case 12: {
        const t = this.u8();
        if (t === 0) return Value.Nullable(undefined);
        if (t === 1) return Value.Nullable(this.value(depth + 1));
        throw new Error("invalid protocol nullable");
      }
      case 13:
        return Value.I64(this.i64());
      case 14:
        return Value.I32(this.i32());
      case 15:
        throw new Error("large default values are not supported in migration lenses");
      default:
        throw new Error("invalid protocol value tag");
    }
  }
  values(depth: number): any[] {
    const out: any[] = [];
    for (let i = this.count(); i > 0; i--) out.push(this.value(depth));
    return out;
  }
  lensOp(): LensOp {
    switch (this.u8()) {
      case 0:
        return { kind: "RenameTable", from: this.string(), to: this.string() };
      case 1:
        return { kind: "RenameColumn", from: this.string(), to: this.string() };
      case 2:
        return { kind: "CopyColumn", from: this.string(), to: this.string() };
      case 3:
        return { kind: "AddColumn", column: this.string(), default: this.value() };
      case 4:
        return { kind: "DropColumn", column: this.string(), backwardsDefault: this.value() };
      case 5:
        return { kind: "TransformColumn", column: this.string(), transform: this.string() };
      case 6:
        return { kind: "RejectSourceDelta", reason: this.string() };
      default:
        throw new Error("invalid migration lens op");
    }
  }
  finish(): void {
    if (this.offset !== this.bytes.length) throw new Error("trailing protocol codec bytes");
  }
}

// ---- migration lenses ----

export type LensOp =
  | { kind: "RenameTable"; from: string; to: string }
  | { kind: "RenameColumn"; from: string; to: string }
  | { kind: "CopyColumn"; from: string; to: string }
  | { kind: "AddColumn"; column: string; default: any }
  | { kind: "DropColumn"; column: string; backwardsDefault: any }
  | { kind: "TransformColumn"; column: string; transform: string }
  | { kind: "RejectSourceDelta"; reason: string };

export interface TableLens {
  sourceTable: string;
  targetTable: string;
  ops: LensOp[];
}

function putLensOp(b: ByteSink, op: LensOp): void {
  switch (op.kind) {
    case "RenameTable":
    case "RenameColumn":
    case "CopyColumn":
      b.u8(op.kind === "RenameTable" ? 0 : op.kind === "RenameColumn" ? 1 : 2);
      putStr(b, op.from);
      putStr(b, op.to);
      break;
    case "AddColumn":
      b.u8(3);
      putStr(b, op.column);
      putValue(b, op.default);
      break;
    case "DropColumn":
      b.u8(4);
      putStr(b, op.column);
      putValue(b, op.backwardsDefault);
      break;
    case "TransformColumn":
      b.u8(5);
      putStr(b, op.column);
      putStr(b, op.transform);
      break;
    case "RejectSourceDelta":
      b.u8(6);
      putStr(b, op.reason);
      break;
  }
}

const compareTableLens = (a: TableLens, b: TableLens) =>
  cmpStr(a.sourceTable, b.sourceTable) || cmpStr(a.targetTable, b.targetTable);

export class MigrationLens {
  id: MigrationLensId = NIL_UUID;
  constructor(
    readonly source: SchemaVersionId,
    readonly target: SchemaVersionId,
    readonly tableLenses: TableLens[],
  ) {}

  static new(
    source: SchemaVersionId,
    target: SchemaVersionId,
    tableLenses: TableLens[],
  ): MigrationLens {
    tableLenses.sort(compareTableLens);
    validateProtocolLensDefaults(tableLenses);
    const sources = new Set<string>();
    const targets = new Set<string>();
    for (const t of tableLenses) {
      if (sources.has(t.sourceTable) || targets.has(t.targetTable))
        throw new Error("migration lens table coordinates must be bijective");
      sources.add(t.sourceTable);
      targets.add(t.targetTable);
    }
    const lens = new MigrationLens(source, target, tableLenses);
    lens.id = lens.contentId();
    return lens;
  }

  contentId(): MigrationLensId {
    return uuidFromBytes(uuidV5(MIGRATION_LENS_NAMESPACE, canonicalLensBytes(this)));
  }

  static write(w: Writer, lens: MigrationLens) {
    w.bytes(canonicalLensBytes(lens));
  }
  static read(r: Reader): MigrationLens {
    const bytes = r.bytes();
    if (bytes.length > ProtocolCodecCursor.MAX_TOTAL_BYTES)
      throw new Error("migration lens blob exceeds aggregate byte budget");
    return decodeCanonicalLensBytes(bytes);
  }
}

function validateProtocolLensDefaults(tableLenses: TableLens[]): void {
  const valid = (v: any): boolean => {
    switch (v.kind) {
      case "Large":
      case "Record":
      case "Enum":
        return false;
      case "Tuple":
      case "Array":
        return v.value.every(valid);
      case "Nullable":
        return v.value === undefined || valid(v.value);
      default:
        return true;
    }
  };
  for (const t of tableLenses)
    for (const op of t.ops)
      if (
        (op.kind === "AddColumn" && !valid(op.default)) ||
        (op.kind === "DropColumn" && !valid(op.backwardsDefault))
      )
        throw new Error(
          "migration lens defaults cannot contain storage-only or descriptor-less values",
        );
}

export function canonicalLensBytes(lens: MigrationLens): Uint8Array {
  const b = new ByteSink();
  putStr(b, "jazz-migration-lens-v1");
  b.raw(uuidBytes(lens.source));
  b.raw(uuidBytes(lens.target));
  const tableLenses = [...lens.tableLenses].sort(compareTableLens);
  b.len(tableLenses.length);
  for (const t of tableLenses) {
    putStr(b, t.sourceTable);
    putStr(b, t.targetTable);
    b.len(t.ops.length);
    for (const op of t.ops) putLensOp(b, op);
  }
  return b.finish();
}

export function decodeCanonicalLensBytes(bytes: Uint8Array): MigrationLens {
  const c = new ProtocolCodecCursor(bytes);
  if (c.string() !== "jazz-migration-lens-v1") throw new Error("invalid migration lens marker");
  const source = c.uuid();
  const target = c.uuid();
  const tableLenses: TableLens[] = [];
  for (let i = c.count(); i > 0; i--) {
    const sourceTable = c.string();
    const targetTable = c.string();
    const ops: LensOp[] = [];
    for (let j = c.count(); j > 0; j--) ops.push(c.lensOp());
    tableLenses.push({ sourceTable, targetTable, ops });
  }
  c.finish();
  let lens: MigrationLens;
  try {
    lens = MigrationLens.new(source, target, tableLenses);
  } catch {
    throw new Error("invalid migration lens");
  }
  if (!equalBytes(canonicalLensBytes(lens), bytes)) throw new Error("noncanonical migration lens");
  return lens;
}

// ---- schema versions ----

export interface SchemaVersion {
  id: SchemaVersionId;
  schema: JazzSchema;
}
export const schemaVersionNew = (schema: JazzSchema): SchemaVersion => ({
  id: schemaVersionIdOf(schema),
  schema,
});
export const CATALOGUE_SCHEMA_VERSION = 2;

export function canonicalCatalogueSchemaBytes(s: SchemaVersion): Uint8Array {
  const json = publicSchemaJsonBytes(s.schema);
  if (json.length > 0xffffffff) throw new Error("catalogue public schema payload too large");
  const b = new ByteSink();
  b.u8(CATALOGUE_SCHEMA_VERSION);
  b.raw(uuidBytes(s.id));
  b.u32(json.length);
  b.raw(json);
  return b.finish();
}

export const writeSchemaVersion = (w: Writer, s: SchemaVersion) => {
  writeUuid(w, s.id);
  w.bytes(publicSchemaJsonBytes(s.schema));
};
export const readSchemaVersion = (r: Reader): SchemaVersion => ({
  id: readUuid(r),
  schema: decodePublicSchemaJson(r.bytes()),
});

// ---- physical identities ----

export interface PhysicalColumnIdentity {
  id: GlobalPhysicalColumnId;
  enumVariants: SortedMap<string, string[]>;
}
export interface PhysicalTableIdentity {
  id: GlobalPhysicalTableId;
  columns: SortedMap<string, PhysicalColumnIdentity>;
}

const freshUuid = () => uuidFromBytes(uuidV4());
type Slot = [string, string | undefined, [string, number] | undefined];

function cloneTable(t: PhysicalTableIdentity): PhysicalTableIdentity {
  const columns = new SortedMap<string, PhysicalColumnIdentity>(cmpStr);
  for (const [name, c] of t.columns)
    columns.set(name, {
      id: c.id,
      enumVariants: new SortedMap(
        cmpStr,
        [...c.enumVariants].map(([p, v]) => [p, [...v]]),
      ),
    });
  return { id: t.id, columns };
}

export class PhysicalIdentityManifest {
  constructor(readonly tables: SortedMap<string, PhysicalTableIdentity> = new SortedMap(cmpStr)) {}

  canonicalBytes(): Uint8Array {
    const b = new ByteSink();
    b.len(this.tables.size);
    for (const [tableName, table] of this.tables) {
      putStr(b, tableName);
      b.raw(uuidBytes(table.id));
      b.len(table.columns.size);
      for (const [columnName, column] of table.columns) {
        putStr(b, columnName);
        b.raw(uuidBytes(column.id));
        b.len(column.enumVariants.size);
        for (const [path, variants] of column.enumVariants) {
          putStr(b, path);
          b.len(variants.length);
          for (const v of variants) b.raw(uuidBytes(v));
        }
      }
    }
    return b.finish();
  }

  static allocate(schema: JazzSchema): PhysicalIdentityManifest {
    const m = new PhysicalIdentityManifest();
    for (const table of schema.tables) m.tables.set(table.name, allocateTableIdentity(table));
    return m;
  }

  validateForSchema(schema: JazzSchema): void {
    if (this.tables.size !== schema.tables.length)
      throw new Error("physical identity manifest table coverage mismatch");
    const seen = new Set<string>();
    const claim = (id: string) => {
      if (id === NIL_UUID || seen.has(id))
        throw new Error("physical identity manifest UUID collision");
      seen.add(id);
    };
    for (const table of schema.tables) {
      const ti = this.tables.get(table.name);
      if (!ti) throw new Error("physical identity manifest table missing");
      claim(ti.id);
      if (ti.columns.size !== table.columns.length)
        throw new Error("physical identity manifest column coverage mismatch");
      for (const column of table.columns) {
        const ci = ti.columns.get(column.name);
        if (!ci) throw new Error("physical identity manifest column missing");
        claim(ci.id);
        const occurrences = new SortedMap<string, number>(cmpStr);
        collectEnumVariantCounts(column.columnType, "root", occurrences);
        if (ci.enumVariants.size !== occurrences.size)
          throw new Error("physical identity manifest enum coverage mismatch");
        for (const [path, count] of occurrences) {
          const variants = ci.enumVariants.get(path);
          if (!variants) throw new Error("physical identity manifest enum occurrence missing");
          if (variants.length !== count)
            throw new Error("physical identity manifest enum case coverage mismatch");
          for (const v of variants) claim(v);
        }
      }
    }
  }

  evolve(
    source: JazzSchema,
    target: JazzSchema,
    lens: MigrationLens,
    newTables: Set<string>,
  ): PhysicalIdentityManifest {
    this.validateForSchema(source);
    const tables = new SortedMap<string, PhysicalTableIdentity>(cmpStr);
    for (const targetTable of target.tables) {
      if (newTables.has(targetTable.name)) {
        tables.set(targetTable.name, allocateTableIdentity(targetTable));
        continue;
      }
      const tableLens = lens.tableLenses.find((t) => t.targetTable === targetTable.name);
      if (!tableLens) throw new Error("physical identity lineage target table missing");
      const sourceTable = source.tables.find((t: any) => t.name === tableLens.sourceTable);
      if (!sourceTable) throw new Error("physical identity lineage source table missing");
      const sourceNameByTarget = new SortedMap<string, string | undefined>(
        cmpStr,
        sourceTable.columns.map((c: any) => [c.name, c.name]),
      );
      const found = this.tables.get(tableLens.sourceTable);
      if (!found) throw new Error("physical identity lineage source identity missing");
      const identity = cloneTable(found);
      const remap = (from: string, to: string | undefined) => {
        for (const [k, v] of sourceNameByTarget)
          if (v === from) {
            sourceNameByTarget.set(k, to);
            break;
          }
      };
      for (const op of tableLens.ops) {
        switch (op.kind) {
          case "RenameColumn": {
            const column = identity.columns.delete(op.from);
            if (!column) throw new Error("physical identity renamed column missing");
            identity.columns.set(op.to, column);
            remap(op.from, op.to);
            break;
          }
          case "CopyColumn":
          case "AddColumn": {
            const to = op.kind === "CopyColumn" ? op.to : op.column;
            const targetColumn = targetTable.columns.find((c: any) => c.name === to);
            if (!targetColumn) throw new Error("physical identity added column missing");
            identity.columns.set(to, allocateColumnIdentity(targetColumn));
            break;
          }
          case "DropColumn":
            identity.columns.delete(op.column);
            remap(op.column, undefined);
            break;
          default:
            break;
        }
      }
      identity.columns.retain((name) => targetTable.columns.some((c: any) => c.name === name));
      for (const targetColumn of targetTable.columns) {
        let column = identity.columns.getOrInsert(targetColumn.name, () =>
          allocateColumnIdentity(targetColumn),
        );
        let sourceName: string | undefined;
        for (const [s, t] of sourceNameByTarget)
          if (t === targetColumn.name) {
            sourceName = s;
            break;
          }
        const compatible =
          sourceName !== undefined &&
          physicalColumnEpochIsCompatible(sourceTable, sourceName, targetTable, targetColumn.name);
        if (!compatible) {
          column = allocateColumnIdentity(targetColumn);
          identity.columns.set(targetColumn.name, column);
        }
        reconcileEnumVariantIdentities(column, targetColumn.columnType);
      }
      tables.set(targetTable.name, identity);
    }
    const evolved = new PhysicalIdentityManifest(tables);
    evolved.validateForSchema(target);
    return evolved;
  }

  evolveWithHistory(
    source: JazzSchema,
    target: JazzSchema,
    lens: MigrationLens,
    newTables: Set<string>,
    history: Iterable<PhysicalIdentityManifest>,
  ): PhysicalIdentityManifest {
    const evolved = this.evolve(source, target, lens, newTables);
    const inherited = this.inheritedUuids(source, evolved, target, lens);
    const reserved = new Set<string>();
    for (const m of history) for (const u of m.allIdentityUuids()) reserved.add(u);
    evolved.remintFreshCollisions(inherited, reserved);
    evolved.validateForSchema(target);
    return evolved;
  }

  validateEvolutionTo(
    source: JazzSchema,
    targetManifest: PhysicalIdentityManifest,
    target: JazzSchema,
    lens: MigrationLens,
  ) {
    this.validateEvolutionToWithHistory(source, targetManifest, target, lens, []);
  }

  validateEvolutionToWithHistory(
    source: JazzSchema,
    targetManifest: PhysicalIdentityManifest,
    target: JazzSchema,
    lens: MigrationLens,
    history: Iterable<PhysicalIdentityManifest>,
  ): void {
    this.validateForSchema(source);
    targetManifest.validateForSchema(target);
    const inherited = this.inheritedUuids(source, targetManifest, target, lens);
    const reserved = this.allIdentityUuids();
    for (const m of history) for (const u of m.allIdentityUuids()) reserved.add(u);
    for (const u of targetManifest.allIdentityUuids())
      if (reserved.has(u) && !inherited.has(u))
        throw new Error("physical retired identity reused across lineage");
  }

  inheritedUuids(
    source: JazzSchema,
    targetManifest: PhysicalIdentityManifest,
    target: JazzSchema,
    lens: MigrationLens,
  ): Set<string> {
    const inherited = new Set<string>();
    for (const tableLens of lens.tableLenses) {
      const sourceTable = this.tables.get(tableLens.sourceTable);
      if (!sourceTable) throw new Error("physical identity source table missing");
      const targetTable = targetManifest.tables.get(tableLens.targetTable);
      if (!targetTable) throw new Error("physical identity target table missing");
      if (sourceTable.id !== targetTable.id)
        throw new Error("physical table identity changed across lineage");
      inherited.add(sourceTable.id);
      const sourceSchema = source.tables.find((t: any) => t.name === tableLens.sourceTable);
      if (!sourceSchema) throw new Error("physical identity source table schema missing");
      const targetSchema = target.tables.find((t: any) => t.name === tableLens.targetTable);
      if (!targetSchema) throw new Error("physical identity target table schema missing");
      const targetNameBySource = new SortedMap<string, string | undefined>(
        cmpStr,
        sourceTable.columns.keysArray().map((n) => [n, n]),
      );
      const clear = (name: string) => {
        for (const [k, v] of targetNameBySource)
          if (v === name) targetNameBySource.set(k, undefined);
      };
      for (const op of tableLens.ops) {
        switch (op.kind) {
          case "RenameColumn":
            for (const [k, v] of targetNameBySource)
              if (v === op.from) {
                targetNameBySource.set(k, op.to);
                break;
              }
            break;
          case "CopyColumn":
            clear(op.to);
            break;
          case "AddColumn":
          case "DropColumn":
            clear(op.column);
            break;
          default:
            break;
        }
      }
      for (const [sourceName, targetName] of targetNameBySource) {
        if (targetName === undefined) continue;
        const sourceColumn = sourceTable.columns.get(sourceName)!;
        const targetColumn = targetTable.columns.get(targetName);
        if (!targetColumn) throw new Error("physical identity inherited column missing");
        const compatible = physicalColumnEpochIsCompatible(
          sourceSchema,
          sourceName,
          targetSchema,
          targetName,
        );
        if (compatible && sourceColumn.id !== targetColumn.id)
          throw new Error("physical compatible column identity changed across lineage");
        if (!compatible && sourceColumn.id === targetColumn.id)
          throw new Error("physical incompatible column epoch reused identity");
        if (!compatible) continue;
        inherited.add(sourceColumn.id);
        for (const [path, sourceVariants] of sourceColumn.enumVariants) {
          const targetVariants = targetColumn.enumVariants.get(path);
          if (!targetVariants) continue;
          const shared = Math.min(sourceVariants.length, targetVariants.length);
          for (let i = 0; i < shared; i++)
            if (sourceVariants[i] !== targetVariants[i])
              throw new Error("physical enum identity changed across lineage");
          for (let i = 0; i < shared; i++) inherited.add(sourceVariants[i]);
        }
      }
    }
    return inherited;
  }

  identitySlots(): Map<string, [Slot, string]> {
    const slots = new Map<string, [Slot, string]>();
    const put = (slot: Slot, uuid: string) => slots.set(JSON.stringify(slot), [slot, uuid]);
    for (const [tableName, table] of this.tables) {
      put([tableName, undefined, undefined], table.id);
      for (const [columnName, column] of table.columns) {
        put([tableName, columnName, undefined], column.id);
        for (const [path, variants] of column.enumVariants)
          variants.forEach((v, index) => put([tableName, columnName, [path, index]], v));
      }
    }
    return slots;
  }

  replaceIdentitySlots(slots: Map<string, [Slot, string]>): void {
    for (const [[tableName, columnName, variant], uuid] of slots.values()) {
      const table = this.tables.get(tableName);
      if (!table) throw new Error("validated identity table");
      if (columnName === undefined) {
        table.id = uuid;
        continue;
      }
      const column = table.columns.get(columnName);
      if (!column) throw new Error("validated identity column");
      if (variant === undefined) column.id = uuid;
      else {
        const variants = column.enumVariants.get(variant[0]);
        if (!variants) throw new Error("validated enum occurrence");
        variants[variant[1]] = uuid;
      }
    }
  }

  remintFreshCollisions(inherited: Set<string>, reserved: Set<string>): void {
    const mint = () => {
      for (;;) {
        const u = freshUuid();
        if (!reserved.has(u)) {
          reserved.add(u);
          return u;
        }
      }
    };
    const check = (id: string) => {
      if (!inherited.has(id) && reserved.has(id)) return mint();
      reserved.add(id);
      return id;
    };
    for (const table of this.tables.valuesArray()) {
      table.id = check(table.id);
      for (const column of table.columns.valuesArray()) {
        column.id = check(column.id);
        for (const variants of column.enumVariants.valuesArray())
          for (let i = 0; i < variants.length; i++) variants[i] = check(variants[i]);
      }
    }
  }

  allIdentityUuids(): Set<string> {
    const out = new Set<string>();
    for (const table of this.tables.valuesArray()) {
      out.add(table.id);
      for (const column of table.columns.valuesArray()) {
        out.add(column.id);
        for (const variants of column.enumVariants.valuesArray())
          for (const v of variants) out.add(v);
      }
    }
    return out;
  }

  equals(other: PhysicalIdentityManifest): boolean {
    return equalBytes(this.canonicalBytes(), other.canonicalBytes());
  }

  static write(w: Writer, m: PhysicalIdentityManifest) {
    w.seq([...m.tables], (w, [name, t]) => {
      w.str(name);
      writeUuid(w, t.id);
      w.seq([...t.columns], (w, [cname, c]) => {
        w.str(cname);
        writeUuid(w, c.id);
        w.seq([...c.enumVariants], (w, [path, vs]) => {
          w.str(path);
          w.seq(vs, writeUuid);
        });
      });
    });
  }
  static read(r: Reader): PhysicalIdentityManifest {
    const tables = r.seq((r): [string, PhysicalTableIdentity] => {
      const name = r.str();
      const id = readUuid(r);
      const columns = r.seq((r): [string, PhysicalColumnIdentity] => {
        const cname = r.str();
        const cid = readUuid(r);
        const enumVariants = r.seq((r): [string, string[]] => [r.str(), r.seq(readUuid)]);
        return [cname, { id: cid, enumVariants: new SortedMap(cmpStr, enumVariants) }];
      });
      return [name, { id, columns: new SortedMap(cmpStr, columns) }];
    });
    return new PhysicalIdentityManifest(new SortedMap(cmpStr, tables));
  }
}

function allocateTableIdentity(table: any): PhysicalTableIdentity {
  return {
    id: freshUuid(),
    columns: new SortedMap(
      cmpStr,
      table.columns.map((c: any) => [c.name, allocateColumnIdentity(c)]),
    ),
  };
}

function allocateColumnIdentity(column: any): PhysicalColumnIdentity {
  const enumVariants = new SortedMap<string, string[]>(cmpStr);
  collectEnumVariantIds(column.columnType, "root", enumVariants);
  return { id: freshUuid(), enumVariants };
}

function reconcileEnumVariantIdentities(column: PhysicalColumnIdentity, target: any): void {
  const counts = new SortedMap<string, number>(cmpStr);
  collectEnumVariantCounts(target, "root", counts);
  column.enumVariants.retain((path) => counts.has(path));
  for (const [path, count] of counts) {
    const variants = column.enumVariants.getOrInsert(path, () => []);
    variants.length = Math.min(variants.length, count);
    while (variants.length < count) variants.push(freshUuid());
  }
}

/** Walk every enum occurrence in a value type, keyed by its structural path. */
function walkEnums(
  valueType: any,
  path: string,
  visit: (path: string, cases: number) => void,
): void {
  switch (valueType.kind) {
    case "EnumTag":
      visit(path, valueType.schema.variants.length);
      break;
    case "Enum":
      visit(path, valueType.schema.cases.length);
      valueType.schema.cases.forEach((c: any, ordinal: number) =>
        walkEnums(ValueType.Record(c.payload), `${path}/case/${ordinal}`, visit),
      );
      break;
    case "Nullable":
      walkEnums(valueType.inner, `${path}/nullable`, visit);
      break;
    case "Array":
      walkEnums(valueType.inner, `${path}/array`, visit);
      break;
    case "Tuple":
      valueType.values.forEach((v: any, index: number) =>
        walkEnums(v, `${path}/tuple/${index}`, visit),
      );
      break;
    case "Record":
      for (const field of valueType.descriptor.fields())
        if (field.name != null) walkEnums(field.valueType, `${path}/record/${field.name}`, visit);
      break;
  }
}

function collectEnumVariantIds(
  valueType: any,
  path: string,
  out: SortedMap<string, string[]>,
): void {
  walkEnums(valueType, path, (p, n) => out.set(p, Array.from({ length: n }, freshUuid)));
}

function collectEnumVariantCounts(
  valueType: any,
  path: string,
  out: SortedMap<string, number>,
): void {
  walkEnums(valueType, path, (p, n) => out.set(p, n));
}

// ---- schema lineage publications ----

export interface SchemaPredecessor {
  lens: MigrationLens;
  newTables: string[];
  droppedTables: string[];
}

export class SchemaLineagePublication {
  constructor(
    public id: SchemaLineagePublicationId,
    readonly schema: SchemaVersion,
    readonly predecessors: SchemaPredecessor[],
    readonly physicalIdentities: PhysicalIdentityManifest,
  ) {}

  static authorFromPredecessors(
    schema: SchemaVersion,
    predecessors: SchemaPredecessor[],
    sources: Map<SchemaVersionId, [JazzSchema, PhysicalIdentityManifest]>,
    historyIn: Iterable<PhysicalIdentityManifest>,
  ): SchemaLineagePublication {
    if (!predecessors.length) throw new Error("schema requires a predecessor");
    for (const p of predecessors) {
      p.newTables.sort();
      p.droppedTables.sort();
    }
    predecessors.sort((a, b) => cmpStr(a.lens.source, b.lens.source));
    if (predecessors.some((p, i) => i > 0 && predecessors[i - 1].lens.source === p.lens.source))
      throw new Error("duplicate schema predecessor");
    const history = [...historyIn];
    const identities = PhysicalIdentityManifest.allocate(schema.schema);
    const inheritedSlots = new Map<string, [Slot, string]>();
    for (const p of predecessors) {
      if (p.lens.target !== schema.id || p.lens.source === schema.id)
        throw new Error("invalid schema predecessor endpoint");
      const src = sources.get(p.lens.source);
      if (!src) throw new Error("schema predecessor is missing");
      const [source, manifest] = src;
      const candidate = manifest.evolve(source, schema.schema, p.lens, new Set(p.newTables));
      const inherited = manifest.inheritedUuids(source, candidate, schema.schema, p.lens);
      for (const [key, [slot, uuid]] of candidate.identitySlots()) {
        if (!inherited.has(uuid)) continue;
        const previous = inheritedSlots.get(key);
        inheritedSlots.set(key, [slot, uuid]);
        if (previous && previous[1] !== uuid)
          throw new Error("schema predecessors disagree on physical identity");
      }
    }
    identities.replaceIdentitySlots(inheritedSlots);
    const inherited = new Set([...inheritedSlots.values()].map(([, u]) => u));
    const reserved = new Set<string>();
    for (const m of [...history, ...[...sources.values()].map(([, m]) => m)])
      for (const u of m.allIdentityUuids()) reserved.add(u);
    identities.remintFreshCollisions(inherited, reserved);
    const publication = new SchemaLineagePublication(NIL_UUID, schema, predecessors, identities);
    publication.validatePredecessorIdentities(sources, history);
    publication.id = publication.contentId();
    return publication;
  }

  validatePredecessorIdentities(
    sources: Map<SchemaVersionId, [JazzSchema, PhysicalIdentityManifest]>,
    history: Iterable<PhysicalIdentityManifest>,
  ): void {
    this.physicalIdentities.validateForSchema(this.schema.schema);
    const inherited = new Set<string>();
    const reserved = new Set<string>();
    for (const p of this.predecessors) {
      const src = sources.get(p.lens.source);
      if (!src) throw new Error("schema predecessor is missing");
      const [source, manifest] = src;
      manifest.validateForSchema(source);
      for (const u of manifest.inheritedUuids(
        source,
        this.physicalIdentities,
        this.schema.schema,
        p.lens,
      ))
        inherited.add(u);
      for (const u of manifest.allIdentityUuids()) reserved.add(u);
    }
    for (const m of history) for (const u of m.allIdentityUuids()) reserved.add(u);
    for (const u of this.physicalIdentities.allIdentityUuids())
      if (reserved.has(u) && !inherited.has(u))
        throw new Error("physical retired identity reused across lineage");
  }

  static authorFromPrior(
    sourceSchema: JazzSchema,
    sourceIdentities: PhysicalIdentityManifest,
    schema: SchemaVersion,
    lens: MigrationLens,
    newTables: string[],
    droppedTables: string[],
  ): SchemaLineagePublication {
    return SchemaLineagePublication.authorFromPriorWithHistory(
      sourceSchema,
      sourceIdentities,
      [sourceIdentities],
      schema,
      lens,
      newTables,
      droppedTables,
    );
  }

  static authorFromPriorWithHistory(
    sourceSchema: JazzSchema,
    sourceIdentities: PhysicalIdentityManifest,
    history: Iterable<PhysicalIdentityManifest>,
    schema: SchemaVersion,
    lens: MigrationLens,
    newTables: string[],
    droppedTables: string[],
  ): SchemaLineagePublication {
    const identities = sourceIdentities.evolveWithHistory(
      sourceSchema,
      schema.schema,
      lens,
      new Set(newTables),
      history,
    );
    const publication = new SchemaLineagePublication(
      NIL_UUID,
      schema,
      [{ lens, newTables: [...newTables].sort(), droppedTables: [...droppedTables].sort() }],
      identities,
    );
    publication.id = publication.contentId();
    return publication;
  }

  static newGenesisFixture(
    schema: SchemaVersion,
    lens: MigrationLens,
    newTables: string[],
    droppedTables: string[],
  ) {
    const publication = new SchemaLineagePublication(
      NIL_UUID,
      schema,
      [{ lens, newTables: [...newTables].sort(), droppedTables: [...droppedTables].sort() }],
      PhysicalIdentityManifest.allocate(schema.schema),
    );
    publication.id = publication.contentId();
    return publication;
  }

  contentId(): SchemaLineagePublicationId {
    const b = new ByteSink();
    putStr(b, "jazz-schema-lineage-publication-v2");
    b.bytes(canonicalCatalogueSchemaBytes(this.schema));
    const predecessors = [...this.predecessors].sort((x, y) =>
      cmpStr(x.lens.source, y.lens.source),
    );
    b.len(predecessors.length);
    for (const p of predecessors) {
      b.bytes(canonicalLensBytes(p.lens));
      for (const tables of [[...p.newTables].sort(), [...p.droppedTables].sort()]) {
        b.len(tables.length);
        for (const t of tables) putStr(b, t);
      }
    }
    b.bytes(this.physicalIdentities.canonicalBytes());
    return uuidFromBytes(uuidV5(SCHEMA_LINEAGE_PUBLICATION_NAMESPACE, b.finish()));
  }

  static write(w: Writer, p: SchemaLineagePublication) {
    writeUuid(w, p.id);
    writeSchemaVersion(w, p.schema);
    w.seq(p.predecessors, (w, s) => {
      MigrationLens.write(w, s.lens);
      w.seq(s.newTables, (w, t) => w.str(t));
      w.seq(s.droppedTables, (w, t) => w.str(t));
    });
    PhysicalIdentityManifest.write(w, p.physicalIdentities);
  }
  static read(r: Reader): SchemaLineagePublication {
    return new SchemaLineagePublication(
      readUuid(r),
      readSchemaVersion(r),
      r.seq((r) => ({
        lens: MigrationLens.read(r),
        newTables: r.seq((r) => r.str()),
        droppedTables: r.seq((r) => r.str()),
      })),
      PhysicalIdentityManifest.read(r),
    );
  }
}

// ---- catalogue lane ----

export interface CurrentWriteSchema {
  revision: number;
  schema: SchemaVersionId;
}
export interface CatalogueAck {
  revision?: number;
  schema?: SchemaVersionId;
  lens?: MigrationLensId;
  applied: boolean;
}
export interface CatalogueSnapshot {
  genesisPhysicalIdentities: PhysicalIdentityManifest;
  schemas: SchemaVersion[];
  lineages: [number, SchemaLineagePublication][];
  currentWriteSchema: CurrentWriteSchema;
}

export const writeCatalogueAck = (w: Writer, a: CatalogueAck) => {
  w.option(a.revision, (w, n) => w.varint(n));
  w.option(a.schema, writeUuid);
  w.option(a.lens, writeUuid);
  w.bool(a.applied);
};
export const readCatalogueAck = (r: Reader): CatalogueAck => ({
  revision: r.option((r) => r.varint()),
  schema: r.option(readUuid),
  lens: r.option(readUuid),
  applied: r.bool(),
});
export const writeCatalogueSnapshot = (w: Writer, s: CatalogueSnapshot) => {
  PhysicalIdentityManifest.write(w, s.genesisPhysicalIdentities);
  w.seq(s.schemas, writeSchemaVersion);
  w.seq(s.lineages, (w, [seq, p]) => {
    w.varint(seq);
    SchemaLineagePublication.write(w, p);
  });
  w.varint(s.currentWriteSchema.revision);
  writeUuid(w, s.currentWriteSchema.schema);
};
export const readCatalogueSnapshot = (r: Reader): CatalogueSnapshot => ({
  genesisPhysicalIdentities: PhysicalIdentityManifest.read(r),
  schemas: r.seq(readSchemaVersion),
  lineages: r.seq(
    (r) => [r.varint(), SchemaLineagePublication.read(r)] as [number, SchemaLineagePublication],
  ),
  currentWriteSchema: { revision: r.varint(), schema: readUuid(r) },
});
