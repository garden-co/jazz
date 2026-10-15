// Durable physical mapping types and recursive enum-registry lowering.
import { type Bytes, compareBytes, equalBytes, toHex } from "../../../support/bytes.js";
import { SortedMap, SortedSet } from "../../../support/collections.js";
import {
  nodeError,
  VT,
  RecordDescriptor,
  EnumCase,
  EnumSchema,
  ScalarEnumSchema,
  variantRegistryIdForPath,
  globalCasePath,
  descriptorEquals,
  type ValueType,
  type Value,
  type Uuid,
  type EnumOccurrenceRemaps,
} from "../../../_extern/node-physical-ingest.js";

const invalid = (m: string) => nodeError("InvalidStoredValue", m);

/** Lower Jazz's durable schema alias into Groove's table-local union-case space. */
export function grooveVariantTag(alias: number): number {
  if (alias > 0xffffffff) throw invalid("physical table variant tag exhausted");
  return alias;
}

export type SchemaPhysicalMapping = {
  /** Globally meaningful identities authored by the catalogue authority. */
  identities: any;
  tables: SortedMap<string, TablePhysicalMapping>;
};

export type TablePhysicalMapping = {
  tableId: number;
  columns: SortedMap<string, number>;
  variantCases: PhysicalVariantCase[];
  scalarEnumCases: SortedMap<number, GlobalScalarEnumCaseId[]>;
  payloadEnumCases: SortedMap<number, GlobalEnumCaseId[]>;
  nestedScalarEnumCases: SortedMap<number, SortedMap<string, GlobalScalarEnumCaseId[]>>;
  nestedPayloadEnumCases: SortedMap<number, SortedMap<string, GlobalEnumCaseId[]>>;
};

export type PhysicalVariantCase = { tag: number; fields: SortedSet<string> };
export type PhysicalHistoryBinding = { storageTable: string; descriptor: any };

/** Permanent authority-issued identity; equality and order use only `id`. */
export type GlobalScalarEnumCaseId = {
  id: Uuid;
  introducingSchema: Uuid;
  introducingOrdinal: number;
};
export type GlobalEnumCaseId = { id: Uuid; introducingSchema: Uuid; introducingOrdinal: number };
export const caseIdEquals = (a: { id: Uuid }, b: { id: Uuid }) => equalBytes(a.id, b.id);
export const cmpCaseId = (a: { id: Uuid }, b: { id: Uuid }) => compareBytes(a.id, b.id);

/** A source cell while lowering a migration-lens path into a current winner projection. */
export type CurrentWinnerCellProjection =
  | { kind: "Field"; name: string; columnId: number; columnType: ValueType }
  | { kind: "Literal"; value: Value }
  | { kind: "Null" };

export function physicalMappingHasEnumBoundary(
  mapping: TablePhysicalMapping,
  columnId: number,
): boolean {
  return (
    mapping.scalarEnumCases.has(columnId) ||
    mapping.payloadEnumCases.has(columnId) ||
    (mapping.nestedScalarEnumCases.get(columnId)?.size ?? 0) > 0 ||
    (mapping.nestedPayloadEnumCases.get(columnId)?.size ?? 0) > 0
  );
}

export function valueTypeHasEnumBoundary(t: ValueType): boolean {
  switch (t.kind) {
    case "EnumTag":
    case "Enum":
      return true;
    case "Nullable":
    case "Array":
      return valueTypeHasEnumBoundary(t.inner);
    case "Tuple":
      return t.items.some(valueTypeHasEnumBoundary);
    case "Record":
      return t.descriptor.fields().some((f: any) => valueTypeHasEnumBoundary(f.valueType));
    default:
      return false;
  }
}

const fieldName = (f: any, message: string): string => {
  if (f.name === undefined) throw invalid(message);
  return f.name;
};

/** Bootstrap mappings have no durable registry entries yet: copy tags by ordinal. */
export function bootstrapCopyEnumRemaps(
  source: ValueType,
  target: ValueType,
  path: string,
  remaps: EnumOccurrenceRemaps,
): void {
  if (source.kind !== target.kind) return;
  switch (source.kind) {
    case "EnumTag":
      if (!remaps.scalar.has(path))
        remaps.scalar.set(
          path,
          source.schema.variants.map((_: any, ordinal: number) => {
            if (ordinal >= target.schema.variants.length) return undefined;
            if (ordinal > 0xff) throw invalid("bootstrap copied scalar enum tag exhausted");
            return ordinal;
          }),
        );
      return;
    case "Enum": {
      const sc = source.schema.cases;
      const tc = target.schema.cases;
      if (!remaps.payload.has(path))
        remaps.payload.set(
          path,
          sc.map((_: any, ordinal: number) => (ordinal < tc.length ? ordinal : undefined)),
        );
      if (!remaps.payloadChildren.has(path))
        remaps.payloadChildren.set(
          path,
          sc.map((_: any, ordinal: number) =>
            ordinal < tc.length ? `${path}/case/bootstrap/${ordinal}` : undefined,
          ),
        );
      for (let ordinal = 0; ordinal < Math.min(sc.length, tc.length); ordinal++)
        bootstrapCopyEnumRemaps(
          VT.record(sc[ordinal].payload),
          VT.record(tc[ordinal].payload),
          `${path}/case/bootstrap/${ordinal}`,
          remaps,
        );
      return;
    }
    case "Nullable":
      return bootstrapCopyEnumRemaps(source.inner, target.inner, `${path}/nullable`, remaps);
    case "Array":
      return bootstrapCopyEnumRemaps(source.inner, target.inner, `${path}/array`, remaps);
    case "Tuple":
      for (let i = 0; i < Math.min(source.items.length, target.items.length); i++)
        bootstrapCopyEnumRemaps(source.items[i], target.items[i], `${path}/tuple/${i}`, remaps);
      return;
    case "Record": {
      const sf = source.descriptor.fields();
      const tf = target.descriptor.fields();
      for (let i = 0; i < Math.min(sf.length, tf.length); i++) {
        const name = fieldName(sf[i], "bootstrap copied enum record field unnamed");
        bootstrapCopyEnumRemaps(sf[i].valueType, tf[i].valueType, `${path}/record/${name}`, remaps);
      }
    }
  }
}

/** Scalar enum tags in a physical table are interned spellings of identity, never ordinals. */
export const physicalScalarEnumCaseName = (c: { id: Uuid }) => `case-${toHex(c.id)}`;
export const physicalEnumCaseName = physicalScalarEnumCaseName;

/** Introducing schema alias, then ordinal, then UUID as tie-breaker. */
export function compareEnumCases(
  aliases: SortedMap<Uuid, number>,
  left: GlobalScalarEnumCaseId | GlobalEnumCaseId,
  right: GlobalScalarEnumCaseId | GlobalEnumCaseId,
): number {
  const la = aliases.get(left.introducingSchema);
  const ra = aliases.get(right.introducingSchema);
  if (la !== ra) return la === undefined ? -1 : ra === undefined ? 1 : la - ra;
  return left.introducingOrdinal - right.introducingOrdinal || compareBytes(left.id, right.id);
}
export const compareScalarEnumCases = compareEnumCases;
export const compareGlobalEnumCases = compareEnumCases;

export function physicalScalarEnumSchema(columnId: number, cases: GlobalScalarEnumCaseId[]): any {
  try {
    return ScalarEnumSchema.new_(
      `physical-column-${columnId}`,
      cases.map(physicalScalarEnumCaseName),
    ).withRegistryId(variantRegistryIdForPath(`physical-column/${columnId}/nullable`));
  } catch {
    throw invalid("invalid physical scalar enum registry");
  }
}

export type PayloadLayoutKey = string; // `${path}\u0000${hex(id)}`
export const payloadLayoutKey = (path: string, c: { id: Uuid }) => `${path}\u0000${toHex(c.id)}`;

/** Lower a nested authored value into the durable physical enum registries for one user column. */
export function physicalNestedEnumValueType(
  t: ValueType,
  path: string,
  scalarRegistries: Map<string, GlobalScalarEnumCaseId[]>,
  payloadRegistries: Map<string, GlobalEnumCaseId[]>,
  payloadLayouts: Map<PayloadLayoutKey, any>,
  columnId: number,
): ValueType {
  const rec = (inner: ValueType, p: string) =>
    physicalNestedEnumValueType(
      inner,
      p,
      scalarRegistries,
      payloadRegistries,
      payloadLayouts,
      columnId,
    );
  switch (t.kind) {
    case "EnumTag": {
      const cases = scalarRegistries.get(path);
      if (!cases) throw invalid("physical nested scalar enum registry missing");
      return VT.enumTag(physicalScalarEnumSchema(columnId, cases));
    }
    case "Nullable":
      return VT.nullable(rec(t.inner, `${path}/nullable`));
    case "Array":
      return VT.array(rec(t.inner, `${path}/array`));
    case "Tuple":
      return VT.tuple(t.items.map((item: ValueType, i: number) => rec(item, `${path}/tuple/${i}`)));
    case "Record":
      return VT.record(
        RecordDescriptor.new_(
          t.descriptor.fields().map((f: any): [string, ValueType] => {
            const name = fieldName(f, "nested scalar enum record field unnamed");
            return [name, rec(f.valueType, `${path}/record/${name}`)];
          }),
        ),
      );
    case "Enum": {
      const cases = payloadRegistries.get(path);
      if (!cases) throw invalid("physical nested payload enum registry missing");
      const lowered = cases.map((identity) => {
        const payload = payloadLayouts.get(payloadLayoutKey(path, identity));
        if (!payload) throw invalid("physical nested payload enum layout missing");
        const r = rec(VT.record(payload), globalCasePath(path, identity));
        return EnumCase.new_(physicalEnumCaseName(identity), r.descriptor);
      });
      let schema: any;
      try {
        schema = EnumSchema.new_(`physical-column-${columnId}-${path}`, lowered);
      } catch {
        throw invalid("invalid physical nested payload enum registry");
      }
      return VT.enum_(
        schema.withRegistryId(variantRegistryIdForPath(`physical-column/${columnId}/${path}`)),
      );
    }
    default:
      return t;
  }
}

/** One durable payload case identity must always describe one layout. */
export function collectNestedPayloadEnumLayouts(
  t: ValueType,
  path: string,
  identities: SortedMap<string, GlobalEnumCaseId[]>,
  output: Map<PayloadLayoutKey, [string, GlobalEnumCaseId, any]>,
): void {
  switch (t.kind) {
    case "Nullable":
      return collectNestedPayloadEnumLayouts(t.inner, `${path}/nullable`, identities, output);
    case "Array":
      return collectNestedPayloadEnumLayouts(t.inner, `${path}/array`, identities, output);
    case "Tuple":
      t.items.forEach((item: ValueType, i: number) =>
        collectNestedPayloadEnumLayouts(item, `${path}/tuple/${i}`, identities, output),
      );
      return;
    case "Record":
      for (const f of t.descriptor.fields())
        collectNestedPayloadEnumLayouts(
          f.valueType,
          `${path}/record/${fieldName(f, "nested payload enum record field unnamed")}`,
          identities,
          output,
        );
      return;
    case "Enum": {
      const cases = identities.get(path);
      if (!cases) throw invalid("nested payload enum identity mapping missing");
      if (cases.length !== t.schema.cases.length)
        throw invalid("nested payload enum identity mapping width mismatch");
      cases.forEach((identity, i) => {
        const c = t.schema.cases[i];
        const key = payloadLayoutKey(path, identity);
        const existing = output.get(key);
        if (!existing) output.set(key, [path, identity, c.payload]);
        else if (!descriptorEquals(existing[2], c.payload))
          throw invalid("same nested payload enum identity has incompatible layout");
        collectNestedPayloadEnumLayouts(
          VT.record(c.payload),
          globalCasePath(path, identity),
          identities,
          output,
        );
      });
    }
  }
}

export const newCaseSet = <C extends { id: Uuid }>() => new SortedSet<C>(cmpCaseId);
export type { Bytes };
