// Physical Groove table/descriptor construction and authored->physical enum value remapping.
import { SortedMap, SortedSet, cmpNum, cmpStr } from "../../../support/collections.js";
import {
  nodeError,
  V,
  VT,
  RecordDescriptor,
  OwnedRecord,
  EnumValue,
  EnumCase,
  EnumSchema,
  ScalarEnumSchema,
  variantRegistryIdForPath,
  GrooveColumnSchema,
  GrooveIndexSchema,
  GrooveTableSchema,
  GrooveTableVariantField,
  FieldIdentity,
  SCHEMA_FIELDS,
  largeValuesPhysicalStorageValueType,
  hydrateNestedScalarEnumCases,
  hydrateNestedPayloadEnumCases,
  globalCasePath,
  descriptorEquals,
  valueTypeEquals,
  type Value,
  type ValueType,
  type Uuid,
  type EnumOccurrenceRemaps,
  type GrooveTableSchema as GrooveTable,
} from "../../../_extern/node-physical-ingest.js";
import { HistoryRowRecord, GlobalCurrentRowRecord, RejectedVersionRowRecord } from "../codec.js";
import {
  type TablePhysicalMapping,
  type SchemaPhysicalMapping,
  type GlobalScalarEnumCaseId,
  type GlobalEnumCaseId,
  type PayloadLayoutKey,
  grooveVariantTag,
  compareEnumCases,
  physicalScalarEnumSchema,
  physicalEnumCaseName,
  physicalNestedEnumValueType,
  collectNestedPayloadEnumLayouts,
  payloadLayoutKey,
  newCaseSet,
  caseIdEquals,
} from "./catalogue.js";
import {
  physicalHistoryTableName,
  physicalGlobalCurrentTableName,
  physicalAheadCurrentTableName,
  physicalRejectedVersionsTableName,
  physicalCurrentIndexName,
  physicalCurrentCompositeIndexName,
  physicalUserColumnField,
} from "./names.js";

const invalid = (m: string) => nodeError("InvalidStoredValue", m);

function findPhysicalColumn(table: GrooveTable, name: string, missing: string): any {
  const c = table.columns.find((c: any) => c.name === name);
  if (!c) throw invalid(missing);
  return c;
}

export function widenedProjectionDescriptor(
  logical: any,
  physicalNames: string[],
  physicalTable: GrooveTable,
): any {
  const fields = logical.fields();
  if (fields.length !== physicalNames.length)
    throw invalid("physical projection descriptor width mismatch");
  return RecordDescriptor.new_(
    fields.map((field: any, i: number) => {
      const physical = findPhysicalColumn(
        physicalTable,
        physicalNames[i],
        "physical projection column missing",
      );
      if (field.name === undefined) throw invalid("physical projection logical field unnamed");
      return [field.name, widenProjectionValueType(field.valueType, physical.columnType)];
    }),
  );
}

/** Writes target the physical table itself: physical names and physical enum registries. */
export function physicalWriteDescriptor(
  logical: any,
  physicalNames: string[],
  physicalTable: GrooveTable,
): any {
  if (logical.fields().length !== physicalNames.length)
    throw invalid("physical write descriptor width mismatch");
  return RecordDescriptor.new_(
    physicalNames.map((name) => [
      name,
      findPhysicalColumn(physicalTable, name, "physical write column missing").columnType,
    ]),
  );
}

export function remapNestedEnumValue(
  value: Value,
  authored: ValueType,
  physical: ValueType,
  remaps: EnumOccurrenceRemaps,
  path: string,
): Value {
  const k = value.kind;
  if (k === "EnumTag" && authored.kind === "EnumTag" && physical.kind === "EnumTag") {
    const tag = remaps.scalar.get(path)?.[value.value];
    if (tag === undefined) throw invalid("nested scalar enum tag absent from physical mapping");
    return V.enumTag(tag);
  }
  if (k === "Nullable" && authored.kind === "Nullable" && physical.kind === "Nullable")
    return value.value === undefined
      ? V.nullable()
      : V.nullable(
          remapNestedEnumValue(
            value.value,
            authored.inner,
            physical.inner,
            remaps,
            `${path}/nullable`,
          ),
        );
  if (k === "Array" && authored.kind === "Array" && physical.kind === "Array")
    return V.array(
      value.value.map((v: Value) =>
        remapNestedEnumValue(v, authored.inner, physical.inner, remaps, `${path}/array`),
      ),
    );
  if (
    k === "Tuple" &&
    authored.kind === "Tuple" &&
    physical.kind === "Tuple" &&
    authored.items.length === physical.items.length &&
    value.value.length === authored.items.length
  )
    return V.tuple(
      value.value.map((v: Value, i: number) =>
        remapNestedEnumValue(v, authored.items[i], physical.items[i], remaps, `${path}/tuple/${i}`),
      ),
    );
  if (
    k === "Record" &&
    authored.kind === "Record" &&
    physical.kind === "Record" &&
    authored.descriptor.fields().length === physical.descriptor.fields().length
  ) {
    const af = authored.descriptor.fields();
    const pf = physical.descriptor.fields();
    const values = value.value.toValues().map((v: Value, i: number) => {
      if (af[i].name === undefined) throw invalid("nested record enum field unnamed");
      return remapNestedEnumValue(
        v,
        af[i].valueType,
        pf[i].valueType,
        remaps,
        `${path}/record/${af[i].name}`,
      );
    });
    return V.record(OwnedRecord.new_(physical.descriptor.create(values), physical.descriptor));
  }
  if (k === "Enum" && authored.kind === "Enum" && physical.kind === "Enum") {
    const authoredTag = value.value.tag();
    const physicalTag = remaps.payload.get(path)?.[authoredTag];
    if (physicalTag === undefined)
      throw invalid("nested payload enum tag absent from physical mapping");
    const authoredCase = authored.schema.case(authoredTag);
    const physicalCase = physical.schema.case(physicalTag);
    const af = authoredCase.payload.fields();
    const pf = physicalCase.payload.fields();
    if (af.length !== pf.length) throw invalid("nested payload enum payload width changed");
    const semanticChildRoot = remaps.payloadChildren.get(path)?.[authoredTag];
    const childRoot = semanticChildRoot ?? `${path}/case/${authoredTag}`;
    const values = value.value
      .record()
      .toValues()
      .map((v: Value, i: number) => {
        if (af[i].name === undefined) throw invalid("nested payload enum field unnamed");
        const childPath =
          semanticChildRoot !== undefined
            ? `${childRoot}/record/${af[i].name}`
            : `${childRoot}/${af[i].name}`;
        return remapNestedEnumValue(v, af[i].valueType, pf[i].valueType, remaps, childPath);
      });
    return V.enumValue(
      EnumValue.new_(
        physicalTag,
        OwnedRecord.new_(physicalCase.payload.create(values), physicalCase.payload),
      ),
    );
  }
  if (valueTypeEquals(authored, physical)) return value;
  throw invalid("nested enum remap descriptor mismatch");
}

function widenProjectionValueType(logical: ValueType, physical: ValueType): ValueType {
  // Large JSON's durable cell is an internal stored-scalar enum: keep that descriptor.
  if (physical.isInternalStorageType()) return physical;
  if (logical.kind === "EnumTag" && physical.kind === "EnumTag") return logical;
  if (logical.kind === "Enum" && physical.kind === "Enum") return logical;
  if (physical.kind === "Nullable" && logical.kind !== "Nullable")
    return widenProjectionValueType(logical, physical.inner);
  if (logical.kind === "Nullable" && physical.kind === "Nullable")
    return VT.nullable(widenProjectionValueType(logical.inner, physical.inner));
  if (logical.kind === "Array" && physical.kind === "Array")
    return VT.array(widenProjectionValueType(logical.inner, physical.inner));
  if (
    logical.kind === "Tuple" &&
    physical.kind === "Tuple" &&
    logical.items.length === physical.items.length
  )
    return VT.tuple(
      logical.items.map((l: ValueType, i: number) =>
        widenProjectionValueType(l, physical.items[i]),
      ),
    );
  return logical;
}

type Variant = [Uuid, any, TablePhysicalMapping, any];
type ColumnPathKey = string; // `${columnId}\u0000${path}`
const columnPathKey = (columnId: number, path: string) => `${columnId}\u0000${path}`;

/** Union identities per occurrence and sort them in durable introduction order. */
function sortedRegistries<C extends GlobalScalarEnumCaseId | GlobalEnumCaseId>(
  registries: Map<string, SortedSet<C>>,
  aliases: SortedMap<Uuid, number>,
): Map<string, C[]> {
  const out = new Map<string, C[]>();
  for (const [key, cases] of registries)
    out.set(
      key,
      cases.toArray().sort((l, r) => compareEnumCases(aliases, l, r)),
    );
  return out;
}
function addCases<C extends { id: Uuid }>(
  registries: Map<string, SortedSet<C>>,
  key: string,
  cases: Iterable<C>,
) {
  let set = registries.get(key);
  if (!set) registries.set(key, (set = newCaseSet<C>()));
  for (const c of cases) set.add(c);
}

function bootstrapIdentities<C>(
  variants: any[],
  rootIds: Uuid[],
  schemaVersion: Uuid,
  max: number,
  exhausted: string,
): C[] {
  return variants.slice(0, rootIds.length).map((_, ordinal) => {
    if (ordinal > max) throw invalid(exhausted);
    return {
      id: rootIds[ordinal],
      introducingSchema: schemaVersion,
      introducingOrdinal: ordinal,
    } as C;
  });
}

function columnIdOf(mapping: TablePhysicalMapping, column: string, missing: string): number {
  const id = mapping.columns.get(column);
  if (id === undefined) throw invalid(missing);
  return id;
}

export function physicalVersionStorageTables(
  catalogueSchemas: SortedMap<Uuid, any>,
  aliases: SortedMap<Uuid, number>,
  mappings: SortedMap<Uuid, SchemaPhysicalMapping>,
): GrooveTable[] {
  const lineages = new SortedMap<number, Variant[]>(cmpNum);
  for (const [schemaVersion, mapping] of mappings) {
    const schema = catalogueSchemas.get(schemaVersion);
    if (!schema) throw invalid("physical mapping schema payload missing");
    for (const [logicalTable, tableMapping] of mapping.tables) {
      const identity = mapping.identities.tables.get(logicalTable);
      if (!identity) throw invalid("physical table identity missing");
      const table = schema.schema.tables.find((t: any) => t.name === logicalTable);
      if (!table) throw invalid("physical mapping logical table missing");
      lineages
        .getOrInsert(tableMapping.tableId, () => [])
        .push([schemaVersion, table, tableMapping, identity]);
    }
  }

  const tables: GrooveTable[] = [];
  for (const [tableId, variants] of lineages) {
    const templateTable = variants[0][1];
    const template = templateTable.historyStorageTable();
    const systemColumns = template.columns.slice(0, HistoryRowRecord.USER_CELLS);
    const trailingHistoryColumns = template.columns.slice(
      HistoryRowRecord.USER_CELLS + templateTable.columns.length,
    );

    // Concurrent schemas may reuse an authored ordinal for distinct cases: union identities first.
    const scalarSets = new Map<string, SortedSet<GlobalScalarEnumCaseId>>();
    for (const [schemaVersion, logical, mapping, identity] of variants)
      for (const column of logical.columns) {
        if (column.columnType.kind !== "EnumTag") continue;
        const columnId = columnIdOf(
          mapping,
          column.name,
          "physical scalar enum column mapping missing",
        );
        const cases =
          mapping.scalarEnumCases.get(columnId) ??
          bootstrapIdentities<GlobalScalarEnumCaseId>(
            column.columnType.schema.variants,
            identity.columns.get(column.name).enumVariants.get("root"),
            schemaVersion,
            0xff,
            "scalar enum tag exhausted",
          );
        addCases(scalarSets, String(columnId), cases);
      }
    const scalarRegistries = sortedRegistries(scalarSets, aliases);

    const nestedScalarSets = new Map<ColumnPathKey, SortedSet<GlobalScalarEnumCaseId>>();
    for (const [schemaVersion, logical, mapping, identity] of variants) {
      // Bootstrap: seed nested occurrences from the authored descriptor before hydration.
      let paths = mapping.nestedScalarEnumCases;
      if (paths.size === 0) {
        paths = new SortedMap(cmpNum);
        for (const column of logical.columns) {
          if (column.columnType.kind === "EnumTag") continue;
          const columnId = columnIdOf(
            mapping,
            column.name,
            "physical nested scalar enum column mapping missing",
          );
          hydrateNestedScalarEnumCases(
            column.columnType,
            identity.columns.get(column.name).enumVariants,
            schemaVersion,
            "root",
            "root",
            paths.getOrInsert(columnId, () => new SortedMap(cmpStr)),
          );
        }
      }
      for (const [columnId, byPath] of paths)
        for (const [path, cases] of byPath)
          addCases(nestedScalarSets, columnPathKey(columnId, path), cases);
    }
    const nestedScalarRegistries = sortedRegistries(nestedScalarSets, aliases);

    // Payload occurrences beneath a payload case are rooted by that case's global UUID.
    const nestedPayloadSets = new Map<ColumnPathKey, SortedSet<GlobalEnumCaseId>>();
    const nestedPayloadLayouts = new Map<string, any>();
    for (const [schemaVersion, logical, mapping, identity] of variants)
      for (const column of logical.columns) {
        const columnId = columnIdOf(
          mapping,
          column.name,
          "physical nested payload enum column mapping missing",
        );
        let paths = mapping.nestedPayloadEnumCases.get(columnId);
        if (mapping.nestedPayloadEnumCases.size === 0) {
          paths = new SortedMap(cmpStr);
          hydrateNestedPayloadEnumCases(
            column.columnType,
            identity.columns.get(column.name).enumVariants,
            schemaVersion,
            "root",
            "root",
            paths,
          );
        }
        if (!paths) continue;
        for (const [path, cases] of paths)
          addCases(nestedPayloadSets, columnPathKey(columnId, path), cases);
        const layouts = new Map<PayloadLayoutKey, [string, GlobalEnumCaseId, any]>();
        collectNestedPayloadEnumLayouts(column.columnType, "root", paths, layouts);
        for (const [key, [, , layout]] of layouts) {
          const k = `${columnId}\u0000${key}`;
          const existing = nestedPayloadLayouts.get(k);
          if (!existing) nestedPayloadLayouts.set(k, layout);
          else if (!descriptorEquals(existing, layout))
            throw invalid("same nested payload enum identity has incompatible layout");
        }
      }
    const nestedPayloadRegistries = sortedRegistries(nestedPayloadSets, aliases);

    const payloadSets = new Map<string, SortedSet<GlobalEnumCaseId>>();
    const payloadLayouts = new Map<string, any>();
    for (const [schemaVersion, logical, mapping, identity] of variants)
      for (const column of logical.columns) {
        if (column.columnType.kind !== "Enum") continue;
        const enumCases = column.columnType.schema.cases;
        const columnId = columnIdOf(
          mapping,
          column.name,
          "physical payload enum column mapping missing",
        );
        const identities =
          mapping.payloadEnumCases.get(columnId) ??
          bootstrapIdentities<GlobalEnumCaseId>(
            enumCases,
            identity.columns.get(column.name).enumVariants.get("root"),
            schemaVersion,
            0xffffffff,
            "payload enum tag exhausted",
          );
        if (identities.length !== enumCases.length)
          throw invalid("payload enum identity mapping width mismatch");
        const nestedRoot = mapping.nestedPayloadEnumCases.get(columnId)?.get("root");
        if (
          nestedRoot &&
          (nestedRoot.length !== identities.length ||
            nestedRoot.some((c, i) => !caseIdEquals(c, identities[i])))
        )
          throw invalid("direct and nested payload enum identity mappings diverged");
        identities.forEach((id, i) => {
          const key = payloadLayoutKey(String(columnId), id);
          const existing = payloadLayouts.get(key);
          if (!existing) payloadLayouts.set(key, enumCases[i].payload);
          else if (!descriptorEquals(existing, enumCases[i].payload))
            throw invalid("same payload enum identity has incompatible layout");
        });
        addCases(payloadSets, String(columnId), identities);
      }
    const payloadRegistries = sortedRegistries(payloadSets, aliases);

    const nestedFor = (columnId: number) => {
      const prefix = `${columnId}\u0000`;
      const strip = <T>(m: Map<string, T>) =>
        new Map(
          [...m]
            .filter(([k]) => k.startsWith(prefix))
            .map(([k, v]): [string, T] => [k.slice(prefix.length), v]),
        );
      return [
        strip(nestedScalarRegistries),
        strip(nestedPayloadRegistries),
        strip(nestedPayloadLayouts),
      ] as const;
    };

    const physicalColumns = new SortedMap<number, ValueType>(cmpNum);
    for (const [, logical, mapping] of variants)
      for (const column of logical.columns) {
        const columnId = columnIdOf(
          mapping,
          column.name,
          "physical history column mapping missing",
        );
        let storageType: ValueType;
        if (column.columnType.kind === "EnumTag") {
          const cases = scalarRegistries.get(String(columnId));
          if (!cases) throw invalid("physical scalar enum registry missing");
          storageType = VT.nullable(VT.enumTag(physicalScalarEnumSchema(columnId, cases)));
        } else if (column.columnType.kind === "Enum") {
          const cases = payloadRegistries.get(String(columnId));
          if (!cases) throw invalid("physical payload enum registry missing");
          const [ns, np, nl] = nestedFor(columnId);
          const lowered = cases.map((identity) => {
            const payload = payloadLayouts.get(payloadLayoutKey(String(columnId), identity));
            if (!payload) throw invalid("physical payload enum layout missing");
            const r = physicalNestedEnumValueType(
              VT.record(payload),
              globalCasePath("root", identity),
              ns,
              np,
              nl,
              columnId,
            );
            return EnumCase.new_(physicalEnumCaseName(identity), r.descriptor);
          });
          let schema: any;
          try {
            schema = EnumSchema.new_(`physical-column-${columnId}`, lowered);
          } catch {
            throw invalid("invalid physical payload enum registry");
          }
          storageType = VT.nullable(
            VT.enum_(
              schema.withRegistryId(variantRegistryIdForPath(`physical-column/${columnId}`)),
            ),
          );
        } else {
          const [ns, np, nl] = nestedFor(columnId);
          storageType =
            ns.size || np.size
              ? VT.nullable(
                  physicalNestedEnumValueType(column.columnType, "root", ns, np, nl, columnId),
                )
              : VT.nullable(physicalStorageValueType(column)).rebindVariantRegistries(
                  `physical-column/${columnId}`,
                );
        }
        const existing = physicalColumns.get(columnId);
        physicalColumns.set(
          columnId,
          existing ? mergePhysicalValueType(existing, storageType) : storageType,
        );
      }
    const userColumns = () =>
      [...physicalColumns].map(([id, t]) =>
        GrooveColumnSchema.new_(physicalUserColumnField(id), t),
      );

    let physical = GrooveTableSchema.newWithBoundRegistries(physicalHistoryTableName(tableId), [
      ...systemColumns,
      ...userColumns(),
      ...trailingHistoryColumns,
    ]);
    physical.primaryKey = template.primaryKey;
    physical.indices = template.indices;

    const logicalGlobal = templateTable.globalCurrentStorageTable();
    const currentColumns = () => [
      ...logicalGlobal.columns.slice(0, GlobalCurrentRowRecord.USER_CELLS),
      ...userColumns(),
      ...logicalGlobal.columns.slice(
        GlobalCurrentRowRecord.USER_CELLS + templateTable.columns.length,
      ),
    ];
    let physicalGlobal = GrooveTableSchema.newWithBoundRegistries(
      physicalGlobalCurrentTableName(tableId),
      currentColumns(),
    );
    physicalGlobal.primaryKey = logicalGlobal.primaryKey;
    physicalGlobal = physicalGlobal.withIndex(
      GrooveIndexSchema.new_(SCHEMA_FIELDS.GLOBAL_CURRENT_BY_SEQ_INDEX, [
        "branch_key",
        "global_time",
        "row_uuid",
      ]),
    );
    const indexedColumns = new SortedSet<number>(cmpNum);
    const compositeIndexes = new Map<string, number[]>();
    for (const [, logical, mapping] of variants) {
      for (const column of logical.globalCurrentIndexedColumns()) {
        const id = mapping.columns.get(column);
        if (id !== undefined) indexedColumns.add(id);
      }
      for (const columns of logical.compositeIndexes) {
        const ids = columns.map((c: string) => mapping.columns.get(c));
        if (ids.every((id: number | undefined) => id !== undefined))
          compositeIndexes.set(ids.join(","), ids);
      }
    }
    const sortedComposites = [...compositeIndexes.values()].sort((a, b) => {
      for (let i = 0; i < Math.min(a.length, b.length); i++) if (a[i] !== b[i]) return a[i] - b[i];
      return a.length - b.length;
    });
    const withCurrentIndexes = (t: any) => {
      for (const id of indexedColumns)
        t = t.withIndex(
          GrooveIndexSchema.new_(physicalCurrentIndexName(id), [
            "branch_key",
            physicalUserColumnField(id),
          ]),
        );
      for (const ids of sortedComposites)
        t = t.withIndex(
          GrooveIndexSchema.new_(physicalCurrentCompositeIndexName(ids), [
            "branch_key",
            ...ids.map(physicalUserColumnField),
          ]),
        );
      return t;
    };
    physicalGlobal = withCurrentIndexes(physicalGlobal);
    const logicalAhead = templateTable.aheadCurrentStorageTable();
    let physicalAhead = GrooveTableSchema.newWithBoundRegistries(
      physicalAheadCurrentTableName(tableId),
      currentColumns(),
    );
    physicalAhead.primaryKey = logicalAhead.primaryKey;
    physicalAhead.indices = logicalAhead.indices;
    physicalAhead = withCurrentIndexes(physicalAhead);

    const rejectedTemplate = templateTable.rejectedVersionsStorageTable();
    let rejected = GrooveTableSchema.newWithBoundRegistries(
      physicalRejectedVersionsTableName(tableId),
      [...rejectedTemplate.columns.slice(0, RejectedVersionRowRecord.USER_CELLS), ...userColumns()],
    );
    rejected.primaryKey = rejectedTemplate.primaryKey;

    const layoutsByTag = new SortedMap<number, string[]>(cmpNum);
    const currentLayoutsByTag = new SortedMap<number, string[]>(cmpNum);
    const rejectedLayoutsByTag = new SortedMap<number, string[]>(cmpNum);
    for (const [schemaVersion, logical, mapping] of variants) {
      const alias = aliases.get(schemaVersion);
      if (alias === undefined) throw invalid("physical history schema alias missing");
      const cases: [number, SortedSet<string> | undefined][] = mapping.variantCases.length
        ? mapping.variantCases.map((c) => [c.tag, c.fields])
        : [[grooveVariantTag(alias), undefined]];
      for (const [tag, fields] of cases) {
        if (
          layoutsByTag.set(tag, physicalHistoryFieldNamesForCase(logical, mapping, fields)) !==
            undefined ||
          currentLayoutsByTag.set(
            tag,
            physicalCurrentFieldNamesForCase(logical, mapping, fields),
          ) !== undefined ||
          rejectedLayoutsByTag.set(
            tag,
            physicalRejectedVersionFieldNamesForCase(logical, mapping, fields),
          ) !== undefined
        )
          throw invalid("physical table variant tag collision");
      }
    }
    for (const [tag, fields] of layoutsByTag)
      physical = physical.withVariantPayload(tag, variantPayloadFieldsForNames(physical, fields));
    for (const [tag, fields] of currentLayoutsByTag) {
      physicalGlobal = physicalGlobal.withVariantPayload(
        tag,
        variantPayloadFieldsForNames(physicalGlobal, fields),
      );
      physicalAhead = physicalAhead.withVariantPayload(
        tag,
        variantPayloadFieldsForNames(physicalAhead, fields),
      );
    }
    for (const [tag, fields] of rejectedLayoutsByTag)
      rejected = rejected.withVariantPayload(tag, variantPayloadFieldsForNames(rejected, fields));
    tables.push(physical, physicalGlobal, physicalAhead, rejected);
  }
  return tables;
}

/** JSON is logically a String but physically an internal stored scalar. */
function physicalStorageValueType(column: any): ValueType {
  return column.largeValueKind === "Json"
    ? largeValuesPhysicalStorageValueType("Json")
    : column.columnType;
}

function variantPayloadFieldsForNames(table: GrooveTable, names: string[]): any[] {
  return names.map((name) =>
    GrooveTableVariantField.shared(
      name,
      findPhysicalColumn(table, name, "physical variant shared column missing").columnType,
      name,
    ),
  );
}

function mergePhysicalRecordDescriptor(existing: any, incoming: any): any {
  const ef = existing.fields();
  const inf = incoming.fields();
  if (ef.length !== inf.length) throw invalid("physical variant payload descriptor width changed");
  return RecordDescriptor.new_(
    ef.map((l: any, i: number) => {
      if (l.name !== inf[i].name) throw invalid("physical variant payload field identity changed");
      if (l.name === undefined) throw invalid("physical variant payload field unnamed");
      return [l.name, mergePhysicalValueType(l.valueType, inf[i].valueType)];
    }),
  );
}

/** Registry identity is authoritative for enums; the older declaration must be a prefix. */
function mergePhysicalValueType(existing: ValueType, incoming: ValueType): ValueType {
  if (
    existing.kind === "EnumTag" &&
    incoming.kind === "EnumTag" &&
    existing.schema.registryId() === incoming.schema.registryId()
  ) {
    // Preserve the established tag prefix; append only newly observed opaque names.
    const variants = [...existing.schema.variants];
    for (const v of incoming.schema.variants) if (!variants.includes(v)) variants.push(v);
    try {
      return VT.enumTag(
        ScalarEnumSchema.new_(existing.schema.name, variants).withRegistryId(
          existing.schema.registryId(),
        ),
      );
    } catch {
      throw invalid("invalid physical enum registry");
    }
  }
  if (
    existing.kind === "Enum" &&
    incoming.kind === "Enum" &&
    existing.schema.registryId === incoming.schema.registryId
  ) {
    const cases = existing.schema.cases.map((c: any) => ({ ...c }));
    for (const c of incoming.schema.cases) {
      const hit = cases.find((e: any) => e.name === c.name);
      if (hit) hit.payload = mergePhysicalRecordDescriptor(hit.payload, c.payload);
      else cases.push(c);
    }
    try {
      return VT.enum_(
        EnumSchema.new_(incoming.schema.name, cases).withRegistryId(existing.schema.registryId),
      );
    } catch {
      throw invalid("invalid physical enum registry");
    }
  }
  if (
    existing.kind === "Tuple" &&
    incoming.kind === "Tuple" &&
    existing.items.length === incoming.items.length
  )
    return VT.tuple(
      existing.items.map((t: ValueType, i: number) => mergePhysicalValueType(t, incoming.items[i])),
    );
  if (existing.kind === "Array" && incoming.kind === "Array")
    return VT.array(mergePhysicalValueType(existing.inner, incoming.inner));
  if (existing.kind === "Nullable" && incoming.kind === "Nullable")
    return VT.nullable(mergePhysicalValueType(existing.inner, incoming.inner));
  if (existing.kind === "Record" && incoming.kind === "Record")
    return VT.record(mergePhysicalRecordDescriptor(existing.descriptor, incoming.descriptor));
  if (valueTypeEquals(existing, incoming)) return existing;
  throw invalid("physical history column type mismatch");
}

export function physicalCurrentDescriptor(table: any, mapping: TablePhysicalMapping): any {
  const logical = table.globalCurrentStorageTable().recordSchema();
  const names = physicalCurrentFieldNames(table, mapping);
  if (logical.fields().length !== names.length)
    throw invalid("physical current descriptor width mismatch");
  return physicalDescriptorWithEnumRegistries(table, logical, names, mapping);
}

export function physicalRejectedVersionDescriptor(table: any, mapping: TablePhysicalMapping): any {
  const logical = table.rejectedVersionsStorageTable().recordSchema();
  const names = physicalRejectedVersionFieldNamesForCase(table, mapping, undefined);
  if (logical.fields().length !== names.length)
    throw invalid("physical rejected-version descriptor width mismatch");
  return physicalDescriptorWithEnumRegistries(table, logical, names, mapping);
}

function physicalDescriptorWithEnumRegistries(
  table: any,
  logical: any,
  physicalNames: string[],
  mapping: TablePhysicalMapping,
): any {
  const slot = (name: string) => (/^_app_\d+$/.test(name) ? Number(name.slice(5)) : undefined);
  const fields = logical.fields();
  return RecordDescriptor.newWithFields(
    physicalNames.map((name, i) => {
      const field = fields[i];
      const id = slot(name);
      if (id === undefined) return { name, identity: field.identity, valueType: field.valueType };
      const logicalName = field.logicalName();
      const identity =
        logicalName !== undefined && logicalName !== name
          ? FieldIdentity.namedSlot(logicalName, id)
          : FieldIdentity.slot(id);
      let valueType: ValueType;
      const cases = mapping.scalarEnumCases.get(id);
      const columnName = [...mapping.columns].find(([, c]) => c === id)?.[0];
      const column =
        columnName === undefined
          ? undefined
          : table.columns.find((c: any) => c.name === columnName);
      if (cases) valueType = VT.nullable(VT.enumTag(physicalScalarEnumSchema(id, cases)));
      else if (column) valueType = VT.nullable(physicalStorageValueType(column));
      else if (field.valueType.kind === "Nullable")
        valueType = VT.nullable(
          field.valueType.inner.rebindVariantRegistries(`physical-column/${id}`),
        );
      else valueType = field.valueType.rebindVariantRegistries(`physical-column/${id}`);
      return { name, identity, valueType };
    }),
  );
}

export const physicalHistoryFieldNames = (table: any, mapping: TablePhysicalMapping) =>
  physicalHistoryFieldNamesForCase(table, mapping, undefined);
export const physicalCurrentFieldNames = (table: any, mapping: TablePhysicalMapping) =>
  physicalCurrentFieldNamesForCase(table, mapping, undefined);

export function physicalHistoryFieldNamesForCase(
  table: any,
  mapping: TablePhysicalMapping,
  present: SortedSet<string> | undefined,
): string[] {
  return physicalRowFieldNames(
    table,
    mapping,
    present,
    HistoryRowRecord.PREFIX_FIELD_NAMES,
    [
      "authored_columns",
      SCHEMA_FIELDS.COUNTER_SIGNS_FIELD,
      SCHEMA_FIELDS.SEQ_FIELD,
      SCHEMA_FIELDS.BASE_SEQ_FIELD,
      SCHEMA_FIELDS.BASE_PENDING_FIELD,
    ],
    "physical history column mapping missing",
  );
}
export function physicalCurrentFieldNamesForCase(
  table: any,
  mapping: TablePhysicalMapping,
  present: SortedSet<string> | undefined,
): string[] {
  return physicalRowFieldNames(
    table,
    mapping,
    present,
    GlobalCurrentRowRecord.PREFIX_FIELD_NAMES,
    ["authored_columns"],
    "physical current column mapping missing",
  );
}
function physicalRejectedVersionFieldNamesForCase(
  table: any,
  mapping: TablePhysicalMapping,
  present: SortedSet<string> | undefined,
): string[] {
  return physicalRowFieldNames(
    table,
    mapping,
    present,
    RejectedVersionRowRecord.PREFIX_FIELD_NAMES,
    [],
    "physical rejected-version column mapping missing",
  );
}

/** Fixed prefix, user fields in schema order by physical id, fixed suffix. */
function physicalRowFieldNames(
  table: any,
  mapping: TablePhysicalMapping,
  present: SortedSet<string> | undefined,
  prefix: string[],
  suffix: string[],
  missing: string,
): string[] {
  const fields = [...prefix];
  for (const column of table.columns) {
    if (present && !present.has(column.name)) continue;
    fields.push(physicalUserColumnField(columnIdOf(mapping, column.name, missing)));
  }
  return fields.concat(suffix);
}
