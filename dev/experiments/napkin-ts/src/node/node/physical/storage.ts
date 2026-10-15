// Physical write plans, authored-column/contribution id translation and record encoding.
import { equalBytes } from "../../../support/bytes.js";
import { SortedMap, SortedSet, cmpNum, cmpStr } from "../../../support/collections.js";
import {
  nodeError,
  V,
  OwnedRecord,
  ValidatedVariantRecord,
  VariantRecord,
  RecursiveEnumRemaps,
  globalCasePath,
  valueEquals,
  valueTypeEquals,
  txTimePhysicalMs,
  type NodeState,
  type Value,
  type Uuid,
  type EnumOccurrenceRemaps,
} from "../../../_extern/node-physical-ingest.js";
import {
  DELETION_COLUMN_ID,
  DELETION_COLUMN_NAME,
  HistoryRowRecord,
  GlobalCurrentRowRecord,
  VersionRow,
  rowAuthorValue,
  historyUpdatedByValue,
  historyPrimaryKey,
  contributionMergeStorageValue as encodeContributionMerge,
  contributionMergeFromStorageRecord as decodeContributionMerge,
} from "../codec.js";
import type {
  PreparedPhysicalWritePlan,
  PhysicalWriteTarget,
  PhysicalWriteField,
} from "../physical.js";
import { type TablePhysicalMapping, grooveVariantTag } from "./catalogue.js";
import {
  physicalHistoryStorageTable,
  physicalCurrentStorageTable,
  physicalRejectedVersionBinding,
} from "./bindings.js";
import {
  physicalHistoryFieldNames,
  physicalCurrentFieldNames,
  physicalWriteDescriptor,
  remapNestedEnumValue,
} from "./descriptors.js";
import {
  physicalToAuthoredEnumRemaps,
  physicalScalarEnumCases,
  physicalPayloadEnumCases,
  physicalNestedScalarEnumCases,
  physicalNestedPayloadEnumCases,
} from "./enum_lookup.js";
import { physicalTableIdForSchema } from "./projections.js";

const invalid = (m: string) => nodeError("InvalidStoredValue", m);

function tableMapping(
  node: NodeState,
  schemaVersion: Uuid,
  table: string,
  missing: string,
): TablePhysicalMapping {
  const m = node.catalogue.physicalMappings.get(schemaVersion)?.tables.get(table);
  if (!m) throw invalid(missing);
  return m;
}

export function authoredColumnIdsForNames(
  node: NodeState,
  schemaVersion: Uuid,
  table: string,
  columns: Iterable<string> | undefined,
): SortedSet<number> | undefined {
  if (!columns) return undefined;
  const mapping = tableMapping(
    node,
    schemaVersion,
    table,
    "authored columns physical table mapping missing",
  );
  const ids = new SortedSet<number>(cmpNum);
  for (const column of columns) {
    if (column === DELETION_COLUMN_NAME) {
      ids.add(DELETION_COLUMN_ID);
      continue;
    }
    const id = mapping.columns.get(column);
    if (id === undefined) throw invalid("authored column physical mapping missing");
    ids.add(id);
  }
  return ids;
}

export function authoredColumnNamesForIds(
  node: NodeState,
  schemaVersion: Uuid,
  table: string,
  columns: Iterable<number> | undefined,
): SortedSet<string> | undefined {
  if (!columns) return undefined;
  const mapping = tableMapping(
    node,
    schemaVersion,
    table,
    "authored columns physical table mapping missing",
  );
  const namesById = new Map<number, string>();
  for (const [name, id] of mapping.columns) {
    if (namesById.has(id)) throw invalid("physical table maps multiple authored columns to one id");
    namesById.set(id, name);
  }
  const names = new SortedSet<string>(cmpStr);
  for (const column of columns) {
    if (column === DELETION_COLUMN_ID) names.add(DELETION_COLUMN_NAME);
    else {
      const name = namesById.get(column);
      if (name === undefined)
        throw invalid("stored authored column id is absent from its schema mapping");
      names.add(name);
    }
  }
  return names;
}

/** A physical user cell re-tagged to `schemaVersion`'s authored enum tags. */
export function physicalCellToAuthored(
  node: NodeState,
  schemaVersion: Uuid,
  table: string,
  column: number,
  value: Value,
): Value {
  const plan = preparedPhysicalWritePlan(node, schemaVersion, table, "History");
  const index = plan.writeFields.findIndex((f) => f.kind === "Enum" && f.column === column);
  if (index < 0) return value;
  const physicalType = plan.physicalDescriptor.fields()[index].valueType;
  const authoredType = plan.sourceTable.columns[column].columnType;
  const mapping = tableMapping(
    node,
    schemaVersion,
    table,
    "enum cell physical table mapping missing",
  );
  const columnId = mapping.columns.get(plan.sourceTable.columns[column].name);
  if (columnId === undefined) throw invalid("enum cell physical column mapping missing");
  const remaps = physicalToAuthoredEnumRemaps(node, mapping, columnId);
  if (value.kind === "Nullable" && value.value !== undefined && physicalType.kind === "Nullable")
    return V.nullable(
      remapNestedEnumValue(value.value, physicalType.inner, authoredType, remaps, "root"),
    );
  return value;
}

/** Resolve a logical name to one local physical id: active schema first, else one unambiguous retained mapping. */
function resolveUnique<T>(
  current: T | undefined,
  all: Iterable<T>,
  missing: string,
  ambiguous: string,
): T {
  if (current !== undefined) return current;
  const unique = new Set(all);
  if (unique.size === 0) throw invalid(missing);
  if (unique.size > 1) throw invalid(ambiguous);
  return unique.values().next().value as T;
}

function allTables(node: NodeState): [string, TablePhysicalMapping][] {
  return node.catalogue.physicalMappings.valuesArray().flatMap((m: any) => [...m.tables]);
}
const localTables = (node: NodeState) =>
  node.catalogue.physicalMappings.get(node.catalogue.localSchemaVersionId)?.tables;

/** Physical ids are node-local and never cross the transaction/wire boundary. */
export function contributionTableIdForStorage(node: NodeState, table: string): number {
  return resolveUnique(
    localTables(node)?.get(table)?.tableId,
    allTables(node)
      .filter(([n]) => n === table)
      .map(([, t]) => t.tableId),
    "contribution physical table mapping missing",
    "contribution logical table maps to multiple physical ids",
  );
}

export function contributionTableNameForStorage(node: NodeState, table: number): string {
  const current =
    localTables(node) && [...localTables(node)].find(([, t]: any) => t.tableId === table)?.[0];
  return resolveUnique(
    current,
    allTables(node)
      .filter(([, t]) => t.tableId === table)
      .map(([n]) => n),
    "stored contribution physical table id is absent from the catalogue",
    "stored contribution physical table id maps to multiple logical names",
  );
}

export function contributionColumnIdForStorage(
  node: NodeState,
  table: string,
  column: string,
): number {
  return resolveUnique(
    localTables(node)?.get(table)?.columns.get(column),
    allTables(node)
      .filter(([n]) => n === table)
      .flatMap(([, t]) => (t.columns.has(column) ? [t.columns.get(column)!] : [])),
    "contribution physical column mapping missing",
    "contribution logical column maps to multiple physical ids",
  );
}

export function contributionColumnNameForStorage(
  node: NodeState,
  table: string,
  column: number,
): string {
  const byId = (t: TablePhysicalMapping) =>
    [...t.columns].filter(([, id]) => id === column).map(([n]) => n);
  const local = localTables(node)?.get(table);
  return resolveUnique(
    local ? byId(local)[0] : undefined,
    allTables(node)
      .filter(([n]) => n === table)
      .flatMap(([, t]) => byId(t)),
    "stored contribution physical column id is absent from its table mapping",
    "stored contribution physical column id maps to multiple logical names",
  );
}

export const contributionMergeStorageValue = (
  node: NodeState,
  provenance: any | undefined,
): Value =>
  encodeContributionMerge(
    provenance,
    (t) => contributionTableIdForStorage(node, t),
    (t, c) => contributionColumnIdForStorage(node, t, c),
  );

export const contributionMergeFromStorageRecord = (node: NodeState, record: any): any =>
  decodeContributionMerge(
    record,
    (t) => contributionTableNameForStorage(node, t),
    (t, c) => contributionColumnNameForStorage(node, t, c),
  );

export function authoredColumnsForVersion(
  node: NodeState,
  version: VersionRow,
): SortedSet<string> | undefined {
  const schemaVersion = node.schemaVersionForAlias(version.schemaVersionAlias());
  if (schemaVersion === undefined) throw invalid("authored columns schema version alias missing");
  return authoredColumnNamesForIds(node, schemaVersion, version.table, version.authoredColumnIds());
}

export function preparedPhysicalWritePlan(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  target: PhysicalWriteTarget,
): PreparedPhysicalWritePlan {
  const cache: SortedMap<
    Uuid,
    Map<string, Map<PhysicalWriteTarget, PreparedPhysicalWritePlan>>
  > = node.catalogue.physicalWritePlanCache;
  const cached = cache.get(schemaVersion)?.get(logicalTable)?.get(target);
  if (cached) return cached;

  const sourceTable = node.tableInSchema(logicalTable, schemaVersion);
  const sourceMapping = tableMapping(
    node,
    schemaVersion,
    logicalTable,
    "physical write table mapping missing",
  );
  const mappings = node.catalogue.physicalMappings;
  const storageTable =
    target === "History"
      ? physicalHistoryStorageTable(mappings, schemaVersion, logicalTable)
      : physicalCurrentStorageTable(
          mappings,
          schemaVersion,
          logicalTable,
          target === "GlobalCurrent" ? "Global" : "Ahead",
        );
  const physicalTable = node.database.tableSchema(storageTable);
  const logicalDescriptor = (
    target === "History"
      ? sourceTable.historyStorageTable()
      : target === "GlobalCurrent"
        ? sourceTable.globalCurrentStorageTable()
        : sourceTable.aheadCurrentStorageTable()
  ).recordSchema();
  const physicalNames =
    target === "History"
      ? physicalHistoryFieldNames(sourceTable, sourceMapping)
      : physicalCurrentFieldNames(sourceTable, sourceMapping);
  const physicalDescriptor = physicalWriteDescriptor(
    logicalDescriptor,
    physicalNames,
    physicalTable,
  );
  const historyDescriptor = sourceTable.historyStorageTable().recordSchema();
  const enumRemaps = prepareAuthoredEnumCellsForPhysical(node, sourceTable, sourceMapping);
  const current = target !== "History";
  const userCells = current ? GlobalCurrentRowRecord.USER_CELLS : HistoryRowRecord.USER_CELLS;
  const historyFields = historyDescriptor.fields();
  const writeFields: PhysicalWriteField[] = physicalDescriptor
    .fields()
    .map((field: any, index: number): PhysicalWriteField => {
      if (!current && index === HistoryRowRecord.FIELD_UPDATED_BY_IDX)
        return { kind: "HistoryUpdatedBy" };
      if (current) {
        if (index === GlobalCurrentRowRecord.FIELD_CREATED_AT_IDX)
          return { kind: "CreatedAtMillis" };
        if (index === GlobalCurrentRowRecord.FIELD_UPDATED_BY_IDX) return { kind: "UpdatedBy" };
        if (index === GlobalCurrentRowRecord.FIELD_UPDATED_AT_IDX)
          return { kind: "UpdatedAtMillis" };
        if (index === GlobalCurrentRowRecord.FIELD_GLOBAL_TIME_IDX) return { kind: "GlobalTime" };
      }
      // A current row has `global_time` where history has none, and ends at `authored_columns`.
      const source =
        current && index > GlobalCurrentRowRecord.FIELD_GLOBAL_TIME_IDX ? index - 1 : index;
      const remaps = index >= userCells ? enumRemaps[index - userCells] : undefined;
      if (remaps && (remaps.scalar.size || remaps.payload.size))
        return { kind: "Enum", source, column: index - userCells };
      return valueTypeEquals(historyFields[source].valueType, field.valueType)
        ? { kind: "Copy", source }
        : { kind: "Decode", source };
    });
  const plan: PreparedPhysicalWritePlan = {
    storageTable,
    sourceTable,
    logicalDescriptor,
    physicalDescriptor,
    historyDescriptor,
    writeFields,
    enumRemaps,
  };
  let byTable = cache.get(schemaVersion);
  if (!byTable) cache.set(schemaVersion, (byTable = new Map()));
  let byTarget = byTable.get(logicalTable);
  if (!byTarget) byTable.set(logicalTable, (byTarget = new Map()));
  byTarget.set(target, plan);
  return plan;
}

export function versionStorageTableForRow(node: NodeState, version: VersionRow): string {
  const schemaVersion = node.schemaVersionForAlias(version.schemaVersionAlias());
  if (schemaVersion === undefined)
    throw invalid("stored row schema version alias missing while resolving storage table");
  return physicalHistoryStorageTable(node.catalogue.physicalMappings, schemaVersion, version.table);
}

export const versionStoragePrimaryKey = (_node: NodeState, version: VersionRow) =>
  historyPrimaryKey(version);

/** Authored tags of each case identity, positioned in the lineage-wide physical registry. */
function authoredToPhysicalTags(
  authored: { id: Uuid }[],
  physical: { id: Uuid }[],
  max: number,
  exhausted: string,
): (number | undefined)[] {
  return authored.map((identity) => {
    const tag = physical.findIndex((c) => equalBytes(c.id, identity.id));
    if (tag < 0) return undefined;
    if (tag > max) throw invalid(exhausted);
    return tag;
  });
}

/** Prepare authored-to-physical enum identities once per catalogue write plan. */
function prepareAuthoredEnumCellsForPhysical(
  node: NodeState,
  sourceTable: any,
  mapping: TablePhysicalMapping,
): EnumOccurrenceRemaps[] {
  return sourceTable.columns.map((column: any) => {
    const columnId = mapping.columns.get(column.name);
    if (columnId === undefined) throw invalid("physical enum write column mapping missing");
    const remaps = RecursiveEnumRemaps.default_();
    const tableId = mapping.tableId;
    const scalar = mapping.scalarEnumCases.get(columnId);
    if (scalar)
      remaps.scalar.set(
        "root",
        authoredToPhysicalTags(
          scalar,
          physicalScalarEnumCases(node, tableId, columnId),
          0xff,
          "physical scalar enum tag exhausted",
        ),
      );
    const payload = mapping.payloadEnumCases.get(columnId);
    if (payload) {
      remaps.payload.set(
        "root",
        authoredToPhysicalTags(
          payload,
          physicalPayloadEnumCases(node, tableId, columnId),
          0xffffffff,
          "physical payload enum tag exhausted",
        ),
      );
      remaps.payloadChildren.set(
        "root",
        payload.map((identity) => globalCasePath("root", identity)),
      );
    }
    for (const [path, cases] of mapping.nestedScalarEnumCases.get(columnId) ?? [])
      remaps.scalar.set(
        path,
        authoredToPhysicalTags(
          cases,
          physicalNestedScalarEnumCases(node, tableId, columnId, path),
          0xff,
          "physical nested scalar enum tag exhausted",
        ),
      );
    for (const [path, cases] of mapping.nestedPayloadEnumCases.get(columnId) ?? []) {
      remaps.payload.set(
        path,
        authoredToPhysicalTags(
          cases,
          physicalNestedPayloadEnumCases(node, tableId, columnId, path),
          0xffffffff,
          "physical nested payload enum tag exhausted",
        ),
      );
      remaps.payloadChildren.set(
        path,
        cases.map((identity) => globalCasePath(path, identity)),
      );
    }
    return remaps;
  });
}

/**
 * Encode `version` for its history table. `txAuthor` is the transaction's `made_by`: the image
 * stores `updated_by` only when it differs.
 */
export function versionStorageWriteBinding(
  node: NodeState,
  version: VersionRow,
  txAuthor: any,
): [string, any] {
  const schemaVersion = node.schemaVersionForAlias(version.schemaVersionAlias());
  if (schemaVersion === undefined)
    throw invalid("stored row schema version alias missing while preparing storage write");
  const plan = preparedPhysicalWritePlan(node, schemaVersion, version.table, "History");
  // Every history write goes through this binding; list the row in its transaction record.
  node.markTxTouchedRow(physicalTableIdForSchema(node, schemaVersion, version.table), version);
  return [plan.storageTable, encodePhysicalVersionRecord(plan, version, undefined, txAuthor)];
}

/** Reuse encoded history fields; only current timestamps and enum tags change representation. */
export function encodePhysicalVersionRecord(
  plan: PreparedPhysicalWritePlan,
  version: VersionRow,
  globalTime: bigint | undefined,
  txAuthor: any | undefined,
): any {
  const input = version.record;
  const historyUpdatedBy = (): Value => {
    if (txAuthor === undefined)
      throw invalid("a history image is written with its transaction author");
    const updatedBy = version.updatedBy();
    return valueEquals(rowAuthorValue(updatedBy), rowAuthorValue(txAuthor))
      ? V.nullable()
      : historyUpdatedByValue(updatedBy);
  };
  const matchingLayout = input.descriptor === plan.historyDescriptor;
  return ValidatedVariantRecord.createWithEncodedFields(
    grooveVariantTag(version.schemaVersionAlias()),
    plan.physicalDescriptor,
    input.raw.length + 16,
    (index, output) => {
      const field = plan.writeFields[index];
      let value: Value;
      switch (field.kind) {
        case "Copy":
          if (matchingLayout) {
            const [s, e] = input.descriptor.fieldSpan(input.raw, field.source);
            output.push(...input.raw.subarray(s, e));
            return;
          }
          value = input.getIdx(field.source);
          break;
        case "Decode":
          value = input.getIdx(field.source);
          break;
        case "CreatedAtMillis":
          value = V.u64(txTimePhysicalMs(version.createdAt()));
          break;
        case "UpdatedAtMillis":
          value = V.u64(txTimePhysicalMs(version.updatedAt()));
          break;
        case "UpdatedBy":
          value = rowAuthorValue(version.updatedBy());
          break;
        case "HistoryUpdatedBy":
          value = historyUpdatedBy();
          break;
        case "GlobalTime":
          value = V.nullable(globalTime === undefined ? undefined : V.u64(globalTime));
          break;
        case "Enum": {
          value = input.getIdx(field.source);
          const physical = plan.physicalDescriptor.fields()[index].valueType;
          if (
            value.kind === "Nullable" &&
            value.value !== undefined &&
            physical.kind === "Nullable"
          )
            value = V.nullable(
              remapNestedEnumValue(
                value.value,
                plan.sourceTable.columns[field.column].columnType,
                physical.inner,
                plan.enumRemaps[field.column],
                "root",
              ),
            );
        }
      }
      plan.physicalDescriptor.encodeFieldInto(index, value, output);
    },
  );
}

export function rejectedVersionStorageWriteBinding(
  node: NodeState,
  version: VersionRow,
  logicalRecord: any,
): [string, any] {
  const schemaVersion = node.schemaVersionForAlias(version.schemaVersionAlias());
  if (schemaVersion === undefined) throw invalid("rejected row schema version alias missing");
  const binding = physicalRejectedVersionBinding(
    node.catalogue.catalogueSchemas,
    node.catalogue.physicalMappings,
    schemaVersion,
    version.table,
  );
  const record = OwnedRecord.new_(logicalRecord.raw.slice(), binding.descriptor);
  return [
    binding.storageTable,
    VariantRecord.new_(grooveVariantTag(version.schemaVersionAlias()), record),
  ];
}
