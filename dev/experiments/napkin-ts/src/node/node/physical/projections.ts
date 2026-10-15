// Groove graph sources and variant projections over the physical version tables.
import {
  SortedMap,
  SortedSet,
  cmpNum,
  cmpStr,
  cmpBytes,
  cmpTuple,
} from "../../../support/collections.js";
import {
  nodeError,
  isGrooveDbError,
  GraphBuilder,
  ProjectField,
  BranchKey,
  V,
  VT,
  literalValue,
  userColumnField,
  type NodeState,
  type Uuid,
  type Value,
  type ValueType,
  type StaticScanSpec,
  type EnumOccurrenceRemaps,
} from "../../../_extern/node-physical-ingest.js";
import { HistoryRowRecord, GlobalCurrentRowRecord, type VersionRow } from "../codec.js";
import {
  type SchemaPhysicalMapping,
  type TablePhysicalMapping,
  type CurrentWinnerCellProjection,
  grooveVariantTag,
  physicalMappingHasEnumBoundary,
  valueTypeHasEnumBoundary,
} from "./catalogue.js";
import {
  physicalHistoryTableName,
  physicalGlobalCurrentTableName,
  physicalAheadCurrentTableName,
  splitPhysicalTableName,
  physicalUserColumnField,
  physicalHistoryProjectionTarget,
  physicalCurrentProjectionTarget,
  physicalCurrentWinnerProjectionTarget,
  physicalCurrentProjectionTargetForEnumColumns,
  authoredHistoryProjectionDescriptor,
  type PhysicalCurrentClass,
  type ContentProjectionShape,
} from "./names.js";
import { physicalCurrentSourceTable, physicalHistorySourceTable } from "./bindings.js";
import {
  widenedProjectionDescriptor,
  physicalWriteDescriptor,
  physicalVersionStorageTables,
  physicalCurrentDescriptor,
  physicalHistoryFieldNames,
  physicalCurrentFieldNames,
  physicalCurrentFieldNamesForCase,
} from "./descriptors.js";
import { physicalToAuthoredEnumRemaps, physicalCopyEnumRemaps } from "./enum_lookup.js";

type Mappings = SortedMap<Uuid, SchemaPhysicalMapping>;
type Source = [Uuid, string, TablePhysicalMapping];
type CaseSpec = [number, SortedSet<string> | undefined];

const invalid = (msg: string) => nodeError("InvalidStoredValue", msg);

function tableMapping(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  missing: string,
): TablePhysicalMapping {
  const mapping = (node.catalogue.physicalMappings as Mappings)
    .get(schemaVersion)
    ?.tables.get(logicalTable);
  if (!mapping) throw invalid(missing);
  return mapping;
}

function schemaAlias(node: NodeState, schemaVersion: Uuid, missing: string): number {
  const alias = node.catalogue.schemaVersionAliases.get(schemaVersion);
  if (alias === undefined) throw invalid(missing);
  return alias;
}

function namedFields(descriptor: any): string[] {
  return descriptor.fields().map((field: any) => {
    if (field.name == null) throw invalid("physical current winner field unnamed");
    return field.name as string;
  });
}

/** Every (schema, logical table, mapping) sharing one physical table id. */
function sourcesFor(node: NodeState, tableId: number): Source[] {
  const out: Source[] = [];
  for (const [schemaVersion, mapping] of node.catalogue.physicalMappings as Mappings)
    for (const [logicalTable, table] of mapping.tables)
      if (table.tableId === tableId) out.push([schemaVersion, logicalTable, table]);
  return out;
}

function allTargets(node: NodeState): Source[] {
  const out: Source[] = [];
  for (const [schemaVersion, mapping] of node.catalogue.physicalMappings as Mappings)
    for (const [logicalTable, table] of mapping.tables)
      out.push([schemaVersion, logicalTable, table]);
  return out;
}

function variantCases(alias: number, mapping: TablePhysicalMapping): CaseSpec[] {
  return mapping.variantCases.length === 0
    ? [[grooveVariantTag(alias), undefined]]
    : mapping.variantCases.map((c) => [c.tag, c.fields] as CaseSpec);
}

export function physicalTableIdForSchema(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
): number {
  node.tableInSchemaRef(logicalTable, schemaVersion);
  const mapping = (node.catalogue.physicalMappings as Mappings)
    .get(schemaVersion)
    ?.tables.get(logicalTable);
  if (!mapping) throw invalid("physical table mapping missing");
  return mapping.tableId;
}

/** Resolve storage publication names to logical tables across every known schema mapping. */
export function logicalTableNamesForStorageTables(
  node: NodeState,
  changedTables: Set<string>,
): Set<string> {
  const changedIds = new Set<number>();
  for (const name of changedTables) {
    const id = physicalTableIdForPublicationName(name);
    if (id !== undefined) changedIds.add(id);
  }
  const out = new Set<string>();
  for (const mapping of (node.catalogue.physicalMappings as Mappings).valuesArray())
    for (const [logicalTable, table] of mapping.tables)
      if (changedIds.has(table.tableId)) out.add(logicalTable);
  return out;
}

export function physicalTableIdForVersion(node: NodeState, version: VersionRow): number {
  const schemaVersion = node.schemaVersionForAlias(version.schemaVersionAlias());
  if (!schemaVersion)
    throw invalid("stored row schema version alias missing while resolving physical table");
  return physicalTableIdForSchema(node, schemaVersion, version.table);
}

export function physicalCurrentTableForSchema(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
): string {
  const tableId = physicalTableIdForSchema(node, schemaVersion, logicalTable);
  return cls === "Global"
    ? physicalGlobalCurrentTableName(tableId)
    : physicalAheadCurrentTableName(tableId);
}

const currentSource = (
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
) =>
  physicalCurrentSourceTable(
    node.catalogue.catalogueSchemas,
    node.catalogue.physicalMappings,
    schemaVersion,
    logicalTable,
    cls,
  );

export function physicalCurrentSourceGraph(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
): GraphBuilder {
  const alias = schemaAlias(node, schemaVersion, "physical current source schema alias missing");
  return GraphBuilder.variantSourceScan(
    currentSource(node, schemaVersion, logicalTable, cls),
    physicalCurrentProjectionTarget(alias, logicalTable),
    sharedBranchScan(undefined),
  );
}

/**
 * Read raw row images of one branch through the physical winner projection. Deletion markers need only system
 * fields, so they must not pass through the read schema's enum lens.
 */
export function physicalCurrentMarkerSourceGraph(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
  branchKey: any,
  scan?: StaticScanSpec,
): GraphBuilder {
  const mapping = tableMapping(
    node,
    schemaVersion,
    logicalTable,
    "physical current marker mapping missing",
  );
  const table = node.tableInSchemaRef(logicalTable, schemaVersion);
  const physicalFields = namedFields(physicalCurrentDescriptor(table, mapping));
  return GraphBuilder.variantSourceScan(
    currentSource(node, schemaVersion, logicalTable, cls),
    physicalCurrentWinnerProjectionTarget(mapping.tableId, physicalFields),
    branchScan(branchKey, scan),
  );
}

export function physicalGlobalMarkerIndexGraph(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  index: string,
  scan: StaticScanSpec,
): GraphBuilder {
  const [table, target] = physicalGlobalMarkerTarget(node, schemaVersion, logicalTable);
  return GraphBuilder.variantIndexScan(table, index, target, scan);
}

export function physicalGlobalMarkerPageGraph(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  scan: StaticScanSpec,
): GraphBuilder {
  const [table, target] = physicalGlobalMarkerTarget(node, schemaVersion, logicalTable);
  return GraphBuilder.variantSourceScan(table, target, scan);
}

function physicalGlobalMarkerTarget(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
): [string, string] {
  const mapping = tableMapping(
    node,
    schemaVersion,
    logicalTable,
    "physical current marker mapping missing",
  );
  const table = node.tableInSchemaRef(logicalTable, schemaVersion);
  const physicalFields = namedFields(physicalCurrentDescriptor(table, mapping));
  return [
    physicalGlobalCurrentTableName(mapping.tableId),
    physicalCurrentWinnerProjectionTarget(mapping.tableId, physicalFields),
  ];
}

export function physicalCurrentSourceGraphWithProjectionTarget(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
  projectionTarget: string,
): GraphBuilder {
  return GraphBuilder.variantSourceScan(
    currentSource(node, schemaVersion, logicalTable, cls),
    projectionTarget,
    sharedBranchScan(undefined),
  );
}

export function physicalCurrentBranchSourceGraphWithProjectionTarget(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
  projectionTarget: string,
  branchKey: any,
  scan?: StaticScanSpec,
): GraphBuilder {
  return GraphBuilder.variantSourceScan(
    currentSource(node, schemaVersion, logicalTable, cls),
    projectionTarget,
    branchScan(branchKey, scan),
  );
}

export function physicalCurrentSourceScanGraph(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
  scan: StaticScanSpec,
): GraphBuilder {
  const alias = schemaAlias(node, schemaVersion, "physical current source schema alias missing");
  return GraphBuilder.variantSourceScan(
    currentSource(node, schemaVersion, logicalTable, cls),
    physicalCurrentProjectionTarget(alias, logicalTable),
    sharedBranchScan(scan),
  );
}

export function physicalCurrentSourceScanGraphWithProjectionTarget(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
  projectionTarget: string,
  scan: StaticScanSpec,
): GraphBuilder {
  return GraphBuilder.variantSourceScan(
    currentSource(node, schemaVersion, logicalTable, cls),
    projectionTarget,
    sharedBranchScan(scan),
  );
}

export function logicalTableForPhysicalAlias(
  node: NodeState,
  tableId: number,
  alias: number,
): string {
  const schemaVersion = node.schemaVersionForAlias(alias);
  if (!schemaVersion) throw invalid("physical row schema version alias missing");
  const mapping = (node.catalogue.physicalMappings as Mappings).get(schemaVersion);
  for (const [logicalTable, table] of mapping?.tables ?? [])
    if (table.tableId === tableId) return logicalTable;
  throw invalid("physical row logical table mapping missing");
}

export function physicalTableIds(node: NodeState): SortedSet<number> {
  const out = new SortedSet<number>(cmpNum);
  for (const mapping of (node.catalogue.physicalMappings as Mappings).valuesArray())
    for (const table of mapping.tables.valuesArray()) out.add(table.tableId);
  return out;
}

export function physicalHistorySourceGraph(
  node: NodeState,
  schemaVersion: Uuid,
  logicalTable: string,
): GraphBuilder {
  const alias = schemaAlias(node, schemaVersion, "physical history source schema alias missing");
  const storageTable = physicalHistorySourceTable(
    node.catalogue.catalogueSchemas,
    node.catalogue.schemaVersionAliases,
    node.catalogue.physicalMappings,
    schemaVersion,
    logicalTable,
  );
  return GraphBuilder.variantSource(
    storageTable,
    physicalHistoryProjectionTarget(alias, logicalTable),
  );
}

export async function registerPhysicalHistoryVariantProjections(node: NodeState): Promise<void> {
  for (const [targetSchema, targetTableName, targetMapping] of allTargets(node)) {
    const targetAlias = schemaAlias(
      node,
      targetSchema,
      "physical projection target schema alias missing",
    );
    const targetTable = node.tableInSchemaRef(targetTableName, targetSchema);
    const projectionTarget = physicalHistoryProjectionTarget(targetAlias, targetTableName);
    const logicalOutput = targetTable.historyStorageTable().recordSchema();
    const physicalNames = physicalHistoryFieldNames(targetTable, targetMapping);
    const sources = sourcesFor(node, targetMapping.tableId);
    const storageTable = physicalHistoryTableName(targetMapping.tableId);
    const output = widenedProjectionDescriptor(
      logicalOutput,
      physicalNames,
      node.database.tableSchema(storageTable),
    );
    node.database.defineVariantProjection(storageTable, projectionTarget, output);
    for (const [sourceSchema, sourceTableName, sourceMapping] of sources) {
      const sourceAlias = schemaAlias(
        node,
        sourceSchema,
        "physical projection source schema alias missing",
      );
      for (const [tag, present] of variantCases(sourceAlias, sourceMapping)) {
        const fields = physicalContentProjectionCase(
          node,
          sourceSchema,
          sourceTableName,
          sourceMapping,
          targetSchema,
          targetTableName,
          "History",
          present,
          undefined,
        );
        if (!fields) node.database.registerVariantIgnoreCase(storageTable, projectionTarget, tag);
        else node.database.registerVariantCase(storageTable, projectionTarget, tag, fields);
      }
    }
  }
}

export async function registerPhysicalCurrentVariantProjections(node: NodeState): Promise<void> {
  for (const [targetSchema, targetTableName, targetMapping] of allTargets(node)) {
    const targetAlias = schemaAlias(
      node,
      targetSchema,
      "physical current projection target schema alias missing",
    );
    const targetTable = node.tableInSchema(targetTableName, targetSchema);
    const projectionTarget = physicalCurrentProjectionTarget(targetAlias, targetTableName);
    const storageTables = [
      physicalGlobalCurrentTableName(targetMapping.tableId),
      physicalAheadCurrentTableName(targetMapping.tableId),
    ];
    for (const storageTable of storageTables) {
      const logicalOutput = targetTable.globalCurrentStorageTable().recordSchema();
      const physicalNames = physicalCurrentFieldNames(targetTable, targetMapping);
      const output = widenedProjectionDescriptor(
        logicalOutput,
        physicalNames,
        node.database.tableSchema(storageTable),
      );
      node.database.defineVariantProjection(storageTable, projectionTarget, output);
    }
    for (const [sourceSchema, sourceTableName, sourceMapping] of sourcesFor(
      node,
      targetMapping.tableId,
    )) {
      const sourceAlias = schemaAlias(
        node,
        sourceSchema,
        "physical current projection source schema alias missing",
      );
      for (const [tag, present] of variantCases(sourceAlias, sourceMapping)) {
        const fields = physicalContentProjectionCase(
          node,
          sourceSchema,
          sourceTableName,
          sourceMapping,
          targetSchema,
          targetTableName,
          "Current",
          present,
          undefined,
        );
        for (const storageTable of storageTables) {
          if (fields)
            node.database.registerVariantCase(storageTable, projectionTarget, tag, fields.slice());
          else node.database.registerVariantIgnoreCase(storageTable, projectionTarget, tag);
        }
      }
    }
  }
}

/**
 * Register (or refresh) a current-source projection that decodes only the enum columns required by one query
 * source; unrequested enum cells become typed nulls and never expose a physical tag.
 */
export function ensurePhysicalCurrentProjectionForEnumColumns(
  node: NodeState,
  targetSchema: Uuid,
  targetTableName: string,
  requiredFields: SortedSet<string>,
): string {
  const targetAlias = schemaAlias(
    node,
    targetSchema,
    "physical current projection target schema alias missing",
  );
  const targetTable = node.tableInSchema(targetTableName, targetSchema);
  const targetMapping = tableMapping(
    node,
    targetSchema,
    targetTableName,
    "target enum physical mapping missing",
  );
  const requiredEnumColumns = new SortedSet<number>(cmpNum);
  let targetHasAnyEnumBoundary = false;
  for (const column of targetTable.columns) {
    const columnId = targetMapping.columns.get(column.name);
    const boundary =
      (columnId !== undefined && physicalMappingHasEnumBoundary(targetMapping, columnId)) ||
      valueTypeHasEnumBoundary(column.columnType);
    if (boundary) targetHasAnyEnumBoundary = true;
    if (requiredFields.has(column.name) && columnId !== undefined && boundary)
      requiredEnumColumns.add(columnId);
  }
  // An enum-bearing table with an empty requirement set is still a query-local compatibility boundary.
  if (requiredEnumColumns.size === 0 && !targetHasAnyEnumBoundary)
    return physicalCurrentProjectionTarget(targetAlias, targetTableName);
  const projectionTarget = physicalCurrentProjectionTargetForEnumColumns(
    targetAlias,
    targetTableName,
    requiredEnumColumns.toArray(),
  );
  const storageTables = [
    physicalGlobalCurrentTableName(targetMapping.tableId),
    physicalAheadCurrentTableName(targetMapping.tableId),
  ];
  for (const storageTable of storageTables)
    node.database.defineVariantProjection(
      storageTable,
      projectionTarget,
      targetTable.globalCurrentStorageTable().recordSchema(),
    );
  for (const [sourceSchema, sourceTableName, sourceMapping] of sourcesFor(
    node,
    targetMapping.tableId,
  )) {
    const sourceAlias = schemaAlias(
      node,
      sourceSchema,
      "physical current projection source schema alias missing",
    );
    for (const [tag, present] of variantCases(sourceAlias, sourceMapping)) {
      const fields = physicalContentProjectionCase(
        node,
        sourceSchema,
        sourceTableName,
        sourceMapping,
        targetSchema,
        targetTableName,
        "Current",
        present,
        requiredEnumColumns,
      );
      for (const storageTable of storageTables) {
        if (fields)
          node.database.registerVariantProjectionCaseOmittingUnrepresentableEnums(
            storageTable,
            projectionTarget,
            tag,
            fields.slice(),
          );
        else node.database.registerVariantIgnoreCase(storageTable, projectionTarget, tag);
      }
    }
  }
  return projectionTarget;
}

/**
 * Register the common physical descriptor used to choose the latest Global/Ahead version before a query-local
 * old-schema projection can omit an unrepresentable enum case.
 */
export async function ensurePhysicalCurrentWinnerProjection(
  node: NodeState,
  targetSchema: Uuid,
  targetTableName: string,
): Promise<[string, string[]]> {
  const prepared = node.catalogue.physicalCurrentWinnerProjections
    .get(targetSchema)
    ?.get(targetTableName);
  if (prepared) return prepared;
  const targetMapping = tableMapping(
    node,
    targetSchema,
    targetTableName,
    "target current winner physical mapping missing",
  );
  const storageTables = [
    physicalGlobalCurrentTableName(targetMapping.tableId),
    physicalAheadCurrentTableName(targetMapping.tableId),
  ];
  const targetTable = node.tableInSchemaRef(targetTableName, targetSchema);
  const authoredOutput = physicalCurrentDescriptor(targetTable, targetMapping);
  const physicalFields = namedFields(authoredOutput);
  // Winner selection operates on raw physical data: keep the target layout, take enum registries from storage.
  const output = physicalWriteDescriptor(
    authoredOutput,
    physicalFields,
    node.database.tableSchema(storageTables[0]),
  );
  const projectionTarget = physicalCurrentWinnerProjectionTarget(
    targetMapping.tableId,
    physicalFields,
  );
  let outputFields: string[] | undefined;
  for (const storageTable of storageTables) {
    const fields = namedFields(output);
    if (outputFields) {
      if (outputFields.length !== fields.length || outputFields.some((f, i) => f !== fields[i]))
        throw invalid("physical current winner descriptors disagree");
    } else outputFields = fields;
    node.database.defineVariantProjection(storageTable, projectionTarget, output);
  }

  for (const [sourceSchema, sourceTableName, sourceMapping] of sourcesFor(
    node,
    targetMapping.tableId,
  )) {
    const sourceAlias = schemaAlias(
      node,
      sourceSchema,
      "physical current winner source schema alias missing",
    );
    const sourceTable = node.tableInSchema(sourceTableName, sourceSchema);
    const targetColumnsByPhysicalField = new Map<string, string>();
    for (const [column, id] of targetMapping.columns)
      targetColumnsByPhysicalField.set(physicalUserColumnField(id), column);
    for (const [tag, present] of variantCases(sourceAlias, sourceMapping)) {
      const available = new SortedSet<string>(
        cmpStr,
        physicalCurrentFieldNamesForCase(sourceTable, sourceMapping, present),
      );
      for (const storageTable of storageTables) {
        const fields = output.fields().map((field: any) => {
          const name: string = field.name;
          if (name == null) throw invalid("physical current winner field unnamed");
          if (available.has(name)) return ProjectField.named(name);
          const column = targetColumnsByPhysicalField.get(name);
          if (column !== undefined) {
            // Only mapped user columns can be absent because of a lens; witness and system fields stay raw.
            return (
              lensProjectionForMissingCurrentField(
                node,
                sourceSchema,
                sourceTableName,
                sourceMapping,
                available,
                targetSchema,
                targetTableName,
                targetMapping,
                column,
                name,
                field.valueType,
              ) ?? ProjectField.literalTyped(name, V.nullable(), field.valueType)
            );
          }
          if (field.valueType.kind === "Nullable")
            return ProjectField.literalTyped(name, V.nullable(), field.valueType);
          throw invalid("physical current winner source misses required field");
        });
        node.database.refreshVariantCaseForRegistryEvolution(
          storageTable,
          projectionTarget,
          tag,
          fields,
        );
      }
    }
  }
  const result: [string, string[]] = [projectionTarget, outputFields ?? []];
  // Publish only after every source variant and both storage layers registered.
  node.catalogue.physicalCurrentWinnerProjections
    .getOrInsert(targetSchema, () => new SortedMap<string, [string, string[]]>(cmpStr))
    .set(targetTableName, result);
  return result;
}

/** Resolve a missing target user field through the migration path before the Global/Ahead arg-max. */
function lensProjectionForMissingCurrentField(
  node: NodeState,
  sourceSchema: Uuid,
  sourceTableName: string,
  sourceMapping: TablePhysicalMapping,
  available: SortedSet<string>,
  targetSchema: Uuid,
  targetTableName: string,
  targetMapping: TablePhysicalMapping,
  targetColumn: string,
  outputName: string,
  outputType: ValueType,
): ProjectField | undefined {
  const cell = lensCurrentCell(
    node,
    sourceSchema,
    sourceTableName,
    sourceMapping,
    available,
    targetSchema,
    targetTableName,
    targetColumn,
  );
  if (!cell || cell.kind === "Null") return undefined;
  if (cell.kind === "Literal") {
    // A nullable lens default is the inner logical value: wrap it so a default null differs from an absent field.
    const value = outputType.kind === "Nullable" ? V.nullable(cell.value) : cell.value;
    return ProjectField.literalTyped(outputName, value, outputType);
  }
  const targetColumnId = targetMapping.columns.get(targetColumn);
  if (targetColumnId === undefined) throw invalid("target current winner column mapping missing");
  const targetColumnType = node
    .tableInSchemaRef(targetTableName, targetSchema)
    .columns.find((c: any) => c.name === targetColumn)?.columnType;
  if (targetColumnType === undefined) throw invalid("target current winner column schema missing");
  if (
    cell.columnId === targetColumnId ||
    (!physicalMappingHasEnumBoundary(sourceMapping, cell.columnId) &&
      !physicalMappingHasEnumBoundary(targetMapping, targetColumnId) &&
      !valueTypeHasEnumBoundary(cell.columnType) &&
      !valueTypeHasEnumBoundary(targetColumnType))
  )
    return ProjectField.renamed(cell.name, outputName);
  return ProjectField.recursiveEnumRemap(
    cell.name,
    outputName,
    outputType,
    physicalCopyEnumRemaps(
      node,
      sourceMapping,
      cell.columnId,
      targetMapping,
      targetColumnId,
      cell.columnType,
      targetColumnType,
    ),
  );
}

/** Apply compiled lens ops (Rename/Copy/Add/Drop) to a column→cell map. */
function applyLensOps<T>(cells: Map<string, T>, ops: any[], literal: (v: Value) => T): void {
  for (const op of ops) {
    switch (op.kind) {
      case "Rename": {
        const value = cells.get(op.from);
        if (value !== undefined) {
          cells.delete(op.from);
          cells.set(op.to, value);
        }
        break;
      }
      case "Copy": {
        const value = cells.get(op.from);
        if (value !== undefined) cells.set(op.to, value);
        break;
      }
      case "Add":
        if (!cells.has(op.column)) cells.set(op.column, literal(op.default));
        break;
      case "Drop":
        cells.delete(op.column);
        break;
    }
  }
}

/** Where a target current-winner user column comes from in one source variant along the migration lens path. */
function lensCurrentCell(
  node: NodeState,
  sourceSchema: Uuid,
  sourceTableName: string,
  sourceMapping: TablePhysicalMapping,
  available: SortedSet<string>,
  targetSchema: Uuid,
  targetTableName: string,
  targetColumn: string,
): CurrentWinnerCellProjection | undefined {
  const sourceTable = node.tableInSchema(sourceTableName, sourceSchema);
  const cells = new Map<string, CurrentWinnerCellProjection>();
  for (const column of sourceTable.columns) {
    const columnId = sourceMapping.columns.get(column.name);
    const name = columnId === undefined ? undefined : physicalUserColumnField(columnId);
    cells.set(
      column.name,
      name !== undefined && available.has(name)
        ? { kind: "Field", name, columnId: columnId!, columnType: column.columnType }
        : { kind: "Null" },
    );
  }
  const path = node.compiledLensPath(sourceSchema, targetSchema, sourceTableName);
  if (!path) return undefined;
  if (path.targetTable !== targetTableName) return undefined;
  applyLensOps(cells, path.ops, (value) => ({ kind: "Literal" as const, value }));
  return cells.get(targetColumn);
}

/** The logical query-local projection placed after physical current winner selection. */
export function physicalCurrentPostWinnerProjectionFields(
  node: NodeState,
  targetSchema: Uuid,
  targetTableName: string,
  requiredFields: SortedSet<string>,
): ProjectField[] {
  const targetMapping = tableMapping(
    node,
    targetSchema,
    targetTableName,
    "target post-winner physical mapping missing",
  );
  const targetTable = node.tableInSchemaRef(targetTableName, targetSchema);
  const required = new SortedSet<number>(cmpNum);
  for (const column of targetTable.columns) {
    const id = requiredFields.has(column.name) ? targetMapping.columns.get(column.name) : undefined;
    if (id !== undefined) required.add(id);
  }
  const fields = physicalContentProjectionCase(
    node,
    targetSchema,
    targetTableName,
    targetMapping,
    targetSchema,
    targetTableName,
    "Current",
    undefined,
    required,
  );
  if (!fields) throw invalid("post-winner current projection unexpectedly lacks fields");
  return fields;
}

export async function synchronizePhysicalVersionTables(node: NodeState): Promise<void> {
  // The registry can evolve without changing a logical schema key: rebuild all metadata with the new registry.
  node.catalogue.physicalCurrentWinnerProjections.clear();
  // A physical schema is a coupled registry: do not leave a prefix behind if a later member fails validation.
  const checkpoint = node.database.runtimeRegistryCheckpoint();
  try {
    await synchronizePhysicalVersionTablesInner(node);
  } catch (error) {
    node.database.restoreRuntimeRegistry(checkpoint);
    node.catalogue.physicalCurrentWinnerProjections.clear();
    throw error;
  }
}

async function synchronizePhysicalVersionTablesInner(node: NodeState): Promise<void> {
  const desiredTables = physicalVersionStorageTables(
    node.catalogue.catalogueSchemas,
    node.catalogue.schemaVersionAliases,
    node.catalogue.physicalMappings,
  );
  for (const desired of desiredTables) {
    let existing: any;
    try {
      existing = node.database.tableSchema(desired.name);
    } catch (error) {
      if (!isGrooveDbError(error, "TableNotFound")) throw error;
    }
    if (!existing) {
      node.database.registerTable(desired);
      continue;
    }
    node.database.evolveTableVariantRegistries(desired.name, desired.columns);
    const addedColumns = desired.columns.filter((column: any) =>
      existing.columns.every((c: any) => c.name !== column.name),
    );
    for (const schemaVersion of desired.variants) {
      if (existing.variant(schemaVersion.tag)) continue;
      node.database.registerTableVariantWithColumns(
        desired.name,
        addedColumns.slice(),
        schemaVersion,
      );
    }
    for (const index of desired.indices) {
      if (existing.indices.some((c: any) => c.name === index.name)) continue;
      await node.database.registerTableIndex(desired.name, index);
    }
  }
  await registerPhysicalHistoryVariantProjections(node);
  await registerPhysicalCurrentVariantProjections(node);
  await registerPhysicalCurrentWinnerProjections(node);
}

/** Keep the raw Global/Ahead winner targets live as the physical enum registries evolve. */
async function registerPhysicalCurrentWinnerProjections(node: NodeState): Promise<void> {
  const targets = new SortedSet<[Uuid, string]>(cmpTuple(cmpBytes, cmpStr));
  for (const [schemaVersion, mapping] of node.catalogue.physicalMappings as Mappings)
    for (const tableName of mapping.tables.keysArray()) targets.add([schemaVersion, tableName]);
  for (const [schemaVersion, tableName] of targets.toArray())
    await ensurePhysicalCurrentWinnerProjection(node, schemaVersion, tableName);
}

type CellProjection =
  | { kind: "Field"; source: string }
  | { kind: "Missing" }
  | { kind: "Literal"; value: Value };

function bootstrapIdentityRemaps(columnType: ValueType): EnumOccurrenceRemaps {
  // At the bootstrap boundary the sole physical descriptor uses exactly the authored tag order.
  if (columnType.kind === "EnumTag")
    return {
      scalar: new Map([
        [
          "root",
          columnType.schema.variants.map((_: unknown, tag: number) =>
            tag <= 0xff ? tag : undefined,
          ),
        ],
      ]),
      payload: new Map(),
      payloadChildren: new Map(),
    };
  const cases: unknown[] = columnType.schema.cases;
  return {
    scalar: new Map(),
    payload: new Map([["root", cases.map((_, tag) => tag)]]),
    payloadChildren: new Map([["root", cases.map((_, tag) => `root/case/bootstrap/${tag}`)]]),
  };
}

function physicalContentProjectionCase(
  node: NodeState,
  sourceSchema: Uuid,
  sourceTableName: string,
  sourceMapping: TablePhysicalMapping,
  targetSchema: Uuid,
  targetTableName: string,
  shape: ContentProjectionShape,
  present: SortedSet<string> | undefined,
  requiredEnumColumns: SortedSet<number> | undefined,
): ProjectField[] | undefined {
  const sourceTable = node.tableInSchema(sourceTableName, sourceSchema);
  const targetTable = node.tableInSchema(targetTableName, targetSchema);
  const cells = new Map<string, CellProjection>();
  for (const column of sourceTable.columns) {
    if (present && !present.has(column.name)) continue;
    const columnId = sourceMapping.columns.get(column.name);
    if (columnId === undefined) throw invalid("physical projection column mapping missing");
    cells.set(column.name, { kind: "Field", source: physicalUserColumnField(columnId) });
  }
  if (cmpBytes(sourceSchema, targetSchema) !== 0 || sourceTableName !== targetTableName) {
    const path = node.compiledLensPath(sourceSchema, targetSchema, sourceTableName);
    if (!path || path.targetTable !== targetTableName) return undefined;
    applyLensOps(cells, path.ops, (value) => ({ kind: "Literal" as const, value }));
  }
  const history = shape === "History";
  const targetStorage = history
    ? targetTable.historyStorageTable()
    : targetTable.globalCurrentStorageTable();
  const targetRecord = targetStorage.recordSchema();
  const userCells = history ? HistoryRowRecord.USER_CELLS : GlobalCurrentRowRecord.USER_CELLS;
  const targetMapping = (node.catalogue.physicalMappings as Mappings)
    .get(targetSchema)
    ?.tables.get(targetTableName);
  if (!targetMapping) throw invalid("target enum physical mapping missing");
  const physicalNames = history
    ? physicalHistoryFieldNames(targetTable, targetMapping)
    : physicalCurrentFieldNames(targetTable, targetMapping);
  const physicalStorage = history
    ? physicalHistoryTableName(targetMapping.tableId)
    : physicalGlobalCurrentTableName(targetMapping.tableId);
  // Query-local enum targets are authored-descriptor boundaries.
  const projectionOutput = requiredEnumColumns
    ? history
      ? authoredHistoryProjectionDescriptor(targetTable)
      : targetRecord
    : widenedProjectionDescriptor(
        targetRecord,
        physicalNames,
        node.database.tableSchema(physicalStorage),
      );
  const recordFields: any[] = targetRecord.fields();
  const fields: ProjectField[] = recordFields
    .slice(0, userCells)
    .map((field) => ProjectField.named(field.name));
  for (const column of targetTable.columns) {
    const output = userColumnField(column.name);
    let projection = cells.get(column.name);
    cells.delete(column.name);
    if (!projection) {
      if (!present) return undefined;
      projection = { kind: "Missing" };
    }
    const nullableColumn = VT.nullable(column.columnType);
    switch (projection.kind) {
      case "Field": {
        const columnId = targetMapping.columns.get(column.name);
        if (columnId === undefined) throw invalid("target enum physical column mapping missing");
        const hasEnumBoundary = physicalMappingHasEnumBoundary(targetMapping, columnId);
        const directEnum =
          column.columnType.kind === "EnumTag" || column.columnType.kind === "Enum";
        if (!hasEnumBoundary && !directEnum) {
          fields.push(ProjectField.renamed(projection.source, output));
          break;
        }
        if (requiredEnumColumns && !requiredEnumColumns.has(columnId)) {
          // Do not decode an unknown physical case merely to populate an unused logical field.
          fields.push(ProjectField.literalTyped(output, V.nullable(), nullableColumn));
          break;
        }
        const remaps = hasEnumBoundary
          ? physicalToAuthoredEnumRemaps(node, targetMapping, columnId)
          : bootstrapIdentityRemaps(column.columnType);
        const index = projectionOutput.fieldIndex(userColumnField(column.name));
        const targetField = index === undefined ? undefined : projectionOutput.fields()[index];
        if (!targetField) throw invalid("target enum projection output field missing");
        fields.push(
          requiredEnumColumns
            ? ProjectField.recursiveEnumRemapOmittingUnrepresentable(
                projection.source,
                output,
                targetField.valueType,
                remaps,
              )
            : ProjectField.recursiveEnumRemap(
                projection.source,
                output,
                targetField.valueType,
                remaps,
              ),
        );
        break;
      }
      case "Missing":
        fields.push(ProjectField.literalTyped(output, V.nullable(), nullableColumn));
        break;
      case "Literal":
        fields.push(
          ProjectField.literalTyped(output, V.nullable(projection.value), nullableColumn),
        );
        break;
    }
  }
  for (const field of recordFields.slice(userCells + targetTable.columns.length))
    fields.push(ProjectField.named(field.name));
  return fields;
}

export const sharedBranchScan = (scan: StaticScanSpec | undefined): StaticScanSpec =>
  branchScan(BranchKey.default_(), scan);

export function branchScan(branchKey: any, scan: StaticScanSpec | undefined): StaticScanSpec {
  const branch = literalValue(V.bytes(branchKey.canonicalBytes()));
  const prepend = (values: any[]) => [branch, ...values];
  if (!scan) return { kind: "Prefix", values: [branch] };
  switch (scan.kind) {
    case "Point":
      return { kind: "Point", values: prepend(scan.values) };
    case "Prefix":
      return { kind: "Prefix", values: prepend(scan.values) };
    case "PrefixLimit":
      return { kind: "PrefixLimit", prefix: prepend(scan.prefix), maxItems: scan.maxItems };
    case "ReversePrefixLimit":
      return { kind: "ReversePrefixLimit", prefix: prepend(scan.prefix), maxItems: scan.maxItems };
    case "Range":
      return { kind: "Range", start: prepend(scan.start), end: prepend(scan.end) };
  }
}

const PUBLICATION_SUFFIXES = ["history", "global_current", "ahead_current", "rejected_versions"];

/** The table id of a per-table physical publication name; the inverse of the `physical*TableName` spellings. */
function physicalTableIdForPublicationName(name: string): number | undefined {
  const split = splitPhysicalTableName(name);
  return split && PUBLICATION_SUFFIXES.includes(split[1]) ? split[0] : undefined;
}
