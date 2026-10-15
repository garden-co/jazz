// Physical mapping allocation, validation and storage-table binding.
import { equalBytes } from "../../../support/bytes.js";
import { SortedMap, SortedSet, cmpNum, cmpStr } from "../../../support/collections.js";
import {
  nodeError,
  validateIdentitiesForSchema,
  type Uuid,
} from "../../../_extern/node-physical-ingest.js";
import {
  type SchemaPhysicalMapping,
  type TablePhysicalMapping,
  type PhysicalVariantCase,
  type PhysicalHistoryBinding,
  type GlobalScalarEnumCaseId,
  type GlobalEnumCaseId,
  grooveVariantTag,
  compareEnumCases,
  newCaseSet,
} from "./catalogue.js";
import {
  physicalHistoryTableName,
  physicalGlobalCurrentTableName,
  physicalAheadCurrentTableName,
  physicalRejectedVersionsTableName,
  type PhysicalCurrentClass,
} from "./names.js";
import { physicalRejectedVersionDescriptor } from "./descriptors.js";

const invalid = (m: string) => nodeError("InvalidStoredValue", m);
type Mappings = SortedMap<Uuid, SchemaPhysicalMapping>;
type Aliases = SortedMap<Uuid, number>;

export const emptyTableMapping = (
  tableId: number,
  columns: SortedMap<string, number>,
): TablePhysicalMapping => ({
  tableId,
  columns,
  variantCases: [],
  scalarEnumCases: new SortedMap(cmpNum),
  payloadEnumCases: new SortedMap(cmpNum),
  nestedScalarEnumCases: new SortedMap(cmpNum),
  nestedPayloadEnumCases: new SortedMap(cmpNum),
});

export function allocateProvisionalPhysicalMapping(
  schema: any,
  identities: any,
  next: { tableId: number; columnId: number },
): SchemaPhysicalMapping {
  const error = validateIdentitiesForSchema(identities, schema);
  if (error !== undefined) throw nodeError("InvalidCatalogueUpdate", error);
  const tables = new SortedMap<string, TablePhysicalMapping>(cmpStr);
  for (const table of schema.tables) {
    const tableId = next.tableId++;
    if (!Number.isSafeInteger(next.tableId)) throw invalid("physical table id exhausted");
    const columns = new SortedMap<string, number>(cmpStr);
    for (const column of table.columns) {
      columns.set(column.name, next.columnId++);
      if (!Number.isSafeInteger(next.columnId)) throw invalid("physical column id exhausted");
    }
    tables.set(table.name, emptyTableMapping(tableId, columns));
  }
  return { identities, tables };
}

/** Allocate and retain the single hidden Groove row case for one Jazz layout. */
export function allocatePhysicalVariantCases(
  mappings: Mappings,
  aliases: Aliases,
  schemaVersion: Uuid,
  logicalTable: string,
  fields: SortedSet<string>,
): PhysicalVariantCase[] {
  const target = mappings.get(schemaVersion)?.tables.get(logicalTable);
  if (!target) throw invalid("variant-case target physical mapping missing");
  if ([...fields].some((f) => !target.columns.has(f)))
    throw invalid("physical table variant contains an unknown field");
  const existing = target.variantCases[0];
  if (existing) {
    if (
      target.variantCases.length !== 1 ||
      existing.fields.toArray().join("\0") !== fields.toArray().join("\0")
    )
      throw invalid("physical table variant case definition changed");
    return target.variantCases;
  }
  const used = new Set<number>();
  const collision = () => invalid("physical table variant tag collision");
  for (const [candidateSchema, mapping] of mappings) {
    const table = mapping.tables.valuesArray().find((t) => t.tableId === target.tableId);
    if (!table) continue;
    if (table.variantCases.length === 0) {
      // The target mapping is still provisional: its alias was never written as a row tag.
      if (equalBytes(candidateSchema, schemaVersion)) continue;
      const alias = aliases.get(candidateSchema);
      if (alias === undefined) throw invalid("variant-case schema alias missing");
      const tag = grooveVariantTag(alias);
      if (used.has(tag)) throw collision();
      used.add(tag);
    } else {
      if (table.variantCases.length !== 1 || used.has(table.variantCases[0].tag)) throw collision();
      used.add(table.variantCases[0].tag);
    }
  }
  const alias = aliases.get(schemaVersion);
  if (alias === undefined) throw invalid("variant-case schema alias missing");
  const tag = grooveVariantTag(alias);
  if (used.has(tag)) throw collision();
  target.variantCases = [{ tag, fields }];
  return target.variantCases;
}

export function validatePhysicalVariantCases(mappings: Mappings, aliases: Aliases): void {
  const byTable = new Map<number, Set<number>>();
  for (const [schemaVersion, mapping] of mappings) {
    for (const table of mapping.tables.valuesArray()) {
      if (new Set(table.columns.valuesArray()).size !== table.columns.size)
        throw invalid("physical table maps multiple columns to one id");
      let tag: number;
      if (table.variantCases.length === 0) {
        const alias = aliases.get(schemaVersion);
        if (alias === undefined) throw invalid("variant-case schema alias missing");
        tag = grooveVariantTag(alias);
      } else {
        if (table.variantCases.length !== 1)
          throw invalid("physical table layout has multiple row cases");
        tag = table.variantCases[0].tag;
      }
      let tags = byTable.get(table.tableId);
      if (!tags) byTable.set(table.tableId, (tags = new Set()));
      if (tags.has(tag)) throw invalid("physical table variant tag collision");
      tags.add(tag);
    }
  }
}

/** Cross-record references and semantic registry order of recovered mappings. */
export function validatePhysicalMappingRegistries(mappings: Mappings, aliases: Aliases): void {
  for (const mapping of mappings.valuesArray()) {
    const tableIds = new Set<number>();
    for (const table of mapping.tables.valuesArray()) {
      if (tableIds.has(table.tableId))
        throw invalid("physical mapping aliases multiple tables to one id");
      tableIds.add(table.tableId);
      const columns = new Set(table.columns.valuesArray());
      if (columns.size !== table.columns.size)
        throw invalid("physical table maps multiple columns to one id");
      for (const c of table.variantCases)
        for (const field of c.fields)
          if (!table.columns.has(field))
            throw invalid("physical table variant contains an unknown field");
      const check = (column: number) => {
        if (!columns.has(column))
          throw invalid("physical enum registry references an unknown column");
      };
      for (const [column, cases] of table.scalarEnumCases)
        (check(column), validateEnumCases(cases, aliases));
      for (const [column, cases] of table.payloadEnumCases)
        (check(column), validateEnumCases(cases, aliases));
      for (const [column, paths] of table.nestedScalarEnumCases) {
        check(column);
        for (const cases of paths.valuesArray()) validateEnumCases(cases, aliases);
      }
      for (const [column, paths] of table.nestedPayloadEnumCases) {
        check(column);
        for (const cases of paths.valuesArray()) validateEnumCases(cases, aliases);
      }
    }
  }
}

function validateEnumCases(
  cases: (GlobalScalarEnumCaseId | GlobalEnumCaseId)[],
  aliases: Aliases,
): void {
  const seen = newCaseSet();
  for (const c of cases) {
    if (c.id.every((b) => b === 0))
      throw invalid("physical enum registry contains a nil global identity");
    if (!seen.add(c)) throw invalid("physical enum registry repeats a case identity");
  }
  for (let i = 1; i < cases.length; i++)
    if (compareEnumCases(aliases, cases[i - 1], cases[i]) > 0)
      throw invalid("physical enum registry has non-canonical case order");
}

/**
 * A case's authored coordinates must name the authority manifest that minted the UUID,
 * so corrupt metadata cannot forge an introduction position.
 */
export function validatePayloadEnumCaseProvenance(mappings: Mappings, aliases: Aliases): void {
  validateCaseProvenance(mappings, aliases, "payload", (t) => [
    ...t.payloadEnumCases,
    ...[...t.nestedPayloadEnumCases].flatMap(([c, paths]) =>
      paths.valuesArray().map((cases): [number, GlobalEnumCaseId[]] => [c, cases]),
    ),
  ]);
}

export function validateScalarEnumCaseProvenance(mappings: Mappings, aliases: Aliases): void {
  validateCaseProvenance(mappings, aliases, "scalar", (t) => [
    ...t.scalarEnumCases,
    ...[...t.nestedScalarEnumCases].flatMap(([c, paths]) =>
      paths.valuesArray().map((cases): [number, GlobalScalarEnumCaseId[]] => [c, cases]),
    ),
  ]);
}

function validateCaseProvenance(
  mappings: Mappings,
  aliases: Aliases,
  kind: "scalar" | "payload",
  registries: (
    t: TablePhysicalMapping,
  ) => [number, { id: Uuid; introducingSchema: Uuid; introducingOrdinal: number }[]][],
): void {
  if (mappings.keysArray().some((s) => !aliases.has(s)))
    throw invalid(`${kind} enum registry provenance schema alias is missing`);
  const disagrees = `${kind} enum registry case provenance disagrees with authority identities`;
  for (const mapping of mappings.valuesArray())
    for (const table of mapping.tables.valuesArray())
      for (const [columnId, cases] of registries(table))
        for (const c of cases) {
          const origin = mappings.get(c.introducingSchema);
          if (!origin)
            throw invalid(`${kind} enum registry case provenance references an unknown schema`);
          const authorityHas = (
            candidate: SchemaPhysicalMapping,
            pred: (variants: Uuid[]) => boolean,
          ) =>
            [...candidate.tables].some(
              ([tableName, t]) =>
                t.tableId === table.tableId &&
                [...t.columns].some(([columnName, id]) => {
                  if (id !== columnId) return false;
                  const identity = candidate.identities.tables
                    .get(tableName)
                    ?.columns.get(columnName);
                  return identity !== undefined && identity.enumVariants.valuesArray().some(pred);
                }),
            );
          if (
            !authorityHas(
              origin,
              (variants) =>
                variants[c.introducingOrdinal] !== undefined &&
                equalBytes(variants[c.introducingOrdinal], c.id),
            )
          )
            throw invalid(disagrees);
          let earliest: Uuid | undefined;
          for (const [schema, candidate] of mappings)
            if (authorityHas(candidate, (variants) => variants.some((id) => equalBytes(id, c.id))))
              if (earliest === undefined || aliases.get(schema)! < aliases.get(earliest)!)
                earliest = schema;
          if (earliest === undefined) throw invalid(disagrees);
          if (!equalBytes(earliest, c.introducingSchema))
            throw invalid(`${kind} enum registry case provenance does not name its introduction`);
        }
}

function tableMapping(
  mappings: Mappings,
  schemaVersion: Uuid,
  logicalTable: string,
  missing: string,
): TablePhysicalMapping {
  const m = mappings.get(schemaVersion)?.tables.get(logicalTable);
  if (!m) throw invalid(missing);
  return m;
}

function requireSchemaTable(
  catalogueSchemas: SortedMap<Uuid, any>,
  schemaVersion: Uuid,
  logicalTable: string,
  missing: string,
): any {
  const schema = catalogueSchemas.get(schemaVersion);
  if (!schema) throw invalid(missing);
  const table = schema.schema.tables.find((t: any) => t.name === logicalTable);
  if (!table) throw nodeError("TableNotFound", logicalTable);
  return table;
}

/** Resolve the physical history table a registered logical table scans. */
export function physicalHistorySourceTable(
  catalogueSchemas: SortedMap<Uuid, any>,
  aliases: Aliases,
  mappings: Mappings,
  schemaVersion: Uuid,
  logicalTable: string,
): string {
  requireSchemaTable(
    catalogueSchemas,
    schemaVersion,
    logicalTable,
    "physical history schema missing",
  );
  if (!aliases.has(schemaVersion)) throw invalid("physical history schema alias missing");
  return physicalHistoryStorageTable(mappings, schemaVersion, logicalTable);
}

export function physicalHistoryStorageTable(
  mappings: Mappings,
  schemaVersion: Uuid,
  logicalTable: string,
): string {
  return physicalHistoryTableName(
    tableMapping(mappings, schemaVersion, logicalTable, "physical history table mapping missing")
      .tableId,
  );
}

export function physicalCurrentSourceTable(
  catalogueSchemas: SortedMap<Uuid, any>,
  mappings: Mappings,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
): string {
  requireSchemaTable(
    catalogueSchemas,
    schemaVersion,
    logicalTable,
    "physical current schema missing",
  );
  return physicalCurrentStorageTable(mappings, schemaVersion, logicalTable, cls);
}

export function physicalCurrentStorageTable(
  mappings: Mappings,
  schemaVersion: Uuid,
  logicalTable: string,
  cls: PhysicalCurrentClass,
): string {
  const id = tableMapping(
    mappings,
    schemaVersion,
    logicalTable,
    "physical current table mapping missing",
  ).tableId;
  return cls === "Global" ? physicalGlobalCurrentTableName(id) : physicalAheadCurrentTableName(id);
}

export function physicalRejectedVersionBinding(
  catalogueSchemas: SortedMap<Uuid, any>,
  mappings: Mappings,
  schemaVersion: Uuid,
  logicalTable: string,
): PhysicalHistoryBinding {
  const table = requireSchemaTable(
    catalogueSchemas,
    schemaVersion,
    logicalTable,
    "physical rejected-version schema missing",
  );
  const mapping = tableMapping(
    mappings,
    schemaVersion,
    logicalTable,
    "physical rejected-version table mapping missing",
  );
  return {
    storageTable: physicalRejectedVersionsTableName(mapping.tableId),
    descriptor: physicalRejectedVersionDescriptor(table, mapping),
  };
}
