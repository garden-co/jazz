// Durable schema catalogue: lineage publication, physical mappings, active schema and aliases.
import {
  type MigrationLens,
  type NodeUuid,
  type SchemaVersionId,
  CatalogueRecordKind,
  CatalogueRowRecord,
  NodeAliasRowRecord,
  PhysicalIdentityManifest,
  PrimaryKeyValue,
  SchemaLineagePublication,
  SchemaVersion,
  UUID_NIL,
  allocatePhysicalVariantCases,
  allocateProvisionalPhysicalMapping,
  encodeActiveSchema,
  encodeCatalogueBootstrapReady,
  encodeCatalogueLens,
  encodeCatalogueLineageActivation,
  encodeCataloguePendingLineage,
  encodeCatalogueSchema,
  encodeCatalogueStagedLineage,
  encodePhysicalMapping,
  enumCaseKey,
  physicalColumnEpochIsCompatible,
  schemaIdFromKey,
  schemaIdKey,
  uuidEquals,
  validatePayloadEnumCaseProvenance,
  validateScalarEnumCaseProvenance,
} from "../../../_extern/node-state.js";
import {
  ActiveSchema,
  type PendingSchemaLineage,
  type SchemaCatalogue,
  type StagedSchemaLineage,
  NodeState,
  V,
  hydrateNestedPayloadEnumCases,
  hydrateNestedScalarEnumCases,
  installEnumCaseIds,
  installPayloadEnumCaseIds,
  invalidCatalogue,
  invalidStored,
  nextGrooveRuntimeToken,
  nodeErr,
} from "../index.js";

const SCALAR_CASE_CAPACITY = 256;
const CATALOGUE = "jazz_catalogue";
const NP = () => NodeState.prototype as any;

/** Stage an upsert of one `jazz_catalogue` record. */
function putCatalogue(batch: any, kind: any, id: any, payload: Uint8Array): void {
  batch.update(CATALOGUE, [
    V("U64", CatalogueRecordKind.key(kind)),
    V("Uuid", id),
    V("Bytes", payload),
  ]);
}

/** Stage the deletion of one `jazz_catalogue` record. */
function deleteCatalogue(batch: any, kind: any, id: any): void {
  batch.delete(
    CATALOGUE,
    PrimaryKeyValue.Composite([
      PrimaryKeyValue.U64(CatalogueRecordKind.key(kind)),
      PrimaryKeyValue.Uuid(id),
    ]),
  );
}

const req = <T>(v: T | undefined, message: string): T => {
  if (v === undefined) throw invalidStored(message);
  return v;
};
const entry = <K, V>(m: Map<K, V>, k: K, make: () => V): V => {
  let v = m.get(k);
  if (v === undefined) m.set(k, (v = make()));
  return v;
};
const cloneMapping = (m: any) => m.clone();
const retainKeys = <V>(m: Map<number, V>, live: Set<number>) => {
  for (const k of [...m.keys()]) if (!live.has(k)) m.delete(k);
};
const activeSchemaEquals = (a: ActiveSchema, b: ActiveSchema) =>
  a.revision === b.revision && uuidEquals(a.schema, b.schema) && a.compiled.equals(b.compiled);

function lineageParents(lineages: Iterable<any>): Map<string, string[]> {
  const parents = new Map<string, string[]>();
  for (const l of lineages)
    parents.set(
      schemaIdKey(l.publication.schema.id),
      l.publication.predecessors.map((p: any) => schemaIdKey(p.lens.source)),
    );
  return parents;
}

function predecessorSources(
  predecessors: any[],
  schemas: Map<string, any>,
  mappings: Map<string, any>,
): Map<string, [any, any]> {
  const sources = new Map<string, [any, any]>();
  for (const predecessor of predecessors) {
    const source = schemas.get(schemaIdKey(predecessor.lens.source));
    if (!source) throw invalidCatalogue("schema predecessor is missing");
    const mapping = mappings.get(schemaIdKey(source.id));
    if (!mapping) throw invalidCatalogue("schema predecessor identities are missing");
    sources.set(schemaIdKey(source.id), [source.schema, mapping.identities]);
  }
  return sources;
}

function reconcilePublicationMapping(
  catalogue: SchemaCatalogue,
  publication: any,
  provisional: any,
): any {
  let mapping = cloneMapping(provisional);
  const predecessors = [...publication.predecessors].sort((a: any, b: any) =>
    schemaIdKey(a.lens.source) < schemaIdKey(b.lens.source) ? -1 : 1,
  );
  for (const p of predecessors)
    mapping = reconcilePhysicalMappingForLensPayloadInCatalogue(
      catalogue,
      p.lens,
      publication.schema,
      mapping,
    );
  return mapping;
}

function validateCandidateScalarEnumCapacity(catalogue: SchemaCatalogue, candidate: any): void {
  const all = [...catalogue.physicalMappings.values(), candidate];
  const tooMany = (collect: (table: any) => any[] | undefined, tableId: number) => {
    const cases = new Set<string>();
    for (const mapping of all)
      for (const table of mapping.tables.values())
        if (table.tableId === tableId)
          for (const c of collect(table) ?? []) cases.add(enumCaseKey(c));
    if (cases.size > SCALAR_CASE_CAPACITY)
      throw invalidCatalogue("physical scalar enum registry exceeds u8 capacity");
  };
  for (const candidateTable of candidate.tables.values()) {
    for (const columnId of candidateTable.scalarEnumCases.keys())
      tooMany((t) => t.scalarEnumCases.get(columnId), candidateTable.tableId);
    for (const [columnId, paths] of candidateTable.nestedScalarEnumCases)
      for (const path of paths.keys())
        tooMany((t) => t.nestedScalarEnumCases.get(columnId)?.get(path), candidateTable.tableId);
  }
}

function reconcilePhysicalMappingForLensPayloadInCatalogue(
  catalogue: SchemaCatalogue,
  lens: MigrationLens,
  targetVersion: any,
  provisionalTarget: any,
): any {
  const sourceMapping = req(
    catalogue.physicalMappings.get(schemaIdKey(lens.source)),
    "source physical mapping missing",
  );
  const targetMapping = cloneMapping(provisionalTarget);
  const sourceSchema = req(
    catalogue.catalogueSchemas.get(schemaIdKey(lens.source)),
    "source physical mapping schema missing",
  ).schema;
  const targetSchema = targetVersion.schema;
  for (const tableLens of lens.tableLenses) {
    const sourceTable = req(
      sourceMapping.tables.get(tableLens.sourceTable),
      "source physical table mapping missing",
    );
    const provisionalTable = req(
      targetMapping.tables.get(tableLens.targetTable),
      "target physical table mapping missing",
    ).clone();
    const sourceTableSchema = req(
      sourceSchema.tables.find((t: any) => t.name === tableLens.sourceTable),
      "source physical table schema missing",
    );
    const targetTableSchema = req(
      targetSchema.tables.find((t: any) => t.name === tableLens.targetTable),
      "target physical table schema missing",
    );
    const targetIdentities = req(
      targetMapping.identities.tables.get(tableLens.targetTable),
      "target physical table identities missing",
    );
    const provisionalColumn = (name: string): number =>
      req(provisionalTable.columns.get(name), "target provisional physical column mapping missing");
    const working = new Map<string, [number, string | undefined]>(
      [...sourceTable.columns].map(([name, id]: [string, number]) => [name, [id, name]]),
    );
    for (const op of tableLens.ops) {
      switch (op.kind) {
        case "RenameColumn": {
          const id = working.get(op.from);
          if (id) {
            working.delete(op.from);
            working.set(op.to, id);
          }
          break;
        }
        case "CopyColumn":
          working.set(op.to, [provisionalColumn(op.to), undefined]);
          break;
        case "AddColumn":
          working.set(op.column, [provisionalColumn(op.column), undefined]);
          break;
        case "DropColumn":
          working.delete(op.column);
          break;
      }
    }
    const targetNames = new Set(targetTableSchema.columns.map((c: any) => c.name));
    for (const name of [...working.keys()]) if (!targetNames.has(name)) working.delete(name);
    for (const column of targetTableSchema.columns)
      if (!working.has(column.name))
        working.set(column.name, [provisionalColumn(column.name), undefined]);
    const columns = new Map<string, number>();
    for (const [targetName, [inherited, sourceName]] of [...working].sort(([a], [b]) =>
      a < b ? -1 : 1,
    )) {
      const compatible =
        sourceName !== undefined &&
        physicalColumnEpochIsCompatible(
          sourceTableSchema,
          sourceName,
          targetTableSchema,
          targetName,
        );
      columns.set(targetName, compatible ? inherited : provisionalColumn(targetName));
    }
    const scalarEnumCases = new Map<number, any[]>();
    const payloadEnumCases = new Map<number, any[]>();
    for (const column of targetTableSchema.columns) {
      const id = req(columns.get(column.name), "enum physical column missing");
      const ids = targetIdentities.columns.get(column.name).enumVariants.get("root");
      if (column.columnType.kind === "EnumTag") {
        const cases = [
          ...(sourceTable.scalarEnumCases.get(id) ??
            provisionalTable.scalarEnumCases.get(id) ??
            []),
        ];
        if (cases.length > column.columnType.schema.variants.length)
          throw invalidStored("scalar enum registry changed non-additively");
        installEnumCaseIds(cases, ids, targetVersion.id);
        scalarEnumCases.set(id, cases);
      } else if (column.columnType.kind === "Enum") {
        const cases = [
          ...(sourceTable.payloadEnumCases.get(id) ??
            provisionalTable.payloadEnumCases.get(id) ??
            []),
        ];
        if (cases.length > column.columnType.schema.cases.length)
          throw invalidStored("payload enum registry changed non-additively");
        installPayloadEnumCaseIds(cases, ids, targetVersion.id);
        payloadEnumCases.set(id, cases);
      }
    }
    const nestedScalar = new Map([
      ...provisionalTable.nestedScalarEnumCases,
      ...sourceTable.nestedScalarEnumCases,
    ]);
    const nestedPayload = new Map([
      ...provisionalTable.nestedPayloadEnumCases,
      ...sourceTable.nestedPayloadEnumCases,
    ]);
    for (const column of targetTableSchema.columns) {
      const id = req(columns.get(column.name), "nested enum physical column missing");
      const identities = targetIdentities.columns.get(column.name).enumVariants;
      hydrateNestedScalarEnumCases(
        column.columnType,
        identities,
        targetVersion.id,
        "root",
        "root",
        entry(nestedScalar as Map<number, any>, id, () => new Map()),
      );
      hydrateNestedPayloadEnumCases(
        column.columnType,
        identities,
        targetVersion.id,
        "root",
        "root",
        entry(nestedPayload as Map<number, any>, id, () => new Map()),
      );
    }
    // Registry entries belong to a live physical column.
    const live = new Set(columns.values());
    for (const m of [scalarEnumCases, payloadEnumCases, nestedScalar, nestedPayload])
      retainKeys(m as Map<number, any>, live);
    targetMapping.tables.set(tableLens.targetTable, {
      tableId: sourceTable.tableId,
      columns,
      variantCases: [],
      scalarEnumCases,
      payloadEnumCases,
      nestedScalarEnumCases: nestedScalar,
      nestedPayloadEnumCases: nestedPayload,
    });
  }
  validateCandidateScalarEnumCapacity(catalogue, targetMapping);
  return targetMapping;
}

export function reconcileSourcePhysicalMappingForLensPayload(
  lens: MigrationLens,
  sourceVersion: any,
  targetVersion: any,
  provisionalSource: any,
  targetMapping: any,
): any {
  const sourceMapping = cloneMapping(provisionalSource);
  for (const tableLens of lens.tableLenses) {
    const provisionalTable = req(
      sourceMapping.tables.get(tableLens.sourceTable),
      "source provisional physical table mapping missing",
    ).clone();
    const targetTable = req(
      targetMapping.tables.get(tableLens.targetTable),
      "target physical table mapping missing",
    );
    const sourceTableSchema = req(
      sourceVersion.schema.tables.find((t: any) => t.name === tableLens.sourceTable),
      "source physical table schema missing",
    );
    const targetTableSchema = req(
      targetVersion.schema.tables.find((t: any) => t.name === tableLens.targetTable),
      "target physical table schema missing",
    );
    const targetNameBySource = new Map<string, string | undefined>(
      sourceTableSchema.columns.map((c: any) => [c.name, c.name]),
    );
    const retarget = (from: string, to: string | undefined) => {
      for (const [k, v] of targetNameBySource)
        if (v === from) {
          targetNameBySource.set(k, to);
          break;
        }
    };
    for (const op of tableLens.ops) {
      if (op.kind === "RenameColumn") retarget(op.from, op.to);
      else if (op.kind === "DropColumn") retarget(op.column, undefined);
    }
    const columns = new Map<string, number>();
    for (const sourceColumn of sourceTableSchema.columns) {
      const provisionalId = req(
        provisionalTable.columns.get(sourceColumn.name),
        "source provisional physical column mapping missing",
      );
      const targetName = targetNameBySource.get(sourceColumn.name);
      const inherited =
        targetName !== undefined &&
        physicalColumnEpochIsCompatible(
          sourceTableSchema,
          sourceColumn.name,
          targetTableSchema,
          targetName,
        )
          ? targetTable.columns.get(targetName)
          : undefined;
      columns.set(sourceColumn.name, inherited ?? provisionalId);
    }
    const live = new Set(columns.values());
    const merged = (a: Map<number, any>, b: Map<number, any>) => {
      const m = new Map([...a, ...b]);
      retainKeys(m, live);
      return m;
    };
    sourceMapping.tables.set(tableLens.sourceTable, {
      tableId: targetTable.tableId,
      columns,
      variantCases: [...targetTable.variantCases],
      scalarEnumCases: merged(provisionalTable.scalarEnumCases, targetTable.scalarEnumCases),
      payloadEnumCases: merged(provisionalTable.payloadEnumCases, targetTable.payloadEnumCases),
      nestedScalarEnumCases: merged(
        provisionalTable.nestedScalarEnumCases,
        targetTable.nestedScalarEnumCases,
      ),
      nestedPayloadEnumCases: merged(
        provisionalTable.nestedPayloadEnumCases,
        targetTable.nestedPayloadEnumCases,
      ),
    });
  }
  return sourceMapping;
}

export const catalogueMethods = {
  /** Apply a node batch and wait until it is durably persisted. */
  async applyAndPersistNodeBatch(this: NodeState, batch: any): Promise<void> {
    const applied = await this.applyNodeBatch(batch);
    this.database.finishPersistence(await this.database.persistWithProgress(applied));
  },

  /** Reservation history for admitting `candidate`; descendant parked envelopes are excluded. */
  physicalIdentityHistoryForCandidate(
    this: NodeState,
    candidate: SchemaVersionId,
    omit?: any,
  ): any[] {
    const c = this.catalogue;
    const parents = lineageParents([
      ...c.activeLineagesByTarget.values(),
      ...c.stagedLineages.valuesArray(),
      ...c.pendingLineages.valuesArray(),
    ]);
    const target = schemaIdKey(candidate);
    const isDescendant = (schema: string) => {
      const remaining = [schema];
      const visited = new Set<string>();
      while (remaining.length) {
        const cursor = remaining.pop()!;
        if (visited.has(cursor)) continue;
        visited.add(cursor);
        const sources = parents.get(cursor);
        if (sources) {
          if (sources.includes(target)) return true;
          remaining.push(...sources);
        }
      }
      return false;
    };
    const keep = (l: any) =>
      !(omit !== undefined && uuidEquals(l.publication.id, omit)) &&
      !isDescendant(schemaIdKey(l.publication.schema.id));
    return [
      ...[...c.physicalMappings.values()].map((m) => m.identities),
      ...c.stagedLineages
        .valuesArray()
        .filter(keep)
        .map((l) => l.publication.physicalIdentities),
      ...c.pendingLineages
        .valuesArray()
        .filter(keep)
        .map((l) => l.publication.physicalIdentities),
    ];
  },
  /** Single-predecessor convenience entry point. */
  authorSchemaLineagePublication(
    this: NodeState,
    schema: any,
    lens: MigrationLens,
    newTables: Iterable<string>,
    droppedTables: Iterable<string>,
  ) {
    return this.authorSchemaFromPredecessors(schema, [
      { lens, newTables: [...newTables], droppedTables: [...droppedTables] },
    ]);
  },
  /** Author one immutable schema record containing every incoming migration. */
  authorSchemaFromPredecessors(this: NodeState, schema: any, predecessors: any[]) {
    const sources = predecessorSources(
      predecessors,
      this.catalogue.catalogueSchemas,
      this.catalogue.physicalMappings,
    );
    try {
      return SchemaLineagePublication.authorFromPredecessors(
        schema,
        predecessors,
        sources,
        this.physicalIdentityHistoryForCandidate(schema.id, undefined),
      );
    } catch (e) {
      throw invalidCatalogue(String((e as Error).message ?? e));
    }
  },
  validatePublicationSources(
    this: unknown,
    publication: any,
    schemas: Map<string, any>,
    mappings: Map<string, any>,
    history: any[],
  ) {
    const sources = predecessorSources(publication.predecessors, schemas, mappings);
    for (const p of publication.predecessors) {
      const source = schemas.get(schemaIdKey(p.lens.source));
      NP().validateMigrationLensBetween(p.lens, source, publication.schema);
      NP().validateLineageTablePartition(
        source.schema,
        publication.schema.schema,
        p.lens,
        p.newTables,
        p.droppedTables,
      );
    }
    try {
      publication.validatePredecessorIdentities(sources, history);
    } catch (e) {
      throw invalidCatalogue(String((e as Error).message ?? e));
    }
  },
  async persistCatalogueSchema(this: NodeState, schema: any): Promise<void> {
    const batch = this.database.openBatch();
    putCatalogue(batch, CatalogueRecordKind.Schema, schema.id, encodeCatalogueSchema(schema));
    await this.applyAndPersistNodeBatch(batch);
  },
  hydrateEnumCaseMapping(this: NodeState, schemaVersion: SchemaVersionId): void {
    const key = schemaIdKey(schemaVersion);
    const schema = req(
      this.catalogue.catalogueSchemas.get(key),
      "physical mapping schema payload missing",
    ).schema;
    const mapping = req(this.catalogue.physicalMappings.get(key), "physical mapping missing");
    for (const table of schema.tables) {
      const tableIdentities = req(
        mapping.identities.tables.get(table.name),
        "physical table identities missing",
      );
      const physical = mapping.tables.get(table.name);
      if (!physical) continue;
      for (const column of table.columns) {
        if (column.columnType.kind !== "EnumTag") continue;
        const id = physical.columns.get(column.name);
        if (id === undefined) continue;
        const ids = tableIdentities.columns.get(column.name).enumVariants.get("root");
        if (ids.length !== column.columnType.schema.variants.length)
          throw invalidStored("scalar enum authority identity width mismatch");
        installEnumCaseIds(
          entry(physical.scalarEnumCases, id, () => []),
          ids,
          schemaVersion,
        );
      }
      // Each column writes only its own registry entries, so payload and non-payload columns hydrate in one pass.
      for (const column of table.columns) {
        const id = physical.columns.get(column.name);
        if (id === undefined) continue;
        const identities = tableIdentities.columns.get(column.name).enumVariants;
        if (column.columnType.kind === "Enum") {
          const ids = identities.get("root");
          if (ids.length !== column.columnType.schema.cases.length)
            throw invalidStored("payload enum authority identity width mismatch");
          installPayloadEnumCaseIds(
            entry(physical.payloadEnumCases, id, () => []),
            ids,
            schemaVersion,
          );
        }
        hydrateNestedScalarEnumCases(
          column.columnType,
          identities,
          schemaVersion,
          "root",
          "root",
          entry(physical.nestedScalarEnumCases, id, () => new Map()),
        );
        hydrateNestedPayloadEnumCases(
          column.columnType,
          identities,
          schemaVersion,
          "root",
          "root",
          entry(physical.nestedPayloadEnumCases, id, () => new Map()),
        );
      }
    }
  },
  async ensureProvisionalPhysicalMapping(
    this: NodeState,
    schemaVersion: SchemaVersionId,
  ): Promise<void> {
    const key = schemaIdKey(schemaVersion);
    const c = this.catalogue;
    if (c.physicalMappings.has(key) && c.schemaVersionAliases.has(key)) {
      this.hydrateEnumCaseMapping(schemaVersion);
      return;
    }
    const schema = req(
      c.catalogueSchemas.get(key),
      "physical mapping schema payload missing",
    ).schema;
    let mapping = c.physicalMappings.get(key);
    if (!mapping) {
      const tables = new Map<string, any>();
      for (const table of schema.tables) {
        const tableId = this.allocatePhysicalTableId();
        const columns = new Map<string, number>();
        for (const column of table.columns)
          columns.set(column.name, this.allocatePhysicalColumnId());
        tables.set(table.name, {
          tableId,
          columns,
          variantCases: [],
          scalarEnumCases: new Map(),
          payloadEnumCases: new Map(),
          nestedScalarEnumCases: new Map(),
          nestedPayloadEnumCases: new Map(),
        });
      }
      mapping = { identities: PhysicalIdentityManifest.allocate(schema), tables };
    }
    let alias = c.schemaVersionAliases.get(key);
    if (alias === undefined) {
      alias = Math.max(0, ...c.schemaVersionAliases.values()) + 1;
      if (!Number.isSafeInteger(alias)) throw invalidStored("schema version alias exhausted");
    }
    const candidateMappings = new Map(c.physicalMappings).set(key, mapping);
    const candidateAliases = new Map(c.schemaVersionAliases).set(key, alias);
    for (const table of schema.tables)
      allocatePhysicalVariantCases(
        candidateMappings,
        candidateAliases,
        schemaVersion,
        table.name,
        table.columns.map((col: any) => col.name),
      );
    const allocated = req(candidateMappings.get(key), "allocated physical mapping disappeared");
    const batch = this.database.openBatch();
    this.writeSchemaVersionMappingToBatch(batch, alias, schemaVersion, allocated);
    await this.applyAndPersistNodeBatch(batch);
    c.schemaVersionAliases.set(key, alias);
    if (key === schemaIdKey(c.localSchemaVersionId)) c.localSchemaVersionAlias = alias;
    c.physicalMappings.set(key, allocated);
  },
  nextSchemaVersionAlias(this: NodeState): number {
    let max = 0;
    for (const alias of this.catalogue.schemaVersionAliases.values()) max = Math.max(max, alias);
    for (const mapping of this.catalogue.physicalMappings.values())
      for (const table of mapping.tables.values())
        for (const c of table.variantCases) max = Math.max(max, c.tag);
    if (!Number.isSafeInteger(max + 1)) throw invalidStored("schema version alias exhausted");
    return max + 1;
  },
  allocatePhysicalTableId(this: NodeState): number {
    const id = this.catalogue.nextPhysicalTableId++;
    if (!Number.isSafeInteger(this.catalogue.nextPhysicalTableId))
      throw invalidStored("physical table id exhausted");
    return id;
  },
  allocatePhysicalColumnId(this: NodeState): number {
    const id = this.catalogue.nextPhysicalColumnId++;
    if (!Number.isSafeInteger(this.catalogue.nextPhysicalColumnId))
      throw invalidStored("physical column id exhausted");
    return id;
  },
  /** Admission must not reserve durable state for a lineage whose shared scalar registry cannot be encoded. */
  validatePendingSchemaLineagePhysicalMapping(
    this: NodeState,
    pending: PendingSchemaLineage,
  ): void {
    const c = this.catalogue;
    if (
      pending.publication.predecessors.some(
        (p: any) => !c.catalogueSchemas.has(schemaIdKey(p.lens.source)),
      )
    )
      return;
    // Allocate against copies; activation performs the identical reconciliation.
    const ids = { table: c.nextPhysicalTableId, column: c.nextPhysicalColumnId };
    const provisional = allocateProvisionalPhysicalMapping(
      pending.publication.schema.schema,
      pending.publication.physicalIdentities,
      ids,
    );
    const mapping = reconcilePublicationMapping(c, pending.publication, provisional);
    const target = schemaIdKey(pending.publication.schema.id);
    const candidateMappings = new Map(c.physicalMappings).set(target, mapping);
    const candidateAliases = new Map(c.schemaVersionAliases).set(
      target,
      this.nextSchemaVersionAlias(),
    );
    validateScalarEnumCaseProvenance(candidateMappings, candidateAliases);
    validatePayloadEnumCaseProvenance(candidateMappings, candidateAliases);
  },
  reconcilePhysicalMappingForLens(this: NodeState, lens: MigrationLens): any {
    const key = schemaIdKey(lens.target);
    const targetMapping = req(
      this.catalogue.physicalMappings.get(key),
      "target physical mapping missing",
    );
    const targetSchema = req(
      this.catalogue.catalogueSchemas.get(key),
      "target physical mapping schema missing",
    );
    return this.reconcilePhysicalMappingForLensPayload(lens, targetSchema, targetMapping);
  },
  reconcilePhysicalMappingForLensPayload(
    this: NodeState,
    lens: MigrationLens,
    targetVersion: any,
    provisional: any,
  ) {
    return reconcilePhysicalMappingForLensPayloadInCatalogue(
      this.catalogue,
      lens,
      targetVersion,
      provisional,
    );
  },
  reconcilePublicationMapping(this: NodeState, publication: any, provisional: any) {
    return reconcilePublicationMapping(this.catalogue, publication, provisional);
  },
  async persistCatalogueSchemaLineage(this: NodeState, staged: StagedSchemaLineage): Promise<void> {
    const batch = this.database.openBatch();
    putCatalogue(
      batch,
      CatalogueRecordKind.SchemaLineageStaged,
      staged.publication.id,
      encodeCatalogueStagedLineage(staged),
    );
    await this.applyAndPersistNodeBatch(batch);
  },
  async persistPendingSchemaLineage(this: NodeState, pending: PendingSchemaLineage): Promise<void> {
    this.validatePendingSchemaLineagePhysicalMapping(pending);
    const batch = this.database.openBatch();
    putCatalogue(
      batch,
      CatalogueRecordKind.SchemaLineagePending,
      pending.publication.id,
      encodeCataloguePendingLineage(pending),
    );
    await this.applyAndPersistNodeBatch(batch);
  },
  async removePendingSchemaLineage(
    this: NodeState,
    catalogueSeq: number,
    publicationId: any,
  ): Promise<void> {
    const batch = this.database.openBatch();
    deleteCatalogue(batch, CatalogueRecordKind.SchemaLineagePending, publicationId);
    await this.applyAndPersistNodeBatch(batch);
    this.catalogue.pendingLineages.delete(catalogueSeq);
  },
  /** One durable activation batch: drop the pending obligation, persist payload, schema, lenses, mapping and marker. */
  writeActiveSchemaLineageToBatch(this: unknown, batch: any, staged: StagedSchemaLineage): void {
    const schema = staged.publication.schema;
    deleteCatalogue(batch, CatalogueRecordKind.SchemaLineagePending, staged.publication.id);
    putCatalogue(
      batch,
      CatalogueRecordKind.SchemaLineageStaged,
      staged.publication.id,
      encodeCatalogueStagedLineage(staged),
    );
    putCatalogue(batch, CatalogueRecordKind.Schema, schema.id, encodeCatalogueSchema(schema));
    for (const p of staged.publication.predecessors)
      putCatalogue(batch, CatalogueRecordKind.Lens, p.lens.id, encodeCatalogueLens(p.lens));
    NP().writeSchemaVersionMappingToBatch(batch, staged.alias, schema.id, staged.mapping);
    const active = { id: staged.publication.id, catalogueSeq: staged.catalogueSeq };
    putCatalogue(
      batch,
      CatalogueRecordKind.SchemaLineageActive,
      active.id,
      encodeCatalogueLineageActivation(active),
    );
  },
  async persistCatalogueLensWithPhysicalMetadata(
    this: NodeState,
    lens: MigrationLens,
    mapping?: any,
  ): Promise<void> {
    const batch = this.database.openBatch();
    putCatalogue(batch, CatalogueRecordKind.Lens, lens.id, encodeCatalogueLens(lens));
    if (mapping) {
      const alias = req(
        this.catalogue.schemaVersionAliases.get(schemaIdKey(lens.target)),
        "physical mapping schema alias missing",
      );
      this.writeSchemaVersionMappingToBatch(batch, alias, lens.target, mapping);
    }
    await this.applyAndPersistNodeBatch(batch);
  },
  writeSchemaVersionMappingToBatch(
    this: unknown,
    batch: any,
    alias: number,
    schemaVersion: SchemaVersionId,
    mapping: any,
  ): void {
    batch.update("jazz_schema_versions", [
      V("U64", alias),
      V("Uuid", schemaVersion),
      V("Bytes", encodePhysicalMapping(mapping)),
    ]);
  },
  writeActiveSchemaToBatch(this: unknown, batch: any, active: ActiveSchema): void {
    putCatalogue(batch, CatalogueRecordKind.ActiveSchema, UUID_NIL, encodeActiveSchema(active));
  },
  async writeActiveSchemaBootstrapToBatch(
    this: NodeState,
    batch: any,
    active: ActiveSchema,
  ): Promise<void> {
    if (!this.catalogueBootstrapMarker) return;
    const [genesis] = await this.database.primaryKeyScanRaw(CATALOGUE, [
      V("U64", CatalogueRecordKind.key(CatalogueRecordKind.Genesis)),
    ]);
    if (!genesis) throw invalidStored("catalogue genesis missing");
    const ready = {
      genesis: genesis.record().getUuid(CatalogueRowRecord.FIELD_ID_IDX),
      currentWriteSchema: active.wirePointer(),
      activeCatalogueSeq: this.catalogue.activeCatalogueSeq,
    };
    putCatalogue(
      batch,
      CatalogueRecordKind.BootstrapReady,
      ready.genesis,
      encodeCatalogueBootstrapReady(ready),
    );
  },
  async persistActiveSchema(this: NodeState, active: ActiveSchema): Promise<void> {
    const batch = this.database.openBatch();
    this.writeActiveSchemaToBatch(batch, active);
    await this.writeActiveSchemaBootstrapToBatch(batch, active);
    // Migrate a legacy policy-bearing catalogue payload on first activation.
    const structural = SchemaVersion.new(active.compiled.withoutPermissions());
    putCatalogue(
      batch,
      CatalogueRecordKind.Schema,
      structural.id,
      encodeCatalogueSchema(structural),
    );
    await this.applyAndPersistNodeBatch(batch);
  },
  async activateSchema(this: NodeState, revision: number, compiled: any): Promise<void> {
    const active = this.prepareActiveSchema(revision, compiled);
    if (activeSchemaEquals(active, this.catalogue.activeSchema)) return;
    await this.persistActiveSchema(active);
    this.installActiveSchema(active);
  },
  validateSchemaActivation(this: NodeState, revision: number, compiled: any): void {
    this.prepareActiveSchema(revision, compiled);
  },
  prepareActiveSchema(this: NodeState, revision: number, compiled: any): ActiveSchema {
    this.requireCatalogueReady();
    if (this.catalogueActivationFailed) throw nodeErr("CatalogueActivationFailed");
    if (revision === 0) throw invalidCatalogue("active schema revision must be positive");
    const pointer = { revision, schema: compiled.versionId() };
    if (!this.catalogue.catalogueSchemas.has(schemaIdKey(pointer.schema)))
      throw invalidCatalogue("active schema requires admitted structural lineage");
    if (this.shortestLensPath(this.catalogue.activeSchema.schema, pointer.schema) === undefined)
      throw invalidCatalogue("active schema requires a complete migration path");
    const active = ActiveSchema.create(pointer, compiled);
    const current = this.catalogue.activeSchema;
    if (revision < current.revision) throw invalidCatalogue("stale active schema revision");
    if (revision === current.revision && !activeSchemaEquals(active, current))
      throw invalidCatalogue("conflicting active schema revision");
    return active;
  },
  installActiveSchema(this: NodeState, active: ActiveSchema): void {
    const c = this.catalogue;
    const authorizationSourceChanged = !active.sameAuthorizationSource(
      c.activeSchema,
      c.physicalMappings.get(schemaIdKey(active.schema)),
      c.physicalMappings.get(schemaIdKey(c.activeSchema.schema)),
    );
    c.catalogueSchemas.set(
      schemaIdKey(active.schema),
      SchemaVersion.new(active.compiled.withoutPermissions()),
    );
    if (uuidEquals(active.schema, c.localSchemaVersionId)) c.schema = active.compiled;
    c.activeSchema = active;
    const q = this.query;
    q.versionStorageSourcesCache.clear();
    q.compiledQueryProgramCache.clear();
    q.queryProgramTemplates.clear();
    q.supportedQueryProgramRequests.length = 0;
    q.readPolicyAuthorizationRequestCache.clear();
    q.policyAuthorizationGraphCache.clear();
    q.policyAuthorizationGraphReplacements.clear();
    if (authorizationSourceChanged) this.grooveRuntimeToken = nextGrooveRuntimeToken();
  },
  async ensureNodeAlias(this: NodeState, nodeUuid: NodeUuid): Promise<number> {
    const isSelf = uuidEquals(nodeUuid, this.nodeUuidValue);
    if (isSelf && this.selfNodeAlias !== undefined) return this.selfNodeAlias;
    const known = this.nodeAliases.get(nodeUuid);
    if (known !== undefined) {
      if (isSelf) this.selfNodeAlias = known;
      return known;
    }
    // Clear before the first await: a failed writer must not leave an absence proof alive.
    this.absentNodeAlias = undefined;
    let maxAlias = this.nodeAliases.maxAlias();
    for (const raw of await this.database.primaryKeyScanRaw("jazz_nodes", [])) {
      const record = raw.record();
      const alias = Number(record.getU64(NodeAliasRowRecord.FIELD_ID_IDX));
      maxAlias = Math.max(maxAlias, alias);
      if (uuidEquals(record.getUuid(NodeAliasRowRecord.FIELD_UUID_IDX), nodeUuid)) {
        this.nodeAliases.insert(nodeUuid, alias);
        if (isSelf) this.selfNodeAlias = alias;
        return alias;
      }
    }
    const alias = maxAlias + 1;
    if (!Number.isSafeInteger(alias)) throw invalidStored("node alias exhausted");
    const batch = this.database.openBatch();
    batch.insert("jazz_nodes", [V("U64", alias), V("Uuid", nodeUuid)]);
    await this.applyAndPersistNodeBatch(batch);
    // Only observable after its catalogue row is durable.
    this.nodeAliases.insert(nodeUuid, alias);
    if (isSelf) this.selfNodeAlias = alias;
    return alias;
  },
  async ensureSchemaVersionAlias(this: NodeState, schemaVersion: SchemaVersionId): Promise<number> {
    const key = schemaIdKey(schemaVersion);
    const alias = this.catalogue.schemaVersionAliases.get(key);
    if (alias !== undefined) {
      if (key === schemaIdKey(this.catalogue.localSchemaVersionId))
        this.catalogue.localSchemaVersionAlias = alias;
      return alias;
    }
    await this.ensureProvisionalPhysicalMapping(schemaVersion);
    return req(
      this.catalogue.schemaVersionAliases.get(key),
      "schema version alias allocation failed",
    );
  },
  schemaVersionForAlias(this: NodeState, alias: number): SchemaVersionId | undefined {
    for (const [key, candidate] of this.catalogue.schemaVersionAliases)
      if (candidate === alias) return schemaIdFromKey(key);
    return undefined;
  },
};
