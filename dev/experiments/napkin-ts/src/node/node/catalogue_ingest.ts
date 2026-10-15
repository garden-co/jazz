// Trusted catalogue snapshot ingestion and planning.
import { SortedMap, SortedSet, cmpStr } from "../../support/collections.js";
import {
  ActiveSchema,
  CatalogueRecordKind,
  SchemaVersion,
  allocateProvisionalPhysicalMapping,
  cloneCatalogue,
  deepEqual,
  deleteCatalogue,
  encodeCatalogueBootstrapReady,
  encodeCatalogueSchema,
  nextGrooveRuntimeToken,
  nodeError,
  nodeInvalidStoredValue,
  putCatalogue,
  uuidSimple,
  validateDurablePhysicalIdentityBindings,
  validatePayloadEnumCaseProvenance,
  validateScalarEnumCaseProvenance,
  NodeStatics,
} from "../../_extern/node-policy-peer.js";

type Catalogue = any;
type PlannedCatalogueSnapshot = { catalogue: Catalogue; activatedLineages: any[] };

const invalid = (msg: string) => nodeError("InvalidCatalogueUpdate", msg);

/** Whether a trusted catalogue replacement changes the live Groove runtime's input. */
export function activeRuntimeLayoutsEqual(left: Catalogue, right: Catalogue): boolean {
  if (
    left.localSchemaVersionId !== right.localSchemaVersionId ||
    left.localSchemaVersionAlias !== right.localSchemaVersionAlias ||
    !deepEqual(left.schema, right.schema)
  )
    return false;
  const active = left.localSchemaVersionId;
  const l = left.physicalMappings.get(active);
  const r = right.physicalMappings.get(active);
  return (
    left.schemaVersionAliases.get(active) === right.schemaVersionAliases.get(active) &&
    !!l &&
    !!r &&
    deepEqual(l.tables, r.tables)
  );
}

function nextSchemaVersionAliasInCatalogue(catalogue: Catalogue): number {
  let max = 0;
  for (const [, alias] of catalogue.schemaVersionAliases) max = Math.max(max, alias);
  if (max >= 0xffffffff) throw nodeInvalidStoredValue("schema version alias exhausted");
  return max + 1;
}

/** Adopt the authority's UUIDs without changing local enum tags. */
function rebindProvisionalMappingIdentities(mapping: any, identities: any): Map<string, string> {
  const replacements = new Map<string, string>();
  if (deepEqual(mapping.identities, identities)) return replacements;
  const origins = new Map<string, string>();
  for (const [tableName, table] of mapping.identities.tables) {
    const authorityTable = identities.tables.get(tableName);
    if (!authorityTable) throw invalid("snapshot identity table missing");
    for (const [columnName, column] of table.columns) {
      const authorityColumn = authorityTable.columns.get(columnName);
      if (!authorityColumn) throw invalid("snapshot identity column missing");
      for (const [path, cases] of column.enumVariants) {
        const authorityCases = authorityColumn.enumVariants.get(path);
        if (!authorityCases) throw invalid("snapshot enum identity occurrence missing");
        if (cases.length !== authorityCases.length)
          throw invalid("snapshot enum identity occurrence width differs");
        cases.forEach((old: string, i: number) => {
          const next = authorityCases[i];
          const prevR = replacements.get(old);
          const prevO = origins.get(next);
          replacements.set(old, next);
          origins.set(next, old);
          if ((prevR !== undefined && prevR !== next) || (prevO !== undefined && prevO !== old))
            throw invalid("snapshot enum identity rebind is ambiguous");
        });
      }
    }
  }
  for (const [old, next] of replacements)
    if (old !== next && replacements.has(next))
      throw invalid("snapshot enum identity rebind overlaps provisional identities");
  rebindMappingEnumCases(mapping, replacements);
  mapping.identities = identities;
  return replacements;
}

function rebindMappingEnumCases(mapping: any, replacements: Map<string, string>): void {
  if (replacements.size === 0) return;
  const pathReplacements = new Map<string, string>();
  for (const [old, next] of replacements) pathReplacements.set(uuidSimple(old), uuidSimple(next));
  const rebindPath = (path: string) => {
    let previous = "";
    return path
      .split("/")
      .map((component) => {
        const rebound =
          previous === "case" ? (pathReplacements.get(component) ?? component) : component;
        previous = component;
        return rebound;
      })
      .join("/");
  };
  const rebindCases = (cases: any[]) => {
    for (const c of cases) {
      const id = replacements.get(c.id);
      if (id !== undefined) c.id = id;
    }
  };
  const rebindNested = (byColumn: Map<any, Map<string, any[]>>) => {
    for (const [column, paths] of byColumn) {
      const next = new SortedMap<string, any[]>(cmpStr);
      for (const [path, cases] of paths) {
        rebindCases(cases);
        if (next.set(rebindPath(path), cases) !== undefined)
          throw invalid("snapshot enum occurrence rebind collides");
      }
      byColumn.set(column, next as any);
    }
  };
  for (const [, table] of mapping.tables) {
    for (const [, cases] of table.scalarEnumCases) rebindCases(cases);
    for (const [, cases] of table.payloadEnumCases) rebindCases(cases);
    rebindNested(table.nestedScalarEnumCases);
    rebindNested(table.nestedPayloadEnumCases);
  }
}

/** Roll a failed snapshot activation back and refuse further catalogue activation. */
function failCatalogueActivation(node: any, previous: Catalogue, checkpoint: any): Error {
  node.catalogue = previous;
  if (checkpoint !== undefined) node.database.restoreRuntimeRegistry(checkpoint);
  node.catalogueActivationFailed = true;
  return nodeError("CatalogueActivationFailed");
}

function catalogueGenesis(catalogue: Catalogue): string {
  const targets = new Set<string>();
  for (const [k] of catalogue.activeLineagesByTarget) targets.add(k);
  for (const [schema] of catalogue.catalogueSchemas) if (!targets.has(schema)) return schema;
  throw nodeInvalidStoredValue("catalogue genesis schema is missing");
}

export async function applyTrustedCatalogueSnapshot(node: any, snapshot: any): Promise<any> {
  try {
    node.database.ensureUsable();
  } catch (error) {
    throw node.catalogueActivationFailed ? nodeError("CatalogueActivationFailed") : error;
  }
  if (node.database.hasUnsettledPublications()) throw nodeError("Groove", "UnsettledPublications");
  const bootstrapUninitialized = node.catalogueBootstrapState === "Uninitialized";
  const plan = planTrustedCatalogueSnapshot(node, snapshot);
  const plannedGenesis = catalogueGenesis(plan.catalogue);
  const runtimeSemanticsChanged = !activeRuntimeLayoutsEqual(node.catalogue, plan.catalogue);
  let physicalIdentitiesChanged = false;
  for (const [schema, previous] of node.catalogue.physicalMappings) {
    const next = plan.catalogue.physicalMappings.get(schema);
    if (next && !deepEqual(previous.identities, next.identities)) physicalIdentitiesChanged = true;
  }
  const authorizationSourceChanged = !node.catalogue.activeSchema.sameAuthorizationSource(
    plan.catalogue.activeSchema,
    node.catalogue.physicalMappings.get(node.catalogue.activeSchema.schema),
    plan.catalogue.physicalMappings.get(plan.catalogue.activeSchema.schema),
  );
  const previousCatalogue = node.catalogue;
  node.catalogue = cloneCatalogue(plan.catalogue);
  const previousGenesis = catalogueGenesis(previousCatalogue);
  const checkpoint = runtimeSemanticsChanged
    ? undefined
    : node.database.runtimeRegistryCheckpoint();
  if (runtimeSemanticsChanged) {
    try {
      await node.rebuildDatabaseSlot();
    } catch {
      throw failCatalogueActivation(node, previousCatalogue, undefined);
    }
  } else {
    try {
      await node.synchronizePhysicalVersionTables();
    } catch {
      throw failCatalogueActivation(node, previousCatalogue, checkpoint);
    }
  }

  const batch = node.database.openBatch();
  if (previousGenesis !== plannedGenesis)
    deleteCatalogue(batch, CatalogueRecordKind.Genesis, previousGenesis);
  if (
    bootstrapUninitialized ||
    node.catalogueBootstrapMarker ||
    previousGenesis !== plannedGenesis
  ) {
    putCatalogue(batch, CatalogueRecordKind.Genesis, plannedGenesis, new Uint8Array());
    const ready = {
      genesis: plannedGenesis,
      currentWriteSchema: plan.catalogue.activeSchema.wirePointer(),
      activeCatalogueSeq: plan.catalogue.activeCatalogueSeq,
    };
    putCatalogue(
      batch,
      CatalogueRecordKind.BootstrapReady,
      ready.genesis,
      encodeCatalogueBootstrapReady(ready),
    );
  }
  for (const staged of plan.activatedLineages)
    NodeStatics.writeActiveSchemaLineageToBatch(batch, staged);
  for (const [, schema] of plan.catalogue.catalogueSchemas) {
    const prev = previousCatalogue.catalogueSchemas.get(schema.id);
    if (prev && deepEqual(prev, schema)) continue;
    putCatalogue(batch, CatalogueRecordKind.Schema, schema.id, encodeCatalogueSchema(schema));
  }
  for (const [schemaVersion, mapping] of plan.catalogue.physicalMappings) {
    const alias = plan.catalogue.schemaVersionAliases.get(schemaVersion);
    const prev = previousCatalogue.physicalMappings.get(schemaVersion);
    if (
      prev &&
      deepEqual(prev, mapping) &&
      previousCatalogue.schemaVersionAliases.get(schemaVersion) === alias
    )
      continue;
    NodeStatics.writeSchemaVersionMappingToBatch(batch, alias, schemaVersion, mapping);
  }
  NodeStatics.writeActiveSchemaToBatch(batch, plan.catalogue.activeSchema);
  try {
    const applied = await node.database.applyBatch(batch);
    const persisted = await node.database.persistWithProgress(applied);
    node.database.finishPersistence(persisted);
  } catch {
    throw failCatalogueActivation(node, previousCatalogue, checkpoint);
  }

  const c = node.catalogue;
  for (const cache of [
    c.stagedLineages,
    c.pendingLineages,
    c.lensPathCache,
    c.compiledLensCache,
    c.physicalWritePlanCache,
    c.physicalCurrentWinnerProjections,
    node.query.versionStorageSourcesCache,
    node.query.queryShapeCache,
    node.query.compiledQueryProgramCache,
    node.query.queryProgramTemplates,
    node.query.supportedQueryProgramRequests,
    node.query.readPolicyAuthorizationRequestCache,
    node.query.policyAuthorizationGraphCache,
    node.query.policyAuthorizationGraphReplacements,
  ])
    cache.clear();
  node.catalogueActivationFailed = false;
  node.catalogueBootstrapState = "Ready";
  node.catalogueBootstrapMarker ||= bootstrapUninitialized;
  if (physicalIdentitiesChanged) {
    node.invalidateSubscriptionScopes();
    node.physicalIdentityGeneration = nextGrooveRuntimeToken();
  }
  if (runtimeSemanticsChanged || authorizationSourceChanged)
    node.grooveRuntimeToken = nextGrooveRuntimeToken();
  const drained = await node.drainParkedCommitUnits();
  await node.drainParkedRelayCommitUnits();
  node.drainParkedShapeRegistrations();
  return {
    value: undefined,
    publications: drained.publications,
    postSettlementWork: drained.postSettlementWork,
  };
}

/** Validate a candidate deployment without installing or persisting it. */
export function validateDeploymentSnapshot(node: any, snapshot: any): void {
  planTrustedCatalogueSnapshot(node, snapshot);
}

function planTrustedCatalogueSnapshot(node: any, snapshot: any): PlannedCatalogueSnapshot {
  if (node.catalogue.pendingLineages.size || node.catalogue.stagedLineages.size)
    throw invalid("trusted catalogue snapshot conflicts with pending catalogue work");
  const genesisIdentities = snapshot.genesisPhysicalIdentities;
  const schemas = new SortedMap<string, any>(cmpStr);
  for (const schema of snapshot.schemas) {
    if (schema.id !== schema.schema.versionId())
      throw invalid("trusted catalogue snapshot schema id mismatch");
    if (schemas.set(schema.id, schema) !== undefined)
      throw invalid("trusted catalogue snapshot repeats a schema id");
  }
  const lineages: [number, any][] = [...snapshot.lineages].sort((a, b) => a[0] - b[0]);
  const lineageTargets = new Set<string>(lineages.map(([, p]) => p.schema.id));
  const genesisIds = schemas.keysArray().filter((id) => !lineageTargets.has(id));
  if (genesisIds.length !== 1)
    throw invalid("trusted catalogue snapshot must contain exactly one genesis schema");
  const genesisId = genesisIds[0];
  let bootstrapGenesis: any;
  if (node.catalogueBootstrapState === "Uninitialized") {
    bootstrapGenesis = schemas.get(genesisId);
    if (!bootstrapGenesis) throw invalid("trusted bootstrap snapshot omits genesis payload");
  }
  let anchor: [number, any] | undefined;
  if (node.catalogueBootstrapState !== "Uninitialized") {
    const local = node.catalogue.localSchemaVersionId;
    const alias = node.catalogue.schemaVersionAliases.get(local);
    const mapping = node.catalogue.physicalMappings.get(local);
    if (alias !== undefined && mapping) anchor = [alias, structuredClone(mapping)];
  }

  let planned: Catalogue;
  if (bootstrapGenesis) {
    const ids = { nextPhysicalTableId: 1, nextPhysicalColumnId: 1 };
    const mapping = allocateProvisionalPhysicalMapping(
      bootstrapGenesis.schema,
      genesisIdentities,
      ids,
    );
    const gid = bootstrapGenesis.id;
    planned = {
      localSchemaVersionId: gid,
      localSchemaVersionAlias: 1,
      schema: bootstrapGenesis.schema,
      schemaVersionAliases: new SortedMap(cmpStr, [[gid, 1]]),
      catalogueSchemas: new SortedMap(cmpStr, [[gid, bootstrapGenesis]]),
      catalogueLenses: new SortedMap(cmpStr),
      physicalMappings: new SortedMap(cmpStr, [[gid, mapping]]),
      physicalCurrentWinnerProjections: new SortedMap(cmpStr),
      stagedLineages: new SortedMap(cmpStr),
      pendingLineages: new SortedMap(cmpStr),
      activeLineagesByTarget: new SortedMap(cmpStr),
      activeCatalogueSeq: 0,
      nextPhysicalTableId: ids.nextPhysicalTableId,
      nextPhysicalColumnId: ids.nextPhysicalColumnId,
      lensPathCache: new Map(),
      compiledLensCache: new Map(),
      physicalWritePlanCache: new Map(),
      activeSchema: ActiveSchema.new({ revision: 0, schema: gid }, bootstrapGenesis.schema),
    };
  } else planned = cloneCatalogue(node.catalogue);
  const activatedLineages: any[] = [];
  for (const [, schema] of schemas) {
    if (lineageTargets.has(schema.id) || planned.catalogueSchemas.has(schema.id)) continue;
    if (schema.id !== genesisId)
      throw invalid("trusted catalogue snapshot schema has no identity publication");
    const mapping = allocateProvisionalPhysicalMapping(schema.schema, genesisIdentities, planned);
    const alias = nextSchemaVersionAliasInCatalogue(planned);
    planned.catalogueSchemas.set(schema.id, schema);
    planned.physicalMappings.set(schema.id, mapping);
    planned.schemaVersionAliases.set(schema.id, alias);
  }

  const genesisMapping = planned.physicalMappings.get(genesisId);
  const genesisRebindings = genesisMapping
    ? rebindProvisionalMappingIdentities(genesisMapping, structuredClone(genesisIdentities))
    : new Map<string, string>();
  for (const [schema, mapping] of planned.physicalMappings)
    if (schema !== genesisId) rebindMappingEnumCases(mapping, genesisRebindings);

  for (const [catalogueSeq, publication] of lineages) {
    try {
      NodeStatics.validateSchemaLineagePublication(publication);
    } catch {
      throw invalid("trusted catalogue snapshot contains invalid lineage identity");
    }
    const existing = planned.activeLineagesByTarget.get(publication.schema.id);
    if (existing) {
      if (existing.catalogueSeq !== catalogueSeq || !deepEqual(existing.publication, publication))
        throw invalid("trusted catalogue snapshot lineage conflicts with catalogue");
      continue;
    }
    if (catalogueSeq !== planned.activeCatalogueSeq + 1)
      throw invalid("trusted catalogue snapshot lineage sequence is not contiguous");
    NodeStatics.validatePublicationSources(
      publication,
      planned.catalogueSchemas,
      planned.physicalMappings,
      [...planned.physicalMappings].map(([, m]: any) => m.identities),
    );
    const fresh = allocateProvisionalPhysicalMapping(
      publication.schema.schema,
      publication.physicalIdentities,
      planned,
    );
    const mapping = NodeStatics.reconcilePublicationMapping(planned, publication, fresh);
    const staged = {
      catalogueSeq,
      publication,
      alias: nextSchemaVersionAliasInCatalogue(planned),
      mapping,
    };
    planned.catalogueSchemas.set(publication.schema.id, publication.schema);
    for (const predecessor of publication.predecessors)
      planned.catalogueLenses.set(predecessor.lens.id, predecessor.lens);
    planned.schemaVersionAliases.set(publication.schema.id, staged.alias);
    planned.physicalMappings.set(publication.schema.id, mapping);
    planned.activeLineagesByTarget.set(publication.schema.id, staged);
    planned.activeCatalogueSeq = catalogueSeq;
    activatedLineages.push(staged);
  }

  for (const [, schema] of schemas) {
    if (!planned.catalogueSchemas.has(schema.id))
      throw invalid("trusted catalogue snapshot schema has no lineage");
    planned.catalogueSchemas.set(schema.id, schema);
  }

  if (anchor) {
    const [localAlias, localMapping] = anchor;
    const anchorId = planned.localSchemaVersionId;
    const authorityMapping = planned.physicalMappings.get(anchorId);
    if (!authorityMapping)
      throw nodeInvalidStoredValue("snapshot authority mapping missing for local anchor");
    const authorityIdentities = structuredClone(authorityMapping.identities);
    const anchorSchema = planned.catalogueSchemas.get(anchorId);
    if (!anchorSchema)
      throw nodeInvalidStoredValue("snapshot authority schema missing for local anchor");
    const problem = authorityIdentities.validateForSchema?.(anchorSchema.schema);
    if (problem) throw invalid(problem);
    const anchorRebindings = rebindProvisionalMappingIdentities(localMapping, authorityIdentities);
    for (const [, mapping] of planned.physicalMappings)
      rebindMappingEnumCases(mapping, anchorRebindings);
    planned.schemaVersionAliases.set(anchorId, localAlias);
    planned.physicalMappings.set(anchorId, localMapping);

    const ancestors = new SortedSet<string>(cmpStr, [anchorId]);
    const byTarget = [...planned.activeLineagesByTarget].map(([, l]: any) => l);
    for (const lineage of [...byTarget].sort((a, b) => b.catalogueSeq - a.catalogueSeq)) {
      if (!ancestors.has(lineage.publication.schema.id)) continue;
      for (const predecessor of lineage.publication.predecessors) {
        const sourceSchema = planned.catalogueSchemas.get(predecessor.lens.source);
        if (!sourceSchema)
          throw nodeInvalidStoredValue(
            "snapshot source schema missing during local mapping reconciliation",
          );
        const targetSchema = planned.catalogueSchemas.get(predecessor.lens.target);
        if (!targetSchema)
          throw nodeInvalidStoredValue(
            "snapshot target schema missing during local mapping reconciliation",
          );
        const provisionalSource = planned.physicalMappings.get(predecessor.lens.source);
        if (!provisionalSource)
          throw nodeInvalidStoredValue(
            "snapshot source mapping missing during local mapping reconciliation",
          );
        const targetMapping = planned.physicalMappings.get(lineage.publication.schema.id);
        if (!targetMapping)
          throw nodeInvalidStoredValue(
            "snapshot target mapping missing during local mapping reconciliation",
          );
        const sourceMapping = NodeStatics.reconcileSourcePhysicalMappingForLensPayload(
          predecessor.lens,
          sourceSchema,
          targetSchema,
          provisionalSource,
          targetMapping,
        );
        planned.physicalMappings.set(predecessor.lens.source, sourceMapping);
        ancestors.add(predecessor.lens.source);
      }
    }
    for (const lineage of [...byTarget].sort((a, b) => a.catalogueSeq - b.catalogueSeq)) {
      const provisional = planned.physicalMappings.get(lineage.publication.schema.id);
      if (!provisional)
        throw nodeInvalidStoredValue(
          "snapshot target mapping missing during forward reconciliation",
        );
      planned.physicalMappings.set(
        lineage.publication.schema.id,
        NodeStatics.reconcilePublicationMapping(
          planned,
          lineage.publication,
          structuredClone(provisional),
        ),
      );
    }
    for (const lineage of [...byTarget, ...activatedLineages]) {
      lineage.alias = planned.schemaVersionAliases.get(lineage.publication.schema.id);
      lineage.mapping = structuredClone(
        planned.physicalMappings.get(lineage.publication.schema.id),
      );
    }
  }

  const cws = snapshot.currentWriteSchema;
  if (
    cws.revision < planned.activeSchema.revision ||
    (cws.revision === planned.activeSchema.revision &&
      !deepEqual(cws, planned.activeSchema.wirePointer())) ||
    !planned.catalogueSchemas.has(cws.schema)
  )
    throw invalid("trusted catalogue snapshot conflicts at write-schema revision");
  const active = ActiveSchema.new(cws, planned.catalogueSchemas.get(cws.schema).schema);
  if (
    active.revision > 0 &&
    active.revision === planned.activeSchema.revision &&
    !active.samePermissions(planned.activeSchema)
  )
    throw invalid("conflicting active schema revision");
  planned.activeSchema = active;
  if (active.revision > 0) {
    const structural = SchemaVersion.new(active.compiled.withoutPermissions());
    planned.catalogueSchemas.set(structural.id, structural);
  }
  const readSchema = planned.catalogueSchemas.get(planned.localSchemaVersionId);
  if (!readSchema) throw invalid("trusted catalogue snapshot omits the active read schema");
  planned.schema = readSchema.schema;
  if (planned.localSchemaVersionId === active.schema) planned.schema = active.compiled;
  planned.localSchemaVersionAlias = planned.schemaVersionAliases.get(planned.localSchemaVersionId);
  const authorityMappings = new SortedMap<string, any>(cmpStr);
  for (const [schema, mapping] of planned.physicalMappings)
    if (schemas.has(schema)) authorityMappings.set(schema, structuredClone(mapping));
  validateDurablePhysicalIdentityBindings(
    schemas,
    authorityMappings,
    planned.stagedLineages,
    planned.activeLineagesByTarget,
    planned.pendingLineages,
    genesisId,
    "Ready",
  );
  validateScalarEnumCaseProvenance(planned.physicalMappings, planned.schemaVersionAliases);
  validatePayloadEnumCaseProvenance(planned.physicalMappings, planned.schemaVersionAliases);
  return { catalogue: planned, activatedLineages };
}
