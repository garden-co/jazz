// Node construction, catalogue open, runtime accessors and large-value lifecycle.
import { SortedMap, SortedSet, cmpNum, cmpStr } from "../../../support/collections.js";
import { blake3 } from "../../../support/hash.js";
import { toHex } from "../../../support/bytes.js";
import { Writer } from "../../../support/postcard.js";
import {
  type AuthorSubject,
  type NodeUuid,
  type SchemaVersionId,
  type TxTime,
  type Value,
  CatalogueRecordKind,
  CatalogueRowRecord,
  Database,
  EnumValue,
  JazzSchema,
  LargeValueKind,
  OwnedRecord,
  PhysicalIdentityManifest,
  SchemaVersion,
  SchemaVersionAliasRowRecord,
  StorageLayout,
  TxTimeOps,
  UnavailableChunkResolver,
  UUID_NIL,
  allocateProvisionalPhysicalMapping,
  authorSubjectKey,
  authorityResultKeyString,
  catalogueWritePointerId,
  decodeActiveSchema,
  decodeCatalogueBootstrapReady,
  decodeCatalogueLens,
  decodeCatalogueLineageActivation,
  decodeCataloguePendingLineage,
  decodeCatalogueSchema,
  decodeCatalogueStagedLineage,
  decodeCatalogueWritePointer,
  decodePhysicalMapping,
  encodeAuthorSubject,
  encodeCatalogueBootstrapReady,
  encodeCatalogueSchema,
  encodeValue,
  grooveVariantTag,
  physicalStorageValueType,
  physicalVersionStorageTables,
  schemaIdKey,
  uuidEquals,
  validatePayloadEnumCaseProvenance,
  validatePhysicalMappingRegistries,
  validatePhysicalVariantCases,
  validateScalarEnumCaseProvenance,
  valueEquals,
} from "../../../_extern/node-state.js";
import {
  type CatalogueBootstrapReady,
  type CatalogueOpenState,
  type StagedSchemaLineage,
  ActiveSchema,
  CatalogueBootstrapState,
  NodeState,
  SchemaCatalogue,
  V,
  cloneParking,
  invalidStored,
  nextGrooveRuntimeToken,
  nodeErr,
  versionIndirectDescriptors,
} from "../index.js";
import { DatabaseSlot } from "../database_slot.js";

const META = "jazz_catalogue";
const kindKey = (k: any) => V("U64", CatalogueRecordKind.key(k));

/** Restores the previous active claims when the scoped operation ends. */
export class ActiveSessionClaimsScope {
  constructor(
    readonly node: NodeState,
    private previous: [AuthorSubject, Map<string, Value>] | undefined,
  ) {}
  release(): void {
    this.node.activeSessionClaims = this.previous;
    this.previous = undefined;
  }
}

// ---------------------------------------------------------------- constructors

/** Open or create a node over the supplied storage. */
export const newNode = (nodeUuid: NodeUuid, schema: any, storage: any) =>
  newWithOptionsInner(nodeUuid, schema, storage, false, CatalogueBootstrapState.Ready);

export const newHistoryComplete = (nodeUuid: NodeUuid, schema: any, storage: any) =>
  newWithOptionsInner(nodeUuid, schema, storage, true, CatalogueBootstrapState.Ready);

/** Recover a partial client replica using a schema already admitted by its durable catalogue. */
export async function newClient(
  nodeUuid: NodeUuid,
  requestedSchema: any,
  storage: any,
  historyComplete: boolean,
) {
  const meta = await Database.newWithStorageLayout(
    JazzSchema.empty().lowerCatalogueMetaToGroove(),
    storage,
    StorageLayout.jazzClassV2(),
  );
  const requestedKey = [
    kindKey(CatalogueRecordKind.Schema),
    V("Uuid", requestedSchema.versionId()),
  ];
  let schema = requestedSchema;
  if ((await meta.primaryKeyGetRaw(META, requestedKey)) === undefined) {
    const genesisRows = await meta.primaryKeyScanRaw(META, [kindKey(CatalogueRecordKind.Genesis)]);
    if (genesisRows.length === 1) {
      const id = genesisRows[0].record().getUuid(CatalogueRowRecord.FIELD_ID_IDX);
      const raw = await meta.primaryKeyGetRaw(META, [
        kindKey(CatalogueRecordKind.Schema),
        V("Uuid", id),
      ]);
      if (!raw) throw invalidStored("durable genesis schema is missing");
      const decoded = decodeCatalogueSchema(
        raw.record().getBytes(CatalogueRowRecord.FIELD_PAYLOAD_IDX),
      );
      if (!uuidEquals(decoded.id, id)) throw invalidStored("catalogue schema id mismatch");
      schema = decoded.schema;
    } else if (genesisRows.length > 1) {
      throw invalidStored("duplicate catalogue genesis marker");
    }
  }
  return newWithOptionsInner(
    nodeUuid,
    schema,
    meta.intoStorage(),
    historyComplete,
    CatalogueBootstrapState.Ready,
  );
}

/** Open a downstream runtime before it has received an authenticated authority catalogue. */
export async function newCatalogueUninitialized(nodeUuid: NodeUuid, storage: any) {
  const [boxed, durableGenesis] = await discoverDurableCatalogueGenesis(storage);
  if (durableGenesis)
    return newWithOptionsInner(
      nodeUuid,
      durableGenesis,
      boxed,
      false,
      CatalogueBootstrapState.Ready,
    );
  return newWithOptionsInner(
    nodeUuid,
    JazzSchema.empty(),
    boxed,
    false,
    CatalogueBootstrapState.Uninitialized,
  );
}

/** Discover an already durable authority genesis from the fixed catalogue layout. */
async function discoverDurableCatalogueGenesis(storage: any): Promise<[any, any | undefined]> {
  const meta = await Database.newWithStorageLayout(
    JazzSchema.empty().lowerToGroove(),
    storage,
    StorageLayout.jazzClassV2(),
  );
  let genesis: SchemaVersionId | undefined;
  const schemas = new Map<string, any>();
  let bootstrapReady: CatalogueBootstrapReady | undefined;
  let activeSelection: any;
  const stagedLineages = new Map<string, any>();
  const activeLineages = new Map<string, number>();
  let hasCatalogueResidue = false;
  for (const raw of await meta.primaryKeyScanRaw(META, [])) {
    hasCatalogueResidue = true;
    const record = raw.record();
    const id = () => record.getUuid(CatalogueRowRecord.FIELD_ID_IDX);
    const payload = () => record.getBytes(CatalogueRowRecord.FIELD_PAYLOAD_IDX);
    switch (CatalogueRecordKind.fromKey(record.getU64(CatalogueRowRecord.FIELD_KIND_IDX))) {
      case CatalogueRecordKind.Genesis:
        if (genesis !== undefined) throw invalidStored("duplicate catalogue genesis marker");
        genesis = id();
        break;
      case CatalogueRecordKind.ActiveSchema:
        if (!uuidEquals(id(), UUID_NIL) || activeSelection)
          throw invalidStored("invalid active schema record identity");
        activeSelection = decodeActiveSchema(payload());
        break;
      case CatalogueRecordKind.Schema: {
        const schema = decodeCatalogueSchema(payload());
        const key = schemaIdKey(schema.id);
        if (!uuidEquals(schema.id, id()) || schemas.has(key))
          throw invalidStored("catalogue schema id mismatch");
        schemas.set(key, schema);
        break;
      }
      case CatalogueRecordKind.SchemaLineageActive: {
        const active = decodeCatalogueLineageActivation(payload());
        const key = schemaIdKey(active.id);
        if (!uuidEquals(active.id, id()) || active.catalogueSeq === 0 || activeLineages.has(key))
          throw invalidStored("catalogue bootstrap active lineage marker is malformed");
        activeLineages.set(key, active.catalogueSeq);
        break;
      }
      case CatalogueRecordKind.SchemaLineageStaged: {
        const staged = decodeCatalogueStagedLineage(payload());
        const key = schemaIdKey(staged.publication.id);
        if (
          !uuidEquals(staged.publication.id, id()) ||
          staged.catalogueSeq === 0 ||
          stagedLineages.has(key)
        )
          throw invalidStored("catalogue bootstrap staged lineage marker is malformed");
        stagedLineages.set(key, staged);
        break;
      }
      case CatalogueRecordKind.BootstrapReady: {
        const ready = decodeCatalogueBootstrapReady(payload());
        if (!uuidEquals(ready.genesis, id()) || bootstrapReady)
          throw invalidStored("duplicate or malformed catalogue bootstrap marker");
        bootstrapReady = ready;
        break;
      }
    }
  }
  const mappingIds = new Set<string>(
    (await meta.primaryKeyScanRaw("jazz_schema_versions", [])).map((raw: any) =>
      schemaIdKey(raw.record().getUuid(SchemaVersionAliasRowRecord.FIELD_UUID_IDX)),
    ),
  );
  const durablePointer = activeSelection?.wirePointer();
  let hasNonCatalogueResidue = false;
  for (const table of ["jazz_transactions", "jazz_rejected_transactions"]) {
    if ((await meta.primaryKeyScanRaw(table, [])).length) {
      hasNonCatalogueResidue = true;
      break;
    }
  }
  const hasResidue =
    hasCatalogueResidue ||
    mappingIds.size > 0 ||
    durablePointer !== undefined ||
    hasNonCatalogueResidue;
  if (!bootstrapReady) {
    if (!hasResidue) return [meta.intoStorage(), undefined];
    if (hasNonCatalogueResidue)
      throw invalidStored("dynamic catalogue state cannot initialize over durable history");
    throw invalidStored("dynamic catalogue state has no bootstrap completion marker");
  }
  const ready = bootstrapReady;
  const activeTargets = new Set<string>();
  const activeSequences = new SortedSet<number>(cmpNum);
  for (const staged of stagedLineages.values()) {
    validateDurableStagedLineage(staged, schemas);
    const pubKey = schemaIdKey(staged.publication.id);
    const targetKey = schemaIdKey(staged.publication.schema.id);
    const sequence = activeLineages.get(pubKey);
    activeLineages.delete(pubKey);
    if (sequence !== undefined && sequence === staged.catalogueSeq) {
      const schema = schemas.get(targetKey);
      if (
        !schema ||
        !schema.schema.hasSameLineageStructure(staged.publication.schema.schema) ||
        activeTargets.has(targetKey)
      )
        throw invalidStored("catalogue bootstrap active lineage does not own its schema");
      activeTargets.add(targetKey);
      if (activeSequences.has(sequence))
        throw invalidStored("catalogue bootstrap active lineage marker is malformed");
      activeSequences.add(sequence);
    } else if (sequence !== undefined) {
      throw invalidStored("catalogue bootstrap active lineage marker conflicts with payload");
    } else if (schemas.has(targetKey) || mappingIds.has(targetKey)) {
      throw invalidStored("catalogue bootstrap inactive lineage has durable target state");
    }
  }
  if (activeLineages.size)
    throw invalidStored("catalogue bootstrap active lineage is missing canonical payload");
  const expected = new Set([schemaIdKey(ready.genesis), ...activeTargets]);
  const sameSet = (a: Set<string>) =>
    a.size === expected.size && [...a].every((k) => expected.has(k));
  const seqs = activeSequences.toArray();
  if (
    genesis === undefined ||
    !uuidEquals(genesis, ready.genesis) ||
    durablePointer === undefined ||
    !durablePointer.equals(ready.currentWriteSchema) ||
    !schemas.has(schemaIdKey(ready.genesis)) ||
    !sameSet(new Set(schemas.keys())) ||
    !sameSet(mappingIds) ||
    seqs.length !== ready.activeCatalogueSeq ||
    seqs.some((s, i) => s !== i + 1)
  )
    throw invalidStored("catalogue bootstrap completion marker does not match durable catalogue");
  return [meta.intoStorage(), schemas.get(schemaIdKey(ready.genesis)).schema];
}

/** Choose the schema a server runtime reopens an existing store with. */
export async function selectDurableReopenSchema(storage: any, requested: any): Promise<[any, any]> {
  const meta = await Database.newWithStorageLayout(
    JazzSchema.empty().lowerCatalogueMetaToGroove(),
    storage,
    StorageLayout.jazzClassV2(),
  );
  let genesis: SchemaVersionId | undefined;
  let active: SchemaVersionId | undefined;
  const schemas = new Map<string, any>();
  for (const raw of await meta.primaryKeyScanRaw(META, [])) {
    const record = raw.record();
    switch (CatalogueRecordKind.fromKey(record.getU64(CatalogueRowRecord.FIELD_KIND_IDX))) {
      case CatalogueRecordKind.Genesis:
        genesis = record.getUuid(CatalogueRowRecord.FIELD_ID_IDX);
        break;
      case CatalogueRecordKind.ActiveSchema:
        active = decodeActiveSchema(record.getBytes(CatalogueRowRecord.FIELD_PAYLOAD_IDX)).schema;
        break;
      case CatalogueRecordKind.Schema: {
        const schema = decodeCatalogueSchema(record.getBytes(CatalogueRowRecord.FIELD_PAYLOAD_IDX));
        schemas.set(schemaIdKey(schema.id), schema.schema);
        break;
      }
    }
  }
  const out = meta.intoStorage();
  if (genesis === undefined || schemas.has(schemaIdKey(requested.versionId())))
    return [out, requested];
  for (const id of [active, genesis])
    if (id !== undefined && schemas.has(schemaIdKey(id)))
      return [out, schemas.get(schemaIdKey(id))];
  return [out, requested];
}

export async function newWithOptionsInner(
  nodeUuid: NodeUuid,
  schema: any,
  storage: any,
  historyComplete: boolean,
  bootstrapState: CatalogueBootstrapState,
): Promise<NodeState> {
  const localId = schema.versionId();
  const localKey = schemaIdKey(localId);
  const opened: CatalogueOpenState = await openCatalogueStage(schema, storage, bootstrapState);
  const {
    schemas,
    lenses,
    schemaVersionAliases,
    physicalMappings,
    stagedLineages,
    activeLineagesByTarget,
  } = opened;
  let activeCatalogueSeq = opened.activeCatalogueSeq;
  const selection = opened.activeSchema;
  const regSchemas = new Map(schemas);
  const regAliases = new Map(schemaVersionAliases);
  const regMappings = new Map(physicalMappings);
  for (const staged of stagedLineages.valuesArray()) {
    const key = schemaIdKey(staged.publication.schema.id);
    regSchemas.set(key, staged.publication.schema);
    regAliases.set(key, staged.alias);
    regMappings.set(key, staged.mapping);
  }
  const database = await openFullDatabase(
    schema,
    regSchemas,
    regAliases,
    regMappings,
    opened.storage,
  );
  for (;;) {
    const next = activeCatalogueSeq + 1;
    const staged = stagedLineages.get(next);
    if (!staged) break;
    if (staged.publication.predecessors.some((p: any) => !schemas.has(schemaIdKey(p.lens.source))))
      break;
    const batch = database.openBatch();
    NodeState.prototype.writeActiveSchemaLineageToBatch(batch, staged);
    if (opened.catalogueBootstrapMarker) {
      const ready: CatalogueBootstrapReady = {
        genesis: localId,
        currentWriteSchema: selection.wirePointer(),
        activeCatalogueSeq: next,
      };
      batch.update(META, [
        kindKey(CatalogueRecordKind.BootstrapReady),
        V("Uuid", ready.genesis),
        V("Bytes", encodeCatalogueBootstrapReady(ready)),
      ]);
    }
    const applied = await database.applyBatch(batch);
    database.finishPersistence(await database.persistWithProgress(applied));
    const target = schemaIdKey(staged.publication.schema.id);
    schemas.set(target, staged.publication.schema);
    for (const p of staged.publication.predecessors) lenses.set(schemaIdKey(p.lens.id), p.lens);
    schemaVersionAliases.set(target, staged.alias);
    physicalMappings.set(target, staged.mapping);
    activeLineagesByTarget.set(target, staged);
    stagedLineages.delete(next);
    activeCatalogueSeq = next;
  }
  // Retain the durable payload learned from the authority instead of the caller's structural schema.
  let activeSchema = schemas.get(localKey)?.schema ?? schema;
  if (uuidEquals(selection.schema, localId)) activeSchema = selection.compiled;
  const chunkResolver = new UnavailableChunkResolver();
  database.setMissingChunkResolver(chunkResolver);
  const catalogue = new SchemaCatalogue(
    localId,
    activeSchema,
    schemaVersionAliases,
    schemas,
    lenses,
    physicalMappings,
    stagedLineages,
    opened.pendingLineages,
    activeLineagesByTarget,
    activeCatalogueSeq,
    opened.nextPhysicalTableId,
    opened.nextPhysicalColumnId,
    selection,
  );
  catalogue.localSchemaVersionAlias = schemaVersionAliases.get(localKey);
  const node = new NodeState(
    nodeUuid,
    catalogue,
    bootstrapState,
    new DatabaseSlot(database),
    historyComplete,
  );
  node.catalogueBootstrapMarker = opened.catalogueBootstrapMarker;
  node.chunkResolver = chunkResolver;
  node.contentRuntimeProvider = database.ownedChunkProvider();
  node.localChunkReader = database.localChunkReader();
  for (const version of [...node.catalogue.catalogueSchemas.values()])
    await node.ensureProvisionalPhysicalMapping(version.id);
  await node.synchronizePhysicalVersionTables();
  await node.database.ensureDeclaredIndexGeneration(1);
  await node.recoverPendingSchemaLineages();
  await node.recoverFromStorage();
  await node.recoverLocalAvailabilityRecords();
  await node.rebuildAheadCurrentKeys();
  node.selfNodeAlias = await node.ensureNodeAlias(nodeUuid);
  node.catalogue.localSchemaVersionAlias = await node.ensureSchemaVersionAlias(localId);
  return node;
}

async function openFullDatabase(
  schema: any,
  catalogueSchemas: Map<string, any>,
  aliases: Map<string, number>,
  mappings: Map<string, any>,
  storage: any,
): Promise<any> {
  const lowered = schema.lowerToGroove();
  lowered.tables.push(...physicalVersionStorageTables(catalogueSchemas, aliases, mappings));
  const database = await Database.newWithStorageLayout(
    lowered,
    storage,
    StorageLayout.jazzClassV2(),
  );
  // Jazz never reads generic root positions of plain ordered results (#2086).
  database.setPlainOutputRootPositionsEnabled(false);
  return database;
}

const NP = () => NodeState.prototype as any;
const nowMsU64 = () => Date.now();

async function openCatalogueStage(
  schema: any,
  storage: any,
  bootstrapState: CatalogueBootstrapState,
): Promise<CatalogueOpenState> {
  const localId = schema.versionId();
  const localKey = schemaIdKey(localId);
  const meta = await Database.newWithStorageLayout(
    schema.lowerCatalogueMetaToGroove(),
    storage,
    StorageLayout.jazzClassV2(),
  );
  let recoveredActive: any;
  const catalogueSchemas = new Map<string, any>();
  const catalogueLenses = new Map<string, any>();
  const stagedById = new Map<string, StagedSchemaLineage>();
  const pendingLineages = new SortedMap<number, any>(cmpNum);
  const activeById = new Map<string, any>();
  const pendingWritePointers = new SortedMap<number, any>(cmpNum);
  let genesisSchema: SchemaVersionId | undefined;
  let bootstrapReady: CatalogueBootstrapReady | undefined;
  for (const raw of await meta.primaryKeyScanRaw(META, [])) {
    const record = raw.record();
    const id = () => record.getUuid(CatalogueRowRecord.FIELD_ID_IDX);
    const payload = () => record.getBytes(CatalogueRowRecord.FIELD_PAYLOAD_IDX);
    switch (CatalogueRecordKind.fromKey(record.getU64(CatalogueRowRecord.FIELD_KIND_IDX))) {
      case CatalogueRecordKind.ActiveSchema:
        if (!uuidEquals(id(), UUID_NIL) || recoveredActive)
          throw invalidStored("invalid active schema record identity");
        recoveredActive = decodeActiveSchema(payload());
        break;
      case CatalogueRecordKind.Schema: {
        const version = decodeCatalogueSchema(payload());
        if (!uuidEquals(version.id, id()) || !uuidEquals(version.id, version.schema.versionId()))
          throw invalidStored("catalogue schema id does not match schema payload");
        catalogueSchemas.set(schemaIdKey(version.id), version);
        break;
      }
      case CatalogueRecordKind.Lens: {
        const lens = decodeCatalogueLens(payload());
        if (!uuidEquals(lens.id, id()) || !uuidEquals(lens.id, lens.contentId()))
          throw invalidStored("catalogue lens id does not match lens payload");
        catalogueLenses.set(schemaIdKey(lens.id), lens);
        break;
      }
      case CatalogueRecordKind.SchemaLineageStaged: {
        const staged = decodeCatalogueStagedLineage(payload());
        if (
          !uuidEquals(staged.publication.id, id()) ||
          !uuidEquals(staged.publication.id, staged.publication.contentId())
        )
          throw invalidStored("staged schema lineage id mismatch");
        stagedById.set(schemaIdKey(staged.publication.id), staged);
        break;
      }
      case CatalogueRecordKind.SchemaLineagePending: {
        const pending = decodeCataloguePendingLineage(payload());
        if (
          !uuidEquals(pending.publication.id, id()) ||
          !uuidEquals(pending.publication.id, pending.publication.contentId()) ||
          pendingLineages.has(pending.catalogueSeq)
        )
          throw invalidStored("pending schema lineage identity conflict");
        pendingLineages.set(pending.catalogueSeq, pending);
        break;
      }
      case CatalogueRecordKind.SchemaLineageActive: {
        const active = decodeCatalogueLineageActivation(payload());
        const key = schemaIdKey(active.id);
        if (!uuidEquals(active.id, id()) || active.catalogueSeq === 0 || activeById.has(key))
          throw invalidStored("active schema lineage id mismatch");
        activeById.set(key, active);
        break;
      }
      case CatalogueRecordKind.WritePointerPending: {
        const pointer = decodeCatalogueWritePointer(payload());
        if (!uuidEquals(id(), catalogueWritePointerId(pointer)))
          throw invalidStored("pending catalogue write-pointer id mismatch");
        if (pendingWritePointers.has(pointer.revision))
          throw invalidStored("duplicate pending catalogue write-pointer revision");
        pendingWritePointers.set(pointer.revision, pointer);
        break;
      }
      case CatalogueRecordKind.Genesis:
        if (genesisSchema !== undefined) throw invalidStored("duplicate catalogue genesis marker");
        genesisSchema = id();
        break;
      case CatalogueRecordKind.BootstrapReady: {
        const ready = decodeCatalogueBootstrapReady(payload());
        if (!uuidEquals(ready.genesis, id()) || bootstrapReady)
          throw invalidStored("duplicate or malformed catalogue bootstrap marker");
        bootstrapReady = ready;
        break;
      }
    }
  }
  // A standalone PublishLens must be validated against its exact durable endpoints.
  for (const lens of catalogueLenses.values()) {
    const source = catalogueSchemas.get(schemaIdKey(lens.source));
    if (!source) throw invalidStored("catalogue lens source schema is missing");
    const target = catalogueSchemas.get(schemaIdKey(lens.target));
    if (!target) throw invalidStored("catalogue lens target schema is missing");
    try {
      NP().validateMigrationLensBetween(lens, source, target);
    } catch {
      throw invalidStored("catalogue lens violates trusted semantic invariants");
    }
  }
  const stagedLineages = new SortedMap<number, StagedSchemaLineage>(cmpNum);
  const activeByTarget = new Map<string, StagedSchemaLineage>();
  const lineageTargets = new Set<string>();
  const activeBySeq = new SortedMap<number, any>(cmpNum);
  for (const active of activeById.values()) {
    if (activeBySeq.has(active.catalogueSeq))
      throw invalidStored("duplicate active catalogue sequence");
    activeBySeq.set(active.catalogueSeq, active.id);
  }
  for (const staged of stagedById.values()) {
    validateDurableStagedLineage(staged, catalogueSchemas);
    if (staged.catalogueSeq === 0)
      throw invalidStored("staged schema lineage sequence must be nonzero");
    const target = schemaIdKey(staged.publication.schema.id);
    if (lineageTargets.has(target)) throw invalidStored("duplicate durable schema lineage target");
    lineageTargets.add(target);
    const pubKey = schemaIdKey(staged.publication.id);
    const active = activeById.get(pubKey);
    if (active) {
      activeById.delete(pubKey);
      if (active.catalogueSeq !== staged.catalogueSeq)
        throw invalidStored("active schema lineage payload conflicts with marker");
      activeByTarget.set(target, staged);
    } else if (activeBySeq.has(staged.catalogueSeq)) {
      throw invalidStored("staged catalogue sequence conflicts with active lineage");
    } else if (stagedLineages.has(staged.catalogueSeq)) {
      throw invalidStored("duplicate staged catalogue sequence");
    } else stagedLineages.set(staged.catalogueSeq, staged);
  }
  if (activeById.size) throw invalidStored("active schema lineage is missing canonical payload");
  activeBySeq.keysArray().forEach((actual, i) => {
    if (actual !== i + 1) throw invalidStored("active catalogue sequences are not contiguous");
  });
  const activeCatalogueSeq = activeBySeq.last()?.[0] ?? 0;
  const aliases = new Map<string, number>();
  const versionForAlias = new Map<number, SchemaVersionId>();
  const mappings = new Map<string, any>();
  for (const raw of await meta.primaryKeyScanRaw("jazz_schema_versions", [])) {
    const record = raw.record();
    const mapping = decodePhysicalMapping(
      record.getBytes(SchemaVersionAliasRowRecord.FIELD_PHYSICAL_MAPPING_IDX),
    );
    const version = record.getUuid(SchemaVersionAliasRowRecord.FIELD_UUID_IDX);
    const key = schemaIdKey(version);
    const alias = Number(record.getU64(SchemaVersionAliasRowRecord.FIELD_ID_IDX));
    // Schema aliases become Groove variant tags; check the narrowing boundary on recovery.
    grooveVariantTag(alias);
    const existing = aliases.get(key);
    if (existing !== undefined && existing !== alias)
      throw invalidStored("schema version has conflicting durable aliases");
    const existingVersion = versionForAlias.get(alias);
    if (existingVersion !== undefined && !uuidEquals(existingVersion, version))
      throw invalidStored("schema alias maps to multiple durable schema versions");
    aliases.set(key, alias);
    versionForAlias.set(alias, version);
    mappings.set(key, mapping);
  }
  validatePhysicalVariantCases(mappings, aliases);
  validatePhysicalMappingRegistries(mappings, aliases);
  const ids = { table: 1, column: 1 };
  const bump = (mapping: any) => {
    for (const table of mapping.tables.values()) {
      ids.table = Math.max(ids.table, table.tableId + 1);
      for (const column of table.columns.values()) ids.column = Math.max(ids.column, column + 1);
    }
  };
  for (const mapping of mappings.values()) bump(mapping);
  for (const staged of stagedLineages.valuesArray()) bump(staged.mapping);
  if (genesisSchema !== undefined && !catalogueSchemas.has(localKey))
    throw invalidStored("opened schema is absent from the durable catalogue");
  if (genesisSchema === undefined && catalogueSchemas.size)
    throw invalidStored("catalogue schemas exist without a genesis marker");
  if (
    bootstrapState === CatalogueBootstrapState.Uninitialized &&
    (genesisSchema !== undefined ||
      catalogueSchemas.size ||
      catalogueLenses.size ||
      mappings.size ||
      aliases.size ||
      activeCatalogueSeq !== 0 ||
      stagedLineages.size ||
      pendingLineages.size ||
      activeByTarget.size ||
      pendingWritePointers.size ||
      recoveredActive)
  )
    throw invalidStored("uninitialized catalogue open requires empty durable catalogue state");
  let activeSchema: any;
  if (recoveredActive) {
    if (!catalogueSchemas.has(schemaIdKey(recoveredActive.schema)))
      throw invalidStored("active schema references an unknown structural schema");
    activeSchema = recoveredActive;
  } else if (genesisSchema !== undefined) {
    throw invalidStored("catalogue active schema missing");
  } else {
    activeSchema = ActiveSchema.create({ revision: 0, schema: localId }, schema);
  }
  const hadCurrentSchema = catalogueSchemas.has(localKey);
  if (!hadCurrentSchema) catalogueSchemas.set(localKey, SchemaVersion.new(schema));
  if (!mappings.has(localKey) || !aliases.has(localKey)) {
    const mapping =
      mappings.get(localKey) ??
      allocateProvisionalPhysicalMapping(schema, PhysicalIdentityManifest.allocate(schema), ids);
    let alias = aliases.get(localKey);
    if (alias === undefined) {
      alias = Math.max(0, ...aliases.values()) + 1;
      if (!Number.isSafeInteger(alias)) throw invalidStored("schema version alias exhausted");
    }
    if (bootstrapState === CatalogueBootstrapState.Ready) {
      const batch = meta.openBatch();
      if (genesisSchema === undefined)
        batch.update(META, [
          kindKey(CatalogueRecordKind.Genesis),
          V("Uuid", localId),
          V("Bytes", new Uint8Array()),
        ]);
      if (!hadCurrentSchema)
        batch.update(META, [
          kindKey(CatalogueRecordKind.Schema),
          V("Uuid", localId),
          V("Bytes", encodeCatalogueSchema(SchemaVersion.new(schema))),
        ]);
      NP().writeSchemaVersionMappingToBatch(batch, alias, localId, mapping);
      if (genesisSchema === undefined) NP().writeActiveSchemaToBatch(batch, activeSchema);
      const applied = await meta.applyBatch(batch);
      meta.finishPersistence(await meta.persistWithProgress(applied));
    }
    aliases.set(localKey, alias);
    mappings.set(localKey, mapping);
  }
  validateDurablePhysicalIdentityBindings(
    catalogueSchemas,
    mappings,
    stagedLineages,
    activeByTarget,
    pendingLineages,
    genesisSchema,
    bootstrapState,
  );
  // Descriptor rebuilding sorts payload cases by their durable introduction provenance.
  validateScalarEnumCaseProvenance(mappings, aliases);
  validatePayloadEnumCaseProvenance(mappings, aliases);
  return {
    storage: meta.intoStorage(),
    schemas: catalogueSchemas,
    lenses: catalogueLenses,
    schemaVersionAliases: aliases,
    physicalMappings: mappings,
    stagedLineages,
    pendingLineages,
    activeLineagesByTarget: activeByTarget,
    activeCatalogueSeq,
    nextPhysicalTableId: ids.table,
    nextPhysicalColumnId: ids.column,
    catalogueBootstrapMarker: bootstrapReady !== undefined,
    activeSchema,
  };
}

function validateDurableStagedLineage(
  staged: StagedSchemaLineage,
  schemas: Map<string, any>,
): void {
  try {
    NP().validateSchemaLineagePublication(staged.publication);
  } catch {
    throw invalidStored("staged schema lineage violates trusted publication invariants");
  }
  for (const predecessor of staged.publication.predecessors) {
    const source = schemas.get(schemaIdKey(predecessor.lens.source));
    if (!source) throw invalidStored("staged schema lineage source is missing");
    try {
      NP().validateMigrationLensBetween(predecessor.lens, source, staged.publication.schema);
    } catch {
      throw invalidStored("staged schema lineage lens is invalid");
    }
    try {
      NP().validateLineageTablePartition(
        source.schema,
        staged.publication.schema.schema,
        predecessor.lens,
        predecessor.newTables,
        predecessor.droppedTables,
      );
    } catch {
      throw invalidStored("staged schema lineage table partition is invalid");
    }
  }
}

/** Bind every recovered local alias mapping to the one authority manifest that published its schema. */
function validateDurablePhysicalIdentityBindings(
  schemas: Map<string, any>,
  mappings: Map<string, any>,
  staged: SortedMap<number, StagedSchemaLineage>,
  active: Map<string, StagedSchemaLineage>,
  pending: SortedMap<number, any>,
  genesis: SchemaVersionId | undefined,
  bootstrapState: CatalogueBootstrapState,
): void {
  if (bootstrapState === CatalogueBootstrapState.Uninitialized) return;
  if (mappings.size !== schemas.size)
    throw invalidStored("durable schema and physical mapping coverage differs");
  for (const [key, schema] of schemas) {
    const mapping = mappings.get(key);
    if (!mapping) throw invalidStored("durable schema physical mapping is missing");
    try {
      mapping.identities.validateForSchema(schema.schema);
    } catch {
      throw invalidStored("durable physical identity manifest is invalid");
    }
  }
  const lineages = [...active.values(), ...staged.valuesArray()];
  const published = new Map<string, any>();
  for (const lineage of lineages) {
    const key = schemaIdKey(lineage.publication.schema.id);
    if (published.has(key))
      throw invalidStored("durable schema has multiple identity publications");
    published.set(key, lineage.publication);
  }
  const sequences = new Map(
    lineages.map((l) => [schemaIdKey(l.publication.schema.id), l.catalogueSeq]),
  );
  for (const lineage of lineages)
    for (const p of lineage.publication.predecessors) {
      const seq = sequences.get(schemaIdKey(p.lens.source));
      if (seq !== undefined && seq >= lineage.catalogueSeq)
        throw invalidStored("schema predecessor must precede its target");
    }
  const roots = [...schemas.keys()].filter((k) => !published.has(k));
  if (roots.length !== 1)
    throw invalidStored("durable catalogue must have exactly one genesis schema");
  const root = roots[0];
  if (genesis !== undefined && schemaIdKey(genesis) !== root)
    throw invalidStored("durable genesis marker disagrees with lineage root");
  if (published.has(root))
    throw invalidStored("durable genesis is incorrectly published as a lineage target");
  // Parked descendants may legally retain an identity first minted by the bundle being recovered.
  const parents = new Map<string, string[]>();
  for (const l of [...lineages, ...pending.valuesArray()])
    parents.set(
      schemaIdKey(l.publication.schema.id),
      l.publication.predecessors.map((p: any) => schemaIdKey(p.lens.source)),
    );
  const historyFor = (candidate: string, omit: any) => {
    const isDescendant = (schema: string) => {
      const remaining = [schema];
      const visited = new Set<string>();
      while (remaining.length) {
        const cursor = remaining.pop()!;
        if (visited.has(cursor)) continue;
        visited.add(cursor);
        const sources = parents.get(cursor);
        if (sources) {
          if (sources.includes(candidate)) return true;
          remaining.push(...sources);
        }
      }
      return false;
    };
    const others = (ls: readonly any[]) =>
      ls
        .filter(
          (l) =>
            !uuidEquals(l.publication.id, omit) &&
            !isDescendant(schemaIdKey(l.publication.schema.id)),
        )
        .map((l) => l.publication.physicalIdentities);
    return [
      ...[...mappings]
        .filter(([s]) => s !== candidate && !isDescendant(s))
        .map(([, m]) => m.identities),
      ...others(staged.valuesArray()),
      ...others(pending.valuesArray()),
    ];
  };
  for (const [key, mapping] of mappings) {
    if (key === root) continue;
    const publication = published.get(key);
    if (!publication) throw invalidStored("durable non-genesis schema has no identity publication");
    if (!mapping.identities.equals(publication.physicalIdentities))
      throw invalidStored("durable mapping identities disagree with authority publication");
    try {
      NP().validatePublicationSources(
        publication,
        schemas,
        mappings,
        historyFor(key, publication.id),
      );
    } catch {
      throw invalidStored("durable identity publication evolution is invalid");
    }
  }
  for (const lineage of pending.valuesArray()) {
    try {
      NP().validateSchemaLineagePublication(lineage.publication);
    } catch {
      throw invalidStored("pending schema lineage violates trusted publication invariants");
    }
    if (lineage.publication.predecessors.some((p: any) => !schemas.has(schemaIdKey(p.lens.source))))
      continue;
    try {
      NP().validatePublicationSources(
        lineage.publication,
        schemas,
        mappings,
        historyFor(schemaIdKey(lineage.publication.schema.id), lineage.publication.id),
      );
    } catch {
      throw invalidStored("pending identity publication evolution is invalid");
    }
  }
}

// ---------------------------------------------------------------- instance methods

const JSON_STORAGE_TYPE = () => physicalStorageValueType(LargeValueKind.Json);

export const lifecycleMethods = {
  /** Rebuild the groove layer over the same storage using the standard open path. */
  async reopenInPlace(this: NodeState): Promise<NodeState> {
    const storage = this.databaseSlot.intoInner().intoStorage();
    const reopened =
      this.catalogueBootstrapState === CatalogueBootstrapState.Uninitialized
        ? await newCatalogueUninitialized(this.nodeUuidValue, storage)
        : await newWithOptionsInner(
            this.nodeUuidValue,
            this.catalogue.schema,
            storage,
            this.historyComplete,
            CatalogueBootstrapState.Ready,
          );
    reopened.database.setMissingChunkResolver(this.chunkResolver);
    reopened.localChunkReader = reopened.database.localChunkReader();
    reopened.chunkResolver = this.chunkResolver;
    reopened.detachCoveredChunkWaits = this.detachCoveredChunkWaits;
    reopened.contentRuntimeProvider = reopened.database.ownedChunkProvider();
    reopened.authoritativeScalarExitRefresh = this.authoritativeScalarExitRefresh;
    return reopened;
  },

  committedGlobalTime(this: NodeState) {
    return this.clock.committedGlobalTime;
  },
  nodeUuid(this: NodeState): NodeUuid {
    return this.nodeUuidValue;
  },
  /** Highest HLC value this runtime has observed or minted. */
  txTimeHighWater(this: NodeState): TxTime {
    const reserved = this.clock.reservationHighWater.value;
    return TxTimeOps.cmp(this.clock.txTime, reserved) >= 0 ? this.clock.txTime : reserved;
  },
  txTimeReservationClock(this: NodeState) {
    return this.clock.reservationHighWater;
  },
  /** Establish a durable-owner supplied lower bound before minting the first transaction identity. */
  seedTxTimeHighWater(this: NodeState, highWater: TxTime): void {
    this.mergeTxTime(highWater);
  },
  authoredCommitDurabilityTier(this: NodeState) {
    return this.authoredCommitDurability;
  },
  setNonDurableClient(this: NodeState): void {
    this.authoredCommitDurability = "None";
  },
  /** Enable only for a host that owns complete current policy inputs. */
  enableAuthoritativeScalarExitRefresh(this: NodeState): void {
    if (!this.clientRelayScope()) this.authoritativeScalarExitRefresh = true;
  },
  /** Mark this process as the durable half of a browser client/worker relay. */
  configureScopeIsolatedClientRelay(this: NodeState, scope: any): void {
    const current = this.relayAuthoritySessionOwner;
    if (current && !current.sameOwner(scope))
      throw nodeErr(
        "InvalidBranchKey",
        "a relay cannot be rebound to a different storage ownership scope",
      );
    this.authoritativeScalarExitRefresh = false;
    this.relayAuthoritySessionOwner = scope;
  },
  clientRelayScope(this: NodeState) {
    return this.relayAuthoritySessionOwner;
  },

  /** Attach process-local auth claims to an accepted subscriber identity. */
  setSessionClaims(this: NodeState, identity: AuthorSubject, claims: Map<string, Value>): void {
    const key = authorSubjectKey(identity);
    const existing = this.sessionClaims.get(key);
    if (existing && sameClaims(existing[1], claims)) return;
    this.sessionClaims.set(key, [identity, claims]);
    this.sessionClaimRevisions.set(key, (this.sessionClaimRevisions.get(key) ?? 0) + 1);
    this.query.readPolicyAuthorizationRequestCache.clear();
    this.query.policyAuthorizationGraphCache.clear();
    this.query.policyAuthorizationGraphReplacements.clear();
  },
  /** Scope one node-locked operation to this subscriber's admitted claims. */
  scopedActiveSessionClaims(this: NodeState, identity: AuthorSubject, claims: Map<string, Value>) {
    const previous = this.activeSessionClaims;
    this.activeSessionClaims = [identity, claims];
    return new ActiveSessionClaimsScope(this, previous);
  },
  scopedOptionalSessionClaims(
    this: NodeState,
    identity: AuthorSubject,
    claims?: Map<string, Value>,
  ) {
    const previous = this.activeSessionClaims;
    if (claims) this.activeSessionClaims = [identity, claims];
    return new ActiveSessionClaimsScope(this, previous);
  },
  /** An opaque, domain-separated identity for the active session scope. */
  activeSessionClaimScopeKey(this: NodeState, identity: AuthorSubject): string | undefined {
    const active = this.activeSessionClaims;
    if (!active || authorSubjectKey(active[0]) !== authorSubjectKey(identity)) return undefined;
    const w = new Writer();
    encodeAuthorSubject(w, identity);
    const entries = [...active[1]].sort(([a], [b]) => cmpStr(a, b));
    w.varint(entries.length);
    for (const [name, value] of entries) {
      w.str(name);
      encodeValue(w, value);
    }
    const domain = new TextEncoder().encode("jazz/active-session-claim-scope/v1\\0");
    const encoded = w.finish();
    const input = new Uint8Array(domain.length + encoded.length);
    input.set(domain);
    input.set(encoded, domain.length);
    return toHex(blake3(input));
  },
  sessionClaimRevision(this: NodeState, identity: AuthorSubject): number {
    return this.sessionClaimRevisions.get(authorSubjectKey(identity)) ?? 0;
  },
  /** Current process-local claims together with their revisions. */
  sessionClaimsWithRevisions(this: NodeState): [AuthorSubject, Map<string, Value>, number][] {
    return [...this.sessionClaims.values()].map(([identity, claims]) => [
      identity,
      new Map(claims),
      this.sessionClaimRevision(identity),
    ]);
  },
  sessionClaimsFor(this: NodeState, identity: AuthorSubject): Map<string, Value> {
    return new Map(this.sessionClaims.get(authorSubjectKey(identity))?.[1] ?? []);
  },
  setPermissionsReady(this: NodeState, ready: boolean): void {
    this.permissionsReady = ready;
  },
  isPermissionsReady(this: NodeState): boolean {
    return this.permissionsReady;
  },

  // ---- chunk storage

  /** Replace the policy-blind immutable content backend used by Groove. */
  setChunkStorage(this: NodeState, storage: any): void {
    this.database.setChunkStorage(storage);
    this.database.setMissingChunkResolver(this.chunkResolver);
    const provider = this.database.ownedChunkProvider();
    this.localChunkReader.refreshFrom(this.database.localChunkReader());
    this.contentRuntimeProvider = provider;
  },
  detachCoveredChunkWaitsHandle(this: NodeState) {
    return this.detachCoveredChunkWaits;
  },
  detachesCoveredChunkWaits(this: NodeState): boolean {
    return this.detachCoveredChunkWaits.value;
  },
  /** Install Jazz's sync-plane fallback for chunks absent from local storage. */
  setMissingChunkResolver(this: NodeState, resolver: any): void {
    this.database.setMissingChunkResolver(resolver);
    const provider = this.database.ownedChunkProvider();
    this.chunkResolver = resolver;
    this.contentRuntimeProvider = provider;
  },
  /** Consult local immutable storage without invoking the peer fallback. */
  localChunk(this: NodeState, locator: any, expectedHash: any): Promise<Uint8Array> {
    return this.localChunkReader.get(locator, expectedHash);
  },
  localChunkReaderHandle(this: NodeState) {
    return this.localChunkReader;
  },

  // ---- large values

  setLargeValueStagingPolicy(this: NodeState, policy: any): void {
    this.largeValueStagingPolicy = policy;
  },
  async enforceLargeValueStagingPolicy(this: NodeState, newest: any): Promise<void> {
    if (!this.admitLargeValueIngress(newest.accounting.encodedBytes)) {
      await this.database.evictStagedLargeValue(newest.id);
      throw nodeErr("LargeValueIngressRateLimited");
    }
  },
  admitLargeValueIngress(this: NodeState, encodedBytes: number): boolean {
    const now = nowMsU64();
    const ingress = this.largeValueIngress;
    const policy = this.largeValueStagingPolicy;
    if (
      ingress.windowStartedMs === 0 ||
      now - ingress.windowStartedMs >= Math.max(policy.windowMs, 1)
    ) {
      ingress.windowStartedMs = now;
      ingress.admittedBytes = 0;
    }
    const next = ingress.admittedBytes + encodedBytes;
    if (next > policy.incomingBytesPerWindow) return false;
    ingress.admittedBytes = next;
    return true;
  },
  async ensureLargeValueStagesCurrent(this: NodeState, ids: Iterable<any>): Promise<void> {
    const wanted = [...ids];
    if (!wanted.length) return;
    const staged = new Set((await this.database.stagedLargeValues()).map((s: any) => String(s.id)));
    for (const id of wanted) if (!staged.has(String(id))) throw nodeErr("LargeValueStageExpired");
  },
  async currentStagedIdsForDescriptors(
    this: NodeState,
    descriptors: any[],
    requireEvery: boolean,
  ): Promise<any[]> {
    if (!descriptors.length) return [];
    const staged = await this.database.stagedLargeValues();
    const ids: any[] = [];
    for (const descriptor of descriptors) {
      const receipt = staged.find((r: any) => r.valueRef.equals(descriptor));
      if (receipt) ids.push(receipt.id);
      else if (requireEvery) throw nodeErr("LargeValueStageExpired");
    }
    return ids;
  },
  async requireStagedLargeValuesForVersions(this: NodeState, versions: any[]): Promise<void> {
    await this.currentStagedIdsForDescriptors(versionIndirectDescriptors(versions), true);
  },
  /** Evict unpublished staging roots older than the configured policy. */
  async evictExpiredStagedLargeValues(this: NodeState): Promise<number> {
    const maxAge = this.largeValueStagingPolicy.maxAgeMs;
    const now = nowMsU64();
    let evicted = 0;
    for (const staged of await this.database.stagedLargeValues())
      if (
        now - staged.createdAtMs > maxAge &&
        (await this.database.evictStagedLargeValue(staged.id))
      )
        evicted++;
    for (const upload of await this.database.pendingLargeValueUploads())
      if (
        now - upload.createdAtMs > maxAge &&
        (await this.database.evictPendingLargeValueUpload(upload.id))
      )
        evicted++;
    return evicted;
  },
  async stagedLargeValueIds(this: NodeState): Promise<any[]> {
    return (await this.database.stagedLargeValues()).map((s: any) => s.id);
  },
  evictStagedLargeValue(this: NodeState, id: any): Promise<boolean> {
    return this.database.evictStagedLargeValue(id);
  },
  async stageLargeValueChunkBatch(
    this: NodeState,
    uploadId: any,
    kind: any,
    chunks: any[],
  ): Promise<void> {
    let encodedBytes = 0;
    for (const chunk of chunks) {
      encodedBytes += chunk.encoded.length;
      if (!Number.isSafeInteger(encodedBytes))
        throw invalidStored("large-value chunk batch accounting overflow");
    }
    if (!this.admitLargeValueIngress(encodedBytes)) {
      await this.database.evictStagedLargeValue(uploadId);
      throw nodeErr("LargeValueIngressRateLimited");
    }
    if (!(await this.database.stageLargeValueChunkBatchIfCurrent(uploadId, kind, chunks)))
      throw nodeErr("LargeValueStageExpired");
  },
  /** Establish the one pending journal that later local stream operations must continue. */
  async beginStreamingLargeValueUpload(this: NodeState, uploadId: any, kind: any): Promise<void> {
    await this.database.stageLargeValueChunkBatch(uploadId, kind, []);
  },
  async evictPendingLargeValueUpload(this: NodeState, uploadId: any): Promise<void> {
    await this.database.evictPendingLargeValueUpload(uploadId);
  },
  async finalizeLargeValueUpload(this: NodeState, uploadId: any, valueRef: any): Promise<any> {
    // Presence is the semantic boundary; wall-clock age alone cannot reject an active upload.
    const staged = await this.database.finalizeLargeValueUploadIfCurrent(uploadId, valueRef);
    if (!staged) throw nodeErr("LargeValueStageExpired");
    return staged;
  },
  async consolidateAndStageLargeValue(this: NodeState, value: any): Promise<any> {
    const staged = await this.database.consolidateAndStageLargeValue(value);
    await this.enforceLargeValueStagingPolicy(staged);
    return staged;
  },
  async appendAndStageLargeValue(this: NodeState, value: any, bytes: Uint8Array): Promise<any> {
    const staged = await this.database.appendAndStageLargeValue(value, bytes);
    await this.enforceLargeValueStagingPolicy(staged);
    return staged;
  },
  async editAndStageLargeValue(
    this: NodeState,
    value: any,
    offset: number,
    deleteLength: number,
    insert: Uint8Array,
  ) {
    const staged = await this.database.editAndStageLargeValue(value, offset, deleteLength, insert);
    await this.enforceLargeValueStagingPolicy(staged);
    return staged;
  },
  readLargeValueRange(
    this: NodeState,
    value: any,
    start: number,
    end: number,
  ): Promise<Uint8Array> {
    return this.database.readLargeValueRange(value, start, end);
  },
  readLargeTextUtf16Range(
    this: NodeState,
    value: any,
    start: number,
    end: number,
  ): Promise<string> {
    return this.database.readLargeTextUtf16Range(value, start, end);
  },
  /** Keep UTF-16-to-byte lowering inside Groove. */
  largeTextUtf16OffsetToByte(this: NodeState, value: any, offset: number): Promise<number> {
    return this.database.largeTextUtf16OffsetToByte(value, offset);
  },
  readLargeJsonPointer(this: NodeState, value: any, pointer: string): Promise<unknown> {
    return this.database.readLargeJsonPointer(value, pointer);
  },

  // ---- hydration

  /** Materialize physical indirect scalar arms before returning cells across a public read boundary. */
  async hydrateLargeValueCells(this: NodeState, cells: Map<string, Value>): Promise<void> {
    for (const [column, value] of cells) {
      if (value.kind === "Large") cells.set(column, await this.materializeLargeValue(value.value));
      else if (value.kind === "Nullable" && value.value?.kind === "Large")
        cells.set(column, V("Nullable", await this.materializeLargeValue(value.value.value)));
    }
  },
  async hydrateCurrentRows(this: NodeState, rows: any[]): Promise<void> {
    for (const row of rows) {
      const descriptor = row.record.descriptor();
      const values = row.record.toValues();
      await this.hydrateRecordValues(values, descriptor);
      row.record = OwnedRecord.new(descriptor.create(values), descriptor);
    }
  },
  /** Materialize physical indirect scalars in one encoded record before a binding exposes it. */
  async hydrateEncodedRecord(
    this: NodeState,
    descriptor: any,
    raw: Uint8Array,
  ): Promise<Uint8Array> {
    const values = descriptor.bind(raw).toValues();
    await this.hydrateRecordValues(values, descriptor);
    return descriptor.create(values);
  },
  /** Values are re-encoded only after the full walk succeeds. */
  async hydrateRecordValues(this: NodeState, values: Value[], descriptor: any): Promise<void> {
    const fields = descriptor.fields();
    if (values.length !== fields.length)
      throw invalidStored("binding record has a descriptor/value arity mismatch");
    for (let i = 0; i < values.length; i++)
      values[i] = await this.hydrateValueForBinding(values[i], fields[i].valueType);
  },
  async hydrateValueForBinding(this: NodeState, value: Value, ty: any): Promise<Value> {
    const large = (kinds: any[], message: string) => {
      if (value.kind === "Large" && kinds.includes(value.value.kind))
        return this.materializeLargeValue(value.value);
      throw invalidStored(message);
    };
    if (ty.kind === "Internal" && ty.equals?.(JSON_STORAGE_TYPE())) {
      if (value.kind === "String") return value;
      return large([LargeValueKind.Json], "binding JSON value does not match its descriptor");
    }
    switch (ty.kind) {
      case "String":
        if (value.kind === "String") return value;
        return large(
          [LargeValueKind.String, LargeValueKind.Json],
          "binding string value does not match its descriptor",
        );
      case "Bytes":
        if (value.kind === "Bytes") return value;
        return large([LargeValueKind.Bytes], "binding bytes value does not match its descriptor");
      case "Nullable":
        if (value.kind !== "Nullable")
          throw invalidStored("binding nullable value does not match its descriptor");
        return value.value === undefined
          ? value
          : V("Nullable", await this.hydrateValueForBinding(value.value, ty.inner));
      case "Array": {
        if (value.kind !== "Array")
          throw invalidStored("binding array value does not match its descriptor");
        const out: Value[] = [];
        for (const v of value.value) out.push(await this.hydrateValueForBinding(v, ty.element));
        return V("Array", out);
      }
      case "Tuple": {
        if (value.kind !== "Tuple" || value.value.length !== ty.members.length)
          throw invalidStored("binding tuple value does not match its descriptor");
        const out: Value[] = [];
        for (let i = 0; i < ty.members.length; i++)
          out.push(await this.hydrateValueForBinding(value.value[i], ty.members[i]));
        return V("Tuple", out);
      }
      case "Record": {
        if (value.kind !== "Record" || !value.value.descriptor().equals(ty.descriptor))
          throw invalidStored("binding nested record does not match its descriptor");
        const values = value.value.toValues();
        await this.hydrateRecordValues(values, ty.descriptor);
        return V("Record", OwnedRecord.new(ty.descriptor.create(values), ty.descriptor));
      }
      case "Enum": {
        if (value.kind !== "Enum")
          throw invalidStored("binding enum value does not match its descriptor");
        const tag = value.value.tag();
        const c = ty.schema.case(tag);
        if (!value.value.record().descriptor().equals(c.payload))
          throw invalidStored("binding enum payload does not match its descriptor");
        const values = value.value.record().toValues();
        await this.hydrateRecordValues(values, c.payload);
        return V("Enum", EnumValue.new(tag, OwnedRecord.new(c.payload.create(values), c.payload)));
      }
      case "U8":
      case "U16":
      case "U32":
      case "U64":
      case "I32":
      case "I64":
      case "Bool":
      case "Uuid":
        if (value.kind === ty.kind) return value;
        break;
      case "F64":
        if (value.kind === "F64" && !Number.isNaN(value.value)) return value;
        throw invalidStored("binding float value does not match its descriptor");
      case "EnumTag":
        if (value.kind === "EnumTag") ty.schema.variant(value.value);
        else if (value.kind === "String") ty.schema.discriminant(value.value);
        else throw invalidStored("binding enum tag value does not match its descriptor");
        return value;
    }
    throw invalidStored("binding value does not match its descriptor");
  },
  async materializeLargeValue(this: NodeState, valueRef: any): Promise<Value> {
    const bytes: Uint8Array = await this.database.readLargeValueRange(
      valueRef,
      0,
      valueRef.byteLength,
    );
    if (valueRef.kind === LargeValueKind.Bytes) return V("Bytes", bytes);
    try {
      return V("String", new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    } catch {
      throw invalidStored("large text is not valid UTF-8");
    }
  },

  // ---- rebuild and receipts

  async rebuildDatabaseSlot(this: NodeState): Promise<void> {
    // Parking is in-memory delivery state, not derivable from storage; retain it across a live refresh.
    const parking = cloneParking(this.parking);
    await this.database.prepareForStorageExtraction();
    const lowered = this.catalogue.schema.lowerToGroove();
    lowered.tables.push(
      ...physicalVersionStorageTables(
        this.catalogue.catalogueSchemas,
        this.catalogue.schemaVersionAliases,
        this.catalogue.physicalMappings,
      ),
    );
    this.database.rebuild(lowered);
    this.database.setMissingChunkResolver(this.chunkResolver);
    this.contentRuntimeProvider = this.database.ownedChunkProvider();
    this.localChunkReader.refreshFrom(this.database.localChunkReader());
    // Register the same frozen derived indexes as ordinary open before compiling live graphs.
    await this.synchronizePhysicalVersionTables();
    this.grooveRuntimeToken = nextGrooveRuntimeToken();
    this.invalidateRuntimeHandlesAfterDatabaseRebuild();
    this.parking = parking;
  },
  /** Drop handles and plans compiled against the old registry while keeping recovered durable facts. */
  invalidateRuntimeHandlesAfterDatabaseRebuild(this: NodeState): void {
    const q = this.query;
    q.queryShapeCache.clear();
    q.compiledQueryProgramCache.clear();
    q.queryProgramTemplates.clear();
    q.supportedQueryProgramRequests.length = 0;
    q.readPolicyAuthorizationRequestCache.clear();
    q.policyAuthorizationGraphCache.clear();
    q.policyAuthorizationGraphReplacements.clear();
    q.txVersionsCache.clear();
    q.txVersionTablesCacheOrder.length = 0;
    q.txVersionTablesCacheOrderSet.clear();
    q.versionStorageSourcesCache.clear();
    this.invalidateSubscriptionScopes();
  },
  /** A reset replaces receipt contents, not the receipt itself. */
  clearSettledResultView(this: NodeState, key: any): void {
    const k = authorityResultKeyString(key);
    const state = this.query.authorityResults.get(k);
    if (state) {
      state.supportingRevision = undefined;
      state.coveredInputVersions.clear();
      state.sourceClosure = { kind: "Pending" };
      state.sourceIncrementals = [];
    }
    this.query.retainedRootWindowSources.delete(k);
  },
  /** Retire a receipt whose exact usage-site ownership has ended. */
  retireAuthorityResultView(this: NodeState, key: any): void {
    const k = authorityResultKeyString(key);
    this.query.authorityResults.delete(k);
    this.query.retainedRootWindowSources.delete(k);
  },
};

function sameClaims(a: Map<string, Value>, b: Map<string, Value>): boolean {
  if (a.size !== b.size) return false;
  for (const [k, v] of a) {
    const w = b.get(k);
    if (w === undefined || !valueEquals(v, w)) return false;
  }
  return true;
}
