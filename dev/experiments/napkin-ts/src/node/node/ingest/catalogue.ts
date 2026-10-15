// Sync-message dispatch and trusted catalogue publication (schema, lineage, lens) ingest.
import { SortedMap, SortedSet, cmpBytes, cmpNum } from "../../../support/collections.js";
import { equalBytes } from "../../../support/bytes.js";
import {
  nodeError,
  grooveDbError,
  isGrooveDbError,
  isLargeValueUploadRejected,
  isCatalogueMutation,
  nextGrooveRuntimeToken,
  LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES,
  PublicationOutcome,
  AuthorSubject,
  SchemaVersion,
  migrationValidation,
  lensContentId,
  publicationContentId,
  publicationEquals,
  validateIdentitiesForSchema,
  validateShapeRegistrationSize,
  versionIndirectDescriptors,
  txTimePhysicalMs,
  type NodeState,
  type SyncMessage,
  type CommitUnitIngestContext,
  type Uuid,
} from "../../../_extern/node-physical-ingest.js";
import {
  allocateProvisionalPhysicalMapping,
  validateScalarEnumCaseProvenance,
  validatePayloadEnumCaseProvenance,
  synchronizePhysicalVersionTables,
} from "../physical.js";
import { encodePhysicalMapping } from "../codec.js";
import {
  authorityWallClockMs,
  MAX_SCHEMA_LINEAGE_DECLARATIONS,
  MAX_SCHEMA_LINEAGE_NAME_BYTES,
  MAX_SCHEMA_LINEAGE_OPS,
} from "../ingest.js";
import { ingestCommitUnitWithContext } from "./commit_bundles.js";
import { applyFateUpdate, drainParkedCommitUnits, drainParkedRelayCommitUnits } from "./fates.js";
import { validateReceivedFateUpdateGlobalTimeDurability } from "../ingest.js";

/** Global restart-persistent metadata ceiling across every connected peer. */
const MAX_PENDING_LARGE_VALUE_UPLOADS = 1024;

type CatalogueActivationMode = "ColdOpen" | "Live";
type Outcome = PublicationOutcome<SyncMessage[]>;

const invalidUpdate = (msg: string) => nodeError("InvalidCatalogueUpdate", msg);
const unsupported = (msg: string) => nodeError("UnsupportedSyncMessage", msg);
const chunkResult = (valueRef: any, status: any): Outcome =>
  PublicationOutcome.settled([{ kind: "ChunkUploadResult", valueRef, status }]);

/** Apply one sync message and return any outgoing sync messages. */
export const applySyncMessage = (node: NodeState, message: SyncMessage): Promise<Outcome> =>
  applySyncMessageWithIngestContext(node, message, undefined);

/** Apply a catalogue mutation from the trusted local administrative lane. */
export const applyTrustedCatalogueMessage = (
  node: NodeState,
  message: SyncMessage,
): Promise<Outcome> =>
  applySyncMessageWithIngestContext(node, message, {
    identity: AuthorSubject.SYSTEM,
    trust: "TrustedBackend",
    admittedWriteAuthorization: false,
    versionReceiptsValidated: false,
  });

function largeValueStatus(progress: any): any {
  return progress.kind === "Missing"
    ? { kind: "Need", nodes: progress.nodes.slice(0, 64) }
    : { kind: "Staged" };
}

/** Apply one sync message from a connection-authenticated upload path. */
export async function applySyncMessageWithIngestContext(
  node: NodeState,
  message: SyncMessage,
  ingestContext: CommitUnitIngestContext | undefined,
): Promise<Outcome> {
  if (isCatalogueMutation(message)) {
    node.database.ensureUsable();
    if (node.database.hasUnsettledPublications()) throw grooveDbError("UnsettledPublications");
  }
  // Uninitialized clients and local relays must install a complete trusted catalogue snapshot first.
  node.requireCatalogueReady();
  if (node.catalogueActivationFailed) throw nodeError("CatalogueActivationFailed");
  switch (message.kind) {
    case "CommitUnit": {
      const { tx, versions } = message;
      if (ingestContext)
        await node.currentStagedIdsForDescriptors(versionIndirectDescriptors(versions), true);
      const nowMs = ingestContext ? authorityWallClockMs() : txTimePhysicalMs(tx.txId.time);
      return ingestCommitUnitWithContext(node, tx, versions, nowMs, ingestContext);
    }
    case "ChunkUploadStart": {
      const start = message.value;
      if (!node.admitLargeValueIngress(LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES))
        return chunkResult(start.valueRef, { kind: "RateLimited" });
      let progress;
      try {
        progress = await node.database.beginLargeValueUploadWithPendingLimit(
          start.valueRef,
          MAX_PENDING_LARGE_VALUE_UPLOADS,
        );
      } catch (error) {
        if (isGrooveDbError(error, "PendingLargeValueUploadLimitExceeded"))
          return chunkResult(start.valueRef, { kind: "RateLimited" });
        if (isLargeValueUploadRejected(error))
          return chunkResult(start.valueRef, { kind: "Rejected" });
        throw error;
      }
      return chunkResult(start.valueRef, largeValueStatus(progress));
    }
    case "ChunkUploadNodes": {
      const batch = message.value;
      const uploads: any[] = await node.database.pendingLargeValueUploads();
      if (
        !uploads.some(
          (upload) =>
            upload.descriptor !== undefined &&
            node.valueRefEquals(upload.descriptor, batch.valueRef),
        )
      )
        return chunkResult(batch.valueRef, { kind: "Rejected" });
      let encodedBytes = 0;
      let nodeCount = 0;
      for (const chunk of batch.chunks) {
        encodedBytes += chunk.encoded.length;
        nodeCount += 1;
        if (!Number.isSafeInteger(encodedBytes) || !Number.isSafeInteger(nodeCount))
          throw unsupported("chunk upload accounting overflow");
      }
      if (!node.admitLargeValueIngress(encodedBytes))
        return chunkResult(batch.valueRef, { kind: "RateLimited" });
      let progress;
      try {
        progress = await node.database.continueLargeValueUploadIfCurrent(
          batch.valueRef,
          batch.chunks,
        );
      } catch (error) {
        if (isLargeValueUploadRejected(error))
          return chunkResult(batch.valueRef, { kind: "Rejected" });
        throw error;
      }
      if (progress === undefined) return chunkResult(batch.valueRef, { kind: "Rejected" });
      return chunkResult(batch.valueRef, largeValueStatus(progress));
    }
    case "ChunkUploadResult":
      throw unsupported("chunk upload result requires peer link context");
    case "SessionClaims":
      if (
        ingestContext &&
        (ingestContext.trust === "TrustedBackend" || ingestContext.trust === "TrustedAuthority")
      )
        node.setSessionClaims(message.identity, message.claims);
      return PublicationOutcome.settled([]);
    case "FateUpdate": {
      const { txId, fate, globalTime, durability } = message;
      validateReceivedFateUpdateGlobalTimeDurability(globalTime, durability);
      await applyFateUpdate(node, txId, fate, globalTime, durability);
      return drainParkedCommitUnits(node);
    }
    case "ViewUpdate": {
      const p = message.value;
      await node.applyViewUpdate({
        wireRows: p.supportingRows,
        subscription: p.subscription,
        settledThrough: p.settledThrough,
        deferSettlement: false,
        resetInputSet: true,
        versionCarriers: p.versionCarriers,
        peerCompleteTxPayloadRefs: p.peerPayloadInventory.completeTxPayloads,
        authorizationProgress: p.peerPayloadInventory.authorizationProgress,
        openingPending: p.peerPayloadInventory.openingPending,
        resultMemberAdds: [],
        resultMemberRemoves: [],
      });
      return PublicationOutcome.settled([]);
    }
    case "RegisterShape":
      if (!validateShapeRegistrationSize(message.ast, message.opts))
        throw unsupported("shape registration exceeds byte limit");
      node.registerShapeWithOptions(message.shapeId, message.ast, message.opts);
      return PublicationOutcome.settled([]);
    case "CatalogueSnapshot":
      throw unsupported("catalogue snapshot requires a trusted upstream link");
    case "ReadBarrier":
      throw unsupported("read barriers are handled by the receiving link");
    case "Subscribe":
      node.applySubscribe(message.value);
      return PublicationOutcome.settled([]);
    case "SubscribeRejected":
      throw unsupported("subscription rejection requires subscription stream context");
    case "Unsubscribe":
      node.applyUnsubscribe(message.subscription);
      return PublicationOutcome.settled([]);
    case "PublishSchema":
      return applyPublishSchema(node, message.author, ingestContext, message.schema);
    case "PublishSchemaWithLens":
      return applyPublishSchemaWithLens(
        node,
        message.author,
        ingestContext,
        message.catalogueSeq,
        message.publication,
      );
    case "PublishLens":
      return PublicationOutcome.settled(
        await applyPublishLens(node, message.author, ingestContext, message.lens),
      );
    case "CatalogueAck":
      return PublicationOutcome.settled([]);
    case "ChunkRequestBatch":
    case "ChunkResponseBatch":
      throw unsupported("chunk traffic requires peer link context");
    // The wire transport reassembles parts into one `ViewUpdate`.
    case "ViewUpdatePart":
      throw unsupported("view-update parts are reassembled by the wire transport");
    default:
      throw unsupported("permission advice requires authenticated link context");
  }
}

function clearCatalogueDerivedCaches(node: NodeState): void {
  node.catalogue.lensPathCache.clear();
  node.catalogue.compiledLensCache.clear();
  node.catalogue.physicalWritePlanCache.clear();
  node.catalogue.physicalCurrentWinnerProjections.clear();
  clearQueryCaches(node);
}

function clearQueryCaches(node: NodeState): void {
  node.query.versionStorageSourcesCache.clear();
  node.query.queryShapeCache.clear();
  node.query.compiledQueryProgramCache.clear();
  node.query.queryProgramTemplates.clear();
  node.query.supportedQueryProgramRequests.clear();
  node.query.readPolicyAuthorizationRequestCache.clear();
  node.query.policyAuthorizationGraphCache.clear();
  node.query.policyAuthorizationGraphReplacements.clear();
}

async function applyPublishSchema(
  node: NodeState,
  author: any,
  ingestContext: CommitUnitIngestContext | undefined,
  published: any,
): Promise<Outcome> {
  requireCatalogueAdmin(node, author, ingestContext);
  if (!equalBytes(published.id, published.schema.versionId()))
    throw invalidUpdate("schema id does not match schema payload");
  if (!node.catalogue.catalogueSchemas.has(published.id))
    throw invalidUpdate("non-genesis schema requires lineage publication");
  const schema = SchemaVersion.new_(published.schema.withoutPermissions());
  node.catalogue.catalogueSchemas.set(schema.id, schema);
  node.query.versionStorageSourcesCache.clear();
  node.query.readPolicyAuthorizationRequestCache.clear();
  node.query.policyAuthorizationGraphCache.clear();
  node.query.policyAuthorizationGraphReplacements.clear();
  await node.persistCatalogueSchema(schema);
  if (equalBytes(schema.id, node.catalogue.activeSchema.schema)) {
    const active = { ...node.catalogue.activeSchema };
    active.compiled = schema.schema.withPermissionsFrom(active.compiled);
    await node.persistActiveSchema(active);
    node.installActiveSchema(active);
  } else if (equalBytes(schema.id, node.catalogue.localSchemaVersionId)) {
    node.catalogue.schema = schema.schema.withPermissionsFrom(node.catalogue.schema);
  }
  await node.ensureProvisionalPhysicalMapping(schema.id);
  await node.ensureSchemaVersionAlias(schema.id);
  await synchronizePhysicalVersionTables(node);
  const outcome = await drainParkedCommitUnits(node);
  await drainParkedRelayCommitUnits(node);
  node.drainParkedShapeRegistrations();
  outcome.value.unshift({
    kind: "CatalogueAck",
    value: { revision: undefined, schema: schema.id, lens: undefined, applied: true },
  });
  return outcome;
}

const singleLens = (publication: any): Uuid | undefined =>
  publication.predecessors.length === 1 ? publication.predecessors[0].lens.id : undefined;

async function applyPublishSchemaWithLens(
  node: NodeState,
  author: any,
  ingestContext: CommitUnitIngestContext | undefined,
  catalogueSeq: number,
  publication: any,
): Promise<Outcome> {
  requireCatalogueAdmin(node, author, ingestContext);
  validateSchemaLineagePublication(publication);
  if (catalogueSeq === 0) throw invalidUpdate("schema lineage catalogue sequence must be nonzero");
  const schema = publication.schema;
  const existing = node.catalogue.activeLineagesByTarget.get(schema.id);
  if (existing) {
    if (
      !publicationEquals(existing.publication, publication) ||
      existing.catalogueSeq !== catalogueSeq
    )
      throw invalidUpdate("schema lineage publication conflicts with catalogue");
    return PublicationOutcome.settled([
      {
        kind: "CatalogueAck",
        value: {
          revision: undefined,
          schema: schema.id,
          lens: singleLens(publication),
          applied: true,
        },
      },
    ]);
  }
  if (catalogueSeq <= node.catalogue.activeCatalogueSeq)
    throw invalidUpdate("schema lineage catalogue sequence conflicts with active catalogue");
  const pendingExisting = node.catalogue.pendingLineages.get(catalogueSeq);
  if (pendingExisting) {
    if (!publicationEquals(pendingExisting.publication, publication))
      throw invalidUpdate("schema lineage catalogue sequence conflict");
  } else {
    if (
      publication.predecessors.every((p: any) => node.catalogue.catalogueSchemas.has(p.lens.source))
    )
      node.validatePublicationSources(
        publication,
        node.catalogue.catalogueSchemas,
        node.catalogue.physicalMappings,
        node.physicalIdentityHistoryForCandidate(publication.schema.id, publication.id),
      );
    const reserved = (lineages: SortedMap<number, any>) =>
      lineages.valuesArray().some((l) => equalBytes(l.publication.schema.id, schema.id));
    if (reserved(node.catalogue.pendingLineages) || reserved(node.catalogue.stagedLineages))
      throw invalidUpdate("schema lineage target is already reserved");
    const pending = { catalogueSeq, publication };
    await node.persistPendingSchemaLineage(pending);
    node.catalogue.pendingLineages.set(catalogueSeq, pending);
  }
  return drainPendingSchemaLineages(node);
}

export async function recoverPendingSchemaLineages(node: NodeState): Promise<void> {
  await activatePendingSchemaLineages(node, "ColdOpen");
}

export const drainPendingSchemaLineages = (node: NodeState): Promise<Outcome> =>
  activatePendingSchemaLineages(node, "Live");

function activationFailed(node: NodeState, staged: any): Error {
  removeStagedSchemaLineageFromMemory(node, staged);
  node.catalogueActivationFailed = true;
  return nodeError("CatalogueActivationFailed");
}

async function activatePendingSchemaLineages(
  node: NodeState,
  mode: CatalogueActivationMode,
): Promise<Outcome> {
  const outcome = PublicationOutcome.settled<SyncMessage[]>([]);
  for (;;) {
    const next = node.catalogue.activeCatalogueSeq + 1;
    const pending = node.catalogue.pendingLineages.get(next);
    if (!pending) break;
    const publication = pending.publication;
    if (
      publication.predecessors.some((p: any) => !node.catalogue.catalogueSchemas.has(p.lens.source))
    )
      break;
    try {
      node.validatePublicationSources(
        publication,
        node.catalogue.catalogueSchemas,
        node.catalogue.physicalMappings,
        node.physicalIdentityHistoryForCandidate(publication.schema.id, publication.id),
      );
      // Reconcile a parked sibling again only once its sequence becomes active: an earlier sibling may have
      // widened a shared scalar registry to the u8 limit meanwhile.
      node.validatePendingSchemaLineagePhysicalMapping(pending);
    } catch {
      await node.removePendingSchemaLineage(next, publication.id);
      break;
    }
    if (node.catalogue.activeLineagesByTarget.has(publication.schema.id))
      throw invalidUpdate("schema lineage target already has an active bundle");
    let staged = node.catalogue.stagedLineages.get(next);
    if (staged) {
      if (!publicationEquals(staged.publication, publication))
        throw invalidUpdate("staged schema lineage conflicts with pending bundle");
    } else {
      // Provisional identities are only candidates: allocate against copies and commit only retained identities.
      const provisional = {
        tableId: node.catalogue.nextPhysicalTableId,
        columnId: node.catalogue.nextPhysicalColumnId,
      };
      const fresh = allocateProvisionalPhysicalMapping(
        publication.schema.schema,
        publication.physicalIdentities,
        provisional,
      );
      const mapping = node.reconcilePublicationMapping(node.catalogue, publication, fresh);
      let nextTableId: number = node.catalogue.nextPhysicalTableId;
      let nextColumnId: number = node.catalogue.nextPhysicalColumnId;
      for (const table of mapping.tables.valuesArray()) {
        if (table.tableId + 1 > Number.MAX_SAFE_INTEGER)
          throw nodeError("InvalidStoredValue", "physical table id exhausted");
        nextTableId = Math.max(nextTableId, table.tableId + 1);
        for (const column of table.columns.valuesArray()) {
          if (column + 1 > Number.MAX_SAFE_INTEGER)
            throw nodeError("InvalidStoredValue", "physical column id exhausted");
          nextColumnId = Math.max(nextColumnId, column + 1);
        }
      }
      staged = { catalogueSeq: next, publication, alias: node.nextSchemaVersionAlias(), mapping };
      const candidateMappings = new SortedMap(cmpBytes, node.catalogue.physicalMappings);
      candidateMappings.set(staged.publication.schema.id, staged.mapping);
      const candidateAliases = new SortedMap<Uuid, number>(
        cmpBytes,
        node.catalogue.schemaVersionAliases,
      );
      candidateAliases.set(staged.publication.schema.id, staged.alias);
      validateScalarEnumCaseProvenance(candidateMappings as any, candidateAliases);
      validatePayloadEnumCaseProvenance(candidateMappings as any, candidateAliases);
      await node.persistCatalogueSchemaLineage(staged);
      node.catalogue.nextPhysicalTableId = nextTableId;
      node.catalogue.nextPhysicalColumnId = nextColumnId;
      node.catalogue.stagedLineages.set(next, staged);
    }

    // A new logical column widens the shared physical current-row descriptor; prepared graphs must be rebuilt.
    const widensSharedCurrentDescriptor = staged.mapping.tables
      .valuesArray()
      .some((target: any) => {
        const existingColumns = new SortedSet<number>(cmpNum);
        for (const mapping of node.catalogue.physicalMappings.valuesArray())
          for (const existing of mapping.tables.valuesArray())
            if (existing.tableId === target.tableId)
              for (const c of existing.columns.valuesArray()) existingColumns.add(c);
        return (
          existingColumns.size > 0 &&
          target.columns.valuesArray().some((c: number) => !existingColumns.has(c))
        );
      });

    installStagedSchemaLineageInMemory(node, staged);
    // Install widened variant cases in the live registry rather than reopening the database.
    try {
      await synchronizePhysicalVersionTables(node);
    } catch {
      throw activationFailed(node, staged);
    }
    // Closed raw receivers can make a cold server look live: prune them explicitly.
    if (mode === "Live") {
      try {
        await node.database.pruneDroppedSubscriptions();
      } catch {
        throw activationFailed(node, staged);
      }
    }
    const rebuildColdRuntime =
      mode === "ColdOpen" || node.database.runtimeStats().activeSubscriptions === 0;
    if (rebuildColdRuntime) {
      try {
        await node.rebuildDatabaseSlot();
      } catch {
        throw activationFailed(node, staged);
      }
    }
    // Peer-serving and maintained caches compile against the shared current-row descriptor too.
    if (widensSharedCurrentDescriptor && !rebuildColdRuntime)
      node.invalidateRuntimeHandlesAfterDatabaseRebuild();
    const batch = node.database.openBatch();
    node.writeActiveSchemaLineageToBatch(batch, staged);
    // A catalogue-only batch writes no history rows, so it applies directly.
    try {
      const applied = await node.database.applyBatch(batch);
      const persisted = await node.database.persistWithProgress(applied);
      node.database.finishPersistence(persisted);
    } catch {
      throw activationFailed(node, staged);
    }
    node.catalogue.stagedLineages.delete(next);
    node.catalogue.pendingLineages.delete(next);
    node.catalogue.activeLineagesByTarget.set(staged.publication.schema.id, staged);
    node.catalogue.activeCatalogueSeq = next;
    if (mode === "Live" && widensSharedCurrentDescriptor)
      node.grooveRuntimeToken = nextGrooveRuntimeToken();
    if (mode === "Live") {
      outcome.value.push({
        kind: "CatalogueAck",
        value: {
          revision: next,
          schema: staged.publication.schema.id,
          lens: singleLens(staged.publication),
          applied: true,
        },
      });
      outcome.extend(await drainParkedCommitUnits(node));
      await drainParkedRelayCommitUnits(node);
      node.drainParkedShapeRegistrations();
    }
  }
  return outcome;
}

function installStagedSchemaLineageInMemory(node: NodeState, staged: any): void {
  const id = staged.publication.schema.id;
  node.catalogue.catalogueSchemas.set(id, staged.publication.schema);
  for (const predecessor of staged.publication.predecessors)
    node.catalogue.catalogueLenses.set(predecessor.lens.id, predecessor.lens);
  node.catalogue.schemaVersionAliases.set(id, staged.alias);
  node.catalogue.physicalMappings.set(id, staged.mapping);
  clearCatalogueDerivedCaches(node);
}

function removeStagedSchemaLineageFromMemory(node: NodeState, staged: any): void {
  const id = staged.publication.schema.id;
  node.catalogue.catalogueSchemas.delete(id);
  for (const predecessor of staged.publication.predecessors)
    node.catalogue.catalogueLenses.delete(predecessor.lens.id);
  node.catalogue.schemaVersionAliases.delete(id);
  node.catalogue.physicalMappings.delete(id);
  clearCatalogueDerivedCaches(node);
}

async function applyPublishLens(
  node: NodeState,
  author: any,
  ingestContext: CommitUnitIngestContext | undefined,
  lens: any,
): Promise<SyncMessage[]> {
  requireCatalogueAdmin(node, author, ingestContext);
  if (!equalBytes(lens.id, lensContentId(lens)))
    throw invalidUpdate("lens id does not match lens payload");
  if (
    !node.catalogue.catalogueSchemas.has(lens.source) ||
    !node.catalogue.catalogueSchemas.has(lens.target)
  )
    throw invalidUpdate("lens endpoint is unknown");
  validateMigrationLens(node, lens);
  const installed = !node.catalogue.catalogueLenses.has(lens.id);
  if (installed) {
    const candidate = node.reconcilePhysicalMappingForLens(lens);
    const authoritative = node.catalogue.physicalMappings.get(lens.target);
    if (!authoritative)
      throw nodeError("InvalidStoredValue", "authoritative physical mapping missing");
    if (!equalBytes(encodePhysicalMapping(candidate), encodePhysicalMapping(authoritative)))
      throw invalidUpdate("cross-lens conflicts with authoritative physical mapping");
  }
  await node.persistCatalogueLensWithPhysicalMetadata(lens, undefined);
  if (installed) node.catalogue.catalogueLenses.set(lens.id, lens);
  node.catalogue.lensPathCache.clear();
  node.catalogue.compiledLensCache.clear();
  node.catalogue.physicalCurrentWinnerProjections.clear();
  clearQueryCaches(node);
  // Both endpoint schemas are Active and their projection cases were registered during activation; a cross-lens
  // adds a catalogue path only (Groove rejects re-registration as a duplicate variant projection).
  return [
    {
      kind: "CatalogueAck",
      value: { revision: undefined, schema: undefined, lens: lens.id, applied: true },
    },
  ];
}

function requireCatalogueAdmin(
  _node: NodeState,
  _claimedAuthor: any,
  ingestContext: CommitUnitIngestContext | undefined,
): void {
  if (
    ingestContext &&
    AuthorSubject.equals(ingestContext.identity, AuthorSubject.SYSTEM) &&
    (ingestContext.trust === "TrustedBackend" || ingestContext.trust === "TrustedAuthority")
  )
    return;
  throw nodeError("UnauthorizedCatalogueUpdate");
}

function validateMigrationLens(node: NodeState, lens: any): void {
  const source = node.catalogue.catalogueSchemas.get(lens.source);
  const target = node.catalogue.catalogueSchemas.get(lens.target);
  if (!source || !target) throw invalidUpdate("lens endpoint is unknown");
  validateMigrationLensBetween(lens, source, target);
}

export const validateMigrationLensBetween = (lens: any, source: any, target: any): void =>
  migrationValidation.validateMigrationLensBetween(lens, source, target);

export const validateLineageTablePartition = (
  source: any,
  target: any,
  lens: any,
  newTables: string[],
  droppedTables: string[],
): void =>
  migrationValidation.validateLineageTablePartition(source, target, lens, newTables, droppedTables);

export function validateSchemaLineagePublicationBounds(publication: any): void {
  let declarations = publication.predecessors.length;
  let operations = 0;
  let namesInBounds = true;
  const nameOk = (name: string) =>
    name.length > 0 && new TextEncoder().encode(name).length <= MAX_SCHEMA_LINEAGE_NAME_BYTES;
  for (const p of publication.predecessors) {
    declarations += p.lens.tableLenses.length + p.newTables.length + p.droppedTables.length;
    for (const table of p.lens.tableLenses) {
      operations += table.ops.length;
      if (!nameOk(table.sourceTable) || !nameOk(table.targetTable)) namesInBounds = false;
    }
    if (![...p.newTables, ...p.droppedTables].every(nameOk)) namesInBounds = false;
  }
  if (
    declarations > MAX_SCHEMA_LINEAGE_DECLARATIONS ||
    operations > MAX_SCHEMA_LINEAGE_OPS ||
    !namesInBounds
  )
    throw invalidUpdate("schema lineage publication exceeds structural limits");
}

export function validateSchemaLineagePublication(publication: any): void {
  validateSchemaLineagePublicationBounds(publication);
  if (!equalBytes(publication.id, publicationContentId(publication)))
    throw invalidUpdate("schema lineage publication id mismatch");
  const identityError = validateIdentitiesForSchema(
    publication.physicalIdentities,
    publication.schema.schema,
  );
  if (identityError !== undefined) throw invalidUpdate(identityError);
  if (!equalBytes(publication.schema.id, publication.schema.schema.versionId()))
    throw invalidUpdate("schema id does not match schema payload");
  if (publication.predecessors.length === 0) throw invalidUpdate("schema requires a predecessor");
  const sources = new SortedSet<Uuid>(cmpBytes);
  for (const { lens } of publication.predecessors) {
    if (!sources.add(lens.source) || equalBytes(lens.source, publication.schema.id))
      throw invalidUpdate("duplicate or self schema predecessor");
    if (!equalBytes(lens.id, lensContentId(lens)))
      throw invalidUpdate("lens id does not match lens payload");
    if (!equalBytes(lens.target, publication.schema.id))
      throw invalidUpdate("lineage lens target does not match schema");
  }
}
