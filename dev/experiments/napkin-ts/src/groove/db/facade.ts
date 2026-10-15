// Database construction, lifecycle and the Groove-owned large-value facade.
import { type Bytes, equalBytes, utf8 } from "../../support/bytes.js";
import { SortedMap } from "../../support/collections.js";
import { uuidV4 } from "../../support/hash.js";
import {
  type DatabaseSchema,
  type LargeValueRef,
  type NodeRef,
  type PublicationId,
  type RecordDescriptor,
  type Waker,
  type ChunkStorage,
  type MissingChunkResolver,
  IvmRuntime,
  IndexIdRegistry,
  ChunkError,
  cmpNodeRef,
  encodeNodeRef,
  decodeNodeRef,
  encodeLargeValueRef,
  ivmErrorFrom,
  largeValues as lv,
  chunks,
  EvaluationInputs,
  deriveKey,
  PENDING,
} from "../../_extern/groove-db-storage.js";
import {
  type BoxedStorage,
  type OrderedKvStorage,
  type ReopenableStorage,
  type StorageLayout,
  type WriteOperation,
  LayoutStorage,
  OwnedStorage,
  RecordStore,
  StagedWriteOverlay,
  StagedWriteState,
  scanPrefix,
  collectScan,
} from "../storage/index.js";
import { DatabaseBatch } from "./batch.js";
import {
  type CompletedLargeValueUpload,
  type LargeValueNodeReferences,
  type PendingLargeValueUpload,
  type StagedLargeValue,
  type StagedLargeValueAccounting,
  Database,
  DbError,
  HostApplicationGuard,
  LARGE_VALUE_METADATA_CF,
  MetadataChunkInstallJournal,
  MetadataChunkInstallObserver,
  completedLargeValueReceiptKey,
  completedLargeValueUploadKey,
  decodeCompletedLargeValueReceiptAtKey,
  decodeCompletedLargeValueUploadAtKey,
  decodeLargeValueNodeReferences,
  decodeLargeValueRootReferences,
  decodePendingLargeValueUploadAtKey,
  decodeStagedLargeValueAtKey,
  emptyRootReferences,
  encodeCompletedLargeValueUpload,
  encodeLargeValueNodeReferences,
  encodeLargeValueRootReferences,
  encodePendingLargeValueUpload,
  encodeStagedLargeValue,
  largeValueNodeKey,
  largeValueNodeTransitionOperations,
  largeValueReclaimDecision,
  largeValueReclaimKey,
  largeValueReclaimNodeRefFromKey,
  largeValueRootKey,
  lvMeta,
  pendingLargeValueUploadKey,
  stagedLargeValueKey,
  uniqueLargeValueChildren,
} from "./index.js";
import { validateApplicationStorageNames, validateDurableKeySchema } from "./schema_admission.js";
import {
  DirectRecordStore,
  type DirectRecordStoreWrite,
  AsyncLock,
  ReadMetricsSink,
} from "./storage_helpers.js";
import { dbTry } from "./commit.js";
import { pollFn } from "./query.js";

const ivm = (error: unknown) => new DbError("IvmRuntime", ivmErrorFrom(error));
const md = LARGE_VALUE_METADATA_CF;
const nowMs = () => Date.now();
const set = (key: Bytes, value: Bytes): WriteOperation => ({ kind: "Set", cf: md, key, value });
const del = (key: Bytes): WriteOperation => ({ kind: "Delete", cf: md, key });
const get = (storage: OrderedKvStorage, key: Bytes) => dbTry(storage.get(md, key));
const sameRef = (a: LargeValueRef, b: LargeValueRef) =>
  equalBytes(encodeLargeValueRef(a), encodeLargeValueRef(b));
const sameAccounting = (a: StagedLargeValueAccounting, b: StagedLargeValueAccounting) =>
  a.encodedBytes === b.encodedBytes && a.nodeCount === b.nodeCount;

export function isRetryableUploadError(error: unknown): boolean {
  if (!(error instanceof DbError)) return false;
  if (error.kind === "Storage") return true;
  if (error.kind !== "IvmRuntime") return false;
  const inner = error.data;
  if (inner?.kind === "Storage") return true;
  return (
    inner?.kind === "Chunk" && ["Backend", "Unavailable", "Retryable"].includes(inner.chunk?.kind)
  );
}

/** Build the metadata half of consuming an incomplete upload. */
export async function pendingLargeValueUploadReleaseOperations(
  storage: OrderedKvStorage,
  key: Bytes,
  upload: PendingLargeValueUpload,
): Promise<WriteOperation[]> {
  const operations: WriteOperation[] = [del(key)];
  for (const nodeRef of upload.chunks) {
    const nodeKey = largeValueNodeKey(nodeRef);
    const encoded = await get(storage, nodeKey);
    if (!encoded) continue;
    const metadata = decodeLargeValueNodeReferences(encoded);
    if (metadata.uploadReferences === 0) throw lvMeta("upload reference count underflow");
    metadata.uploadReferences -= 1;
    operations.push(set(nodeKey, encodeLargeValueNodeReferences(metadata)));
    if (metadata.references === 0 && metadata.uploadReferences === 0) {
      let encodedRef: Bytes;
      try {
        encodedRef = encodeNodeRef(nodeRef);
      } catch (error) {
        throw lvMeta(`cannot encode reclaim entry: ${error}`);
      }
      operations.push(set(largeValueReclaimKey(nodeRef), encodedRef));
    }
  }
  return operations;
}

/** How much of an upload finalization must re-read before issuing a receipt. */
type UploadValidation = { kind: "Full" } | { kind: "DerivedFrom"; base: LargeValueRef };

/** Whether `root` holds a staging receipt or a durably published reference. */
export async function largeValueRootIsActive(
  storage: OrderedKvStorage,
  root: NodeRef,
): Promise<boolean> {
  const encoded = await get(storage, largeValueRootKey(root));
  if (!encoded) return false;
  const references = decodeLargeValueRootReferences(encoded);
  return references.durable > 0 || references.staged > 0;
}

/** Return the staged-receipt metadata transition without committing it. */
export async function stagedLargeValueRegistrationOperations(
  storage: OrderedKvStorage,
  id: Bytes,
  valueRef: LargeValueRef,
  accounting: StagedLargeValueAccounting,
): Promise<[StagedLargeValue, WriteOperation[]]> {
  const stagedKey = stagedLargeValueKey(id);
  const existingEncoded = await get(storage, stagedKey);
  if (existingEncoded) {
    const existing = decodeStagedLargeValueAtKey(stagedKey, existingEncoded);
    if (sameRef(existing.valueRef, valueRef) && sameAccounting(existing.accounting, accounting))
      return [existing, []];
    throw lvMeta("staged receipt id is already bound to a different descriptor");
  }
  const staged: StagedLargeValue = { id, valueRef, accounting, createdAtMs: nowMs() };
  const encoded = encodeStagedLargeValue(staged);
  const rootKey = largeValueRootKey(staged.valueRef.root);
  const rootEncoded = await get(storage, rootKey);
  const references = rootEncoded
    ? decodeLargeValueRootReferences(rootEncoded)
    : emptyRootReferences();
  const activateRoot = references.durable === 0 && references.staged === 0;
  if (activateRoot) references.nodeActive = true;
  if (references.staged >= 0xffffffff) throw lvMeta("staged root count overflow");
  references.staged += 1;
  const operations = [
    set(stagedKey, encoded),
    set(rootKey, encodeLargeValueRootReferences(references)),
  ];
  if (activateRoot)
    operations.push(
      ...(await largeValueNodeTransitionOperations(
        storage,
        new SortedMap<NodeRef, LargeValueNodeReferences>(cmpNodeRef),
        [[staged.valueRef.root, 1]],
        false,
      )),
    );
  return [staged, operations];
}

/** Read one completed upload's bidirectional journals. */
export async function completedLargeValueUploadJournal(
  storage: OrderedKvStorage,
  uploadId: Bytes,
): Promise<CompletedLargeValueUpload | undefined> {
  const uploadKey = completedLargeValueUploadKey(uploadId);
  const encoded = await get(storage, uploadKey);
  if (!encoded) return undefined;
  const completed = decodeCompletedLargeValueUploadAtKey(uploadKey, encoded);
  const receiptKey = completedLargeValueReceiptKey(completed.receiptId);
  const reverse = await get(storage, receiptKey);
  if (!reverse) throw lvMeta("completed upload has no receipt reverse binding");
  if (!equalBytes(decodeCompletedLargeValueReceiptAtKey(receiptKey, reverse), uploadId))
    throw lvMeta("completed upload bindings disagree");
  return completed;
}

/** Resolve a completed upload only after its journal and staging receipt agree. */
export async function completedLargeValueUploadReceipt(
  storage: OrderedKvStorage,
  uploadId: Bytes,
): Promise<StagedLargeValue | undefined> {
  const completed = await completedLargeValueUploadJournal(storage, uploadId);
  if (!completed) return undefined;
  const stagedKey = stagedLargeValueKey(completed.receiptId);
  const encoded = await get(storage, stagedKey);
  if (!encoded) throw lvMeta("completed upload points to a missing staged receipt");
  const staged = decodeStagedLargeValueAtKey(stagedKey, encoded);
  if (
    !equalBytes(staged.id, completed.receiptId) ||
    !sameRef(completed.descriptor, staged.valueRef) ||
    !sameAccounting(staged.accounting, completed.accounting)
  )
    throw lvMeta("completed upload receipt does not match its binding");
  return staged;
}

export async function completedLargeValueUpload(
  storage: OrderedKvStorage,
  uploadId: Bytes,
  valueRef: LargeValueRef,
): Promise<StagedLargeValue | undefined> {
  const staged = await completedLargeValueUploadReceipt(storage, uploadId);
  if (staged && !sameRef(staged.valueRef, valueRef))
    throw lvMeta("completed upload is bound to a different descriptor");
  return staged;
}

export async function completedLargeValueCleanupOperations(
  storage: OrderedKvStorage,
  receiptId: Bytes,
): Promise<WriteOperation[]> {
  const receiptKey = completedLargeValueReceiptKey(receiptId);
  const encoded = await get(storage, receiptKey);
  if (!encoded) return [];
  const uploadId = decodeCompletedLargeValueReceiptAtKey(receiptKey, encoded);
  const uploadKey = completedLargeValueUploadKey(uploadId);
  const forward = await get(storage, uploadKey);
  if (!forward) throw lvMeta("completed receipt has no upload binding");
  if (!equalBytes(decodeCompletedLargeValueUploadAtKey(uploadKey, forward).receiptId, receiptId))
    throw lvMeta("completed upload bindings disagree");
  return [del(uploadKey), del(receiptKey)];
}

async function scanMetadata(storage: OrderedKvStorage, prefix: string) {
  return collectScan(await dbTry(storage.scan(scanPrefix(md, utf8(prefix)))));
}

export async function cooperativeYieldOnce(): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

/** Install the storage-backed chunk provider with metadata observer and journal. */
function installChunkProvider(
  ivmRuntime: IvmRuntime,
  storage: LayoutStorage,
  lifecycle: AsyncLock,
  chunkStorage: ChunkStorage,
  resolver: MissingChunkResolver,
) {
  ivmRuntime.setChunkProvider(
    chunks.StorageChunkProvider.withResolverObserverAndJournal(
      chunkStorage,
      resolver,
      new MetadataChunkInstallObserver(new WeakRef(storage), new WeakRef(lifecycle)),
      new MetadataChunkInstallJournal(new WeakRef(storage)),
    ),
  );
}

export function fromRuntimeStorage(
  ivmRuntime: IvmRuntime,
  storage: LayoutStorage,
  lifecycle: AsyncLock,
  chunkStorage: ChunkStorage,
  chunkResolver: MissingChunkResolver,
): Database {
  installChunkProvider(ivmRuntime, storage, lifecycle, chunkStorage, chunkResolver);
  const database = new Database(storage, ivmRuntime, new ReadMetricsSink(ivmRuntime.indexIds()));
  database.chunkStorage = chunkStorage;
  database.chunkResolver = chunkResolver;
  database.largeValueLifecycle = lifecycle;
  return database;
}

/** Open a schema-aware database over an ordered key/value store. */
export function openDatabase(
  schema: DatabaseSchema,
  storage: ReopenableStorage,
): Promise<Database> {
  return openDatabaseWithStorageLayout(schema, storage, "Identity");
}

export async function openDatabaseWithStorageLayout(
  schema: DatabaseSchema,
  inner: ReopenableStorage,
  layout: StorageLayout,
): Promise<Database> {
  // Must precede `LayoutStorage.create`, whose marker initialization can
  // otherwise create durable engine state for an invalid schema.
  validateApplicationStorageNames(schema);
  validateDurableKeySchema(schema);
  try {
    IvmRuntime.create(schema);
  } catch (error) {
    throw ivm(error);
  }
  const storage = await dbTry(LayoutStorage.create(inner, layout));
  // Durable index ids must be known before schema indexes compile.
  let ivmRuntime: IvmRuntime;
  try {
    const indexIds = await IndexIdRegistry.load(storage);
    ivmRuntime = IvmRuntime.newWithIndexIds(schema, { value: indexIds });
  } catch (error) {
    throw DbError.from(error);
  }
  const chunkStorage = new chunks.ManagedChunkStorage(
    new chunks.OrderedChunkStorage(new WeakRef(storage)),
  );
  return fromRuntimeStorage(
    ivmRuntime,
    storage,
    new AsyncLock(),
    chunkStorage,
    new chunks.UnavailableChunkResolver(),
  );
}

export class FacadeMethods {
  getDurablePublicationFrontier(this: Database): PublicationId | undefined {
    return this.durablePublicationFrontier;
  }

  residentStorage(this: Database): StagedWriteOverlay {
    return new StagedWriteOverlay(this.storage, this.residentWrites);
  }

  /** Reject any host operation after an ambiguous durable finalization. */
  ensureUsable(this: Database) {
    this.ensureNotPoisoned();
  }

  /** Arm cancellation/error poisoning before a host-owned durable mutation. */
  guardHostApplication(this: Database): HostApplicationGuard {
    this.ensureNotPoisoned();
    return new HostApplicationGuard(this.publicationPersistence, this.abandonedApplication);
  }

  approximateClassBytes(this: Database, cf: string) {
    return dbTry(this.storage.approximateClassBytes(cf));
  }

  approximateClassBytesForLogicalFamilies(this: Database, logicalFamilies: Iterable<string>) {
    return dbTry(this.storage.approximateClassBytesForLogicalFamilies(logicalFamilies));
  }

  /** Register the database owner's wake while external publications settle. */
  waitForPublicationSettlement(this: Database, waker?: Waker): boolean {
    this.ensureNotPoisoned();
    if (!this.hasUnsettledPublications()) return false;
    if (waker) this.publicationPersistence.ownerWaiter = waker;
    return true;
  }

  hasUnsettledPublications(this: Database): boolean {
    return this.residentPublications.size > 0;
  }

  /** Finish the durable obligations of a runtime that will be replaced. */
  async prepareForStorageExtraction(this: Database): Promise<void> {
    this.ensureNotPoisoned();
    if (this.residentPublications.size > 0) throw new DbError("UnsettledPublications");
    this.poisoned = true;
    const storage = new OwnedStorage(this.storage);
    await pollFn((cx) => this.ivmRuntime.pollStorageExtraction(storage, cx)).catch((error) => {
      throw DbError.from(error);
    });
    this.storageExtractionPrepared = true;
  }

  /** Replace a successfully prepared runtime over the same storage owner. */
  rebuild(this: Database, schema: DatabaseSchema) {
    if (!this.storageExtractionPrepared)
      throw new Error("database rebuild requires completed preparation");
    validateApplicationStorageNames(schema);
    validateDurableKeySchema(schema);
    let runtime: IvmRuntime;
    try {
      runtime = IvmRuntime.newWithIndexIds(schema, this.ivmRuntime.indexIds());
    } catch (error) {
      throw ivm(error);
    }
    runtime.setPlainOutputRootPositionsEnabled(this.ivmRuntime.plainOutputRootPositionsEnabled());
    const replacement = fromRuntimeStorage(
      runtime,
      this.storage,
      this.largeValueLifecycle,
      this.chunkStorage,
      this.chunkResolver,
    );
    Object.assign(this, replacement);
  }

  /** Consume this runtime and cancel reconstructible query work. */
  intoStorage(this: Database): BoxedStorage {
    if (this.residentPublications.size > 0) throw new Error("database has unsettled publications");
    if (this.ivmRuntime.hasPendingStorageWrites())
      throw new Error("database has pending durable writes");
    this.ivmRuntime.dispose?.();
    return this.storage.intoInner();
  }

  close(this: Database) {
    return dbTry(this.storage.close());
  }
  setWriteFlushCadence(this: Database, every: number) {
    return dbTry(this.storage.setWriteFlushCadence(every));
  }
  flushWriteBoundary(this: Database) {
    this.ensureNotPoisoned();
    return dbTry(this.storage.flushWriteBoundary());
  }
  setAutoDirectFamilyEnabled(this: Database, enabled: boolean) {
    this.ivmRuntime.setAutoDirectFamilyEnabled(enabled);
  }
  setPlainOutputRootPositionsEnabled(this: Database, enabled: boolean) {
    this.ivmRuntime.setPlainOutputRootPositionsEnabled(enabled);
  }
  setChunkProvider(this: Database, provider: any) {
    this.ivmRuntime.setChunkProvider(provider);
  }
  setOwnedChunkProvider(this: Database, provider: any) {
    this.ivmRuntime.setOwnedChunkProvider(provider);
  }
  ownedChunkProvider(this: Database) {
    return this.ivmRuntime.chunkProvider();
  }
  localChunkReader(this: Database) {
    return new chunks.LocalChunkReader(this.chunkStorage);
  }
  setChunkStorage(this: Database, storage: ChunkStorage) {
    installChunkProvider(
      this.ivmRuntime,
      this.storage,
      this.largeValueLifecycle,
      storage,
      this.chunkResolver,
    );
    this.chunkStorage = storage;
  }
  setMissingChunkResolver(this: Database, resolver: MissingChunkResolver) {
    installChunkProvider(
      this.ivmRuntime,
      this.storage,
      this.largeValueLifecycle,
      this.chunkStorage,
      resolver,
    );
    this.chunkResolver = resolver;
  }

  // ---- large-value staging ----

  /** Prepare and stage a complete logical value entirely inside Groove. */
  async prepareAndStageLargeValue(
    this: Database,
    kind: any,
    bytes: Bytes,
  ): Promise<StagedLargeValue> {
    let prepared;
    try {
      prepared = lv.prepare(kind, bytes);
    } catch (error) {
      throw ivm(error);
    }
    return this.stageLargeValuePreparation(prepared);
  }

  async stageLargeValuePreparation(this: Database, prepared: any): Promise<StagedLargeValue> {
    const uploadId = uuidV4();
    await this.stageLargeValueChunkBatch(uploadId, prepared.valueRef.kind, prepared.stagedChunks);
    return this.finalizeLargeValueUpload(uploadId, prepared.valueRef);
  }

  /** Stage a descriptor Groove derived from an already authenticated local value. */
  async stageDerivedLargeValuePreparation(this: Database, base: LargeValueRef, prepared: any) {
    const uploadId = uuidV4();
    await this.stageLargeValueChunkBatch(uploadId, prepared.valueRef.kind, prepared.stagedChunks);
    await this.bindPendingUploadDescriptor(uploadId, prepared.valueRef);
    const staged = await this.finalizeLargeValueUploadWithPresence(uploadId, prepared.valueRef, {
      kind: "DerivedFrom",
      base,
    });
    if (!staged) throw lvMeta("pending upload is missing");
    return staged;
  }

  async stageLargeValueChunkBatch(
    this: Database,
    uploadId: Bytes,
    kind: any,
    chunkBatch: any[],
  ): Promise<void> {
    await this.stageLargeValueChunkBatchWithPresenceAndPendingLimit(
      uploadId,
      kind,
      chunkBatch,
      false,
      undefined,
    );
  }

  stageLargeValueChunkBatchIfCurrent(
    this: Database,
    uploadId: Bytes,
    kind: any,
    chunkBatch: any[],
  ) {
    return this.stageLargeValueChunkBatchWithPresenceAndPendingLimit(
      uploadId,
      kind,
      chunkBatch,
      true,
      undefined,
    );
  }

  async stageLargeValueChunkBatchWithPresenceAndPendingLimit(
    this: Database,
    uploadId: Bytes,
    kind: any,
    chunkBatch: any[],
    requireExisting: boolean,
    pendingLimit: number | undefined,
  ): Promise<boolean> {
    const release = await this.largeValueLifecycle.lock();
    try {
      if (await completedLargeValueUploadReceipt(this.storage, uploadId)) return false;
      // Must precede both chunk staging and metadata mutation.
      try {
        lv.validateStagedChunkBatch(kind, chunkBatch);
      } catch (error) {
        throw ivm(error);
      }
      const key = pendingLargeValueUploadKey(uploadId);
      const encoded = await get(this.storage, key);
      let upload: PendingLargeValueUpload;
      if (encoded) upload = decodePendingLargeValueUploadAtKey(key, encoded);
      else {
        if (requireExisting) return false;
        if (
          pendingLimit !== undefined &&
          (await this.pendingLargeValueUploadLimitReached(pendingLimit))
        )
          throw new DbError("PendingLargeValueUploadLimitExceeded", { limit: pendingLimit });
        upload = {
          id: uploadId,
          descriptor: undefined,
          receiptId: undefined,
          accounting: { encodedBytes: 0, nodeCount: 0 },
          createdAtMs: nowMs(),
          chunks: [],
        } as PendingLargeValueUpload;
      }
      const newMembers = new SortedMap<NodeRef, true>(cmpNodeRef);
      for (const chunk of chunkBatch) {
        if (upload.chunks.some((c) => cmpNodeRef(c, chunk.nodeRef) === 0)) continue;
        upload.accounting.encodedBytes += chunk.encoded.length;
        if (!Number.isSafeInteger(upload.accounting.encodedBytes))
          throw lvMeta("upload byte count overflow");
        upload.accounting.nodeCount += 1;
        if (!Number.isSafeInteger(upload.accounting.nodeCount))
          throw lvMeta("upload node count overflow");
        upload.chunks.push(chunk.nodeRef);
        newMembers.set(chunk.nodeRef, true);
      }
      const operations = [set(key, encodePendingLargeValueUpload(upload))];
      for (const chunk of chunkBatch) {
        if (!newMembers.delete(chunk.nodeRef)) continue;
        const nodeKey = largeValueNodeKey(chunk.nodeRef);
        const existing = await get(this.storage, nodeKey);
        let metadata: LargeValueNodeReferences;
        if (existing) {
          metadata = decodeLargeValueNodeReferences(existing);
          if (metadata.uploadReferences >= 0xffffffff)
            throw lvMeta("upload reference count overflow");
          metadata.uploadReferences += 1;
        } else {
          let node;
          try {
            node = lv.decodeAuthenticatedNode(chunk.nodeRef.objectHash, chunk.encoded);
          } catch (error) {
            throw lvMeta(`cannot decode pushed chunk metadata: ${error}`);
          }
          metadata = {
            references: 0,
            uploadReferences: 1,
            children: uniqueLargeValueChildren(node),
          };
        }
        operations.push(set(nodeKey, encodeLargeValueNodeReferences(metadata)));
      }
      // The pending-upload record is a durable intent journal committed before
      // any separate blob backend put.
      await dbTry(this.storage.writeMany(operations));
      try {
        await this.chunkStorage.stage(chunkBatch);
      } catch (error) {
        throw ivm(ChunkError.from(error));
      }
      return true;
    } finally {
      release();
    }
  }

  async pendingLargeValueUploadLimitReached(this: Database, limit: number): Promise<boolean> {
    if (limit === 0) return true;
    const cursor = await dbTry(this.storage.scan(scanPrefix(md, utf8("upload/"))));
    let count = 0;
    for (let batch; (batch = await dbTry(cursor.nextBatch())); ) {
      count += batch.length;
      if (count >= limit) return true;
    }
    return false;
  }

  descriptorUploadId(this: Database, valueRef: LargeValueRef): Bytes {
    let encoded: Bytes;
    try {
      encoded = encodeLargeValueRef(valueRef);
    } catch (error) {
      throw lvMeta(`cannot encode upload descriptor: ${error}`);
    }
    return deriveKey("groove pending descriptor upload v1", encoded).slice(0, 16);
  }

  /** Start or resume a descriptor-keyed root-first upload. */
  beginLargeValueUpload(this: Database, valueRef: LargeValueRef, pendingLimit?: number) {
    return this.beginLargeValueUploadInner(valueRef, pendingLimit);
  }

  async beginLargeValueUploadInner(
    this: Database,
    valueRef: LargeValueRef,
    pendingLimit?: number,
  ): Promise<any> {
    const uploadId = this.descriptorUploadId(valueRef);
    let staged = await completedLargeValueUpload(this.storage, uploadId, valueRef);
    if (staged) return { kind: "Staged", staged };
    const fresh = await this.stageLargeValueChunkBatchWithPresenceAndPendingLimit(
      uploadId,
      valueRef.kind,
      [],
      false,
      pendingLimit,
    );
    if (!fresh && (staged = await completedLargeValueUpload(this.storage, uploadId, valueRef)))
      return { kind: "Staged", staged };
    await this.bindPendingUploadDescriptor(uploadId, valueRef);
    const progress = await this.largeValueUploadProgress(uploadId, valueRef, false);
    if (!progress) throw lvMeta("pending upload is missing");
    return progress;
  }

  /** Bind a descriptor-keyed peer upload before the first frontier is disclosed. */
  async bindPendingUploadDescriptor(
    this: Database,
    uploadId: Bytes,
    valueRef: LargeValueRef,
  ): Promise<void> {
    const release = await this.largeValueLifecycle.lock();
    try {
      const key = pendingLargeValueUploadKey(uploadId);
      const encoded = await get(this.storage, key);
      if (!encoded) throw lvMeta("pending upload is missing");
      const upload = decodePendingLargeValueUploadAtKey(key, encoded);
      if (upload.descriptor) {
        if (sameRef(upload.descriptor, valueRef)) return;
        throw lvMeta("pending upload is bound to a different descriptor");
      }
      upload.descriptor = valueRef;
      await dbTry(this.storage.writeMany([set(key, encodePendingLargeValueUpload(upload))]));
    } finally {
      release();
    }
  }

  /** Install receiver-requested nodes and derive the next missing frontier. */
  async continueLargeValueUpload(this: Database, valueRef: LargeValueRef, chunkBatch: any[]) {
    const progress = await this.continueLargeValueUploadWithPresence(valueRef, chunkBatch, false);
    if (!progress) throw lvMeta("pending upload is missing");
    return progress;
  }

  continueLargeValueUploadIfCurrent(this: Database, valueRef: LargeValueRef, chunkBatch: any[]) {
    return this.continueLargeValueUploadWithPresence(valueRef, chunkBatch, true);
  }

  async continueLargeValueUploadWithPresence(
    this: Database,
    valueRef: LargeValueRef,
    chunkBatch: any[],
    requireExisting: boolean,
  ): Promise<any> {
    const FRONTIER_LIMIT = 64;
    const uploadId = this.descriptorUploadId(valueRef);
    const done = await completedLargeValueUpload(this.storage, uploadId, valueRef);
    if (done) return { kind: "Staged", staged: done };
    let requested: NodeRef[];
    try {
      requested = await lv.missingUploadFrontier(valueRef, this.localChunkReader(), FRONTIER_LIMIT);
    } catch (error: any) {
      // A corrupt locally durable chunk is terminal; availability stays retryable.
      if (
        error?.kind === "LargeValue" ||
        (error?.kind === "Chunk" && error.chunk?.kind === "Integrity")
      )
        return this.rejectLargeValueUpload(uploadId, ivm(error.inner ?? error.chunk));
      throw ivm(error.chunk ?? error);
    }
    if (chunkBatch.length === 0 && requested.length > 0)
      return this.rejectLargeValueUpload(uploadId, lvMeta("upload supplied no requested nodes"));
    const newChunks: any[] = [];
    for (const chunk of chunkBatch) {
      if (requested.some((r) => cmpNodeRef(r, chunk.nodeRef) === 0)) {
        newChunks.push(chunk);
        continue;
      }
      // A stale response is idempotent only when byte-for-byte the stored node.
      const alreadyStored = await this.localChunkReader()
        .get(chunk.nodeRef.locator, chunk.nodeRef.objectHash)
        .then((encoded: Bytes) => equalBytes(encoded, chunk.encoded))
        .catch(() => false);
      if (!alreadyStored)
        return this.rejectLargeValueUpload(
          uploadId,
          lvMeta("upload supplied a node outside the authenticated missing frontier"),
        );
    }
    try {
      lv.validateStagedChunkBatch(valueRef.kind, newChunks);
    } catch (error) {
      return this.rejectLargeValueUpload(uploadId, ivm(error));
    }
    let staged: boolean;
    try {
      staged = requireExisting
        ? await this.stageLargeValueChunkBatchIfCurrent(uploadId, valueRef.kind, newChunks)
        : (await this.stageLargeValueChunkBatch(uploadId, valueRef.kind, newChunks), true);
    } catch (error) {
      if (!isRetryableUploadError(error)) return this.rejectLargeValueUpload(uploadId, error);
      throw error;
    }
    if (!staged) {
      const completed = await completedLargeValueUpload(this.storage, uploadId, valueRef);
      return completed ? { kind: "Staged", staged: completed } : undefined;
    }
    await this.bindPendingUploadDescriptor(uploadId, valueRef);
    return this.largeValueUploadProgress(uploadId, valueRef, requireExisting);
  }

  async largeValueUploadProgress(
    this: Database,
    uploadId: Bytes,
    valueRef: LargeValueRef,
    requireExisting: boolean,
  ): Promise<any> {
    let missing: NodeRef[];
    try {
      missing = await lv.missingUploadFrontier(valueRef, this.localChunkReader(), 64);
    } catch (error: any) {
      const converted = ivm(error.inner ?? error.chunk ?? error);
      if (!isRetryableUploadError(converted))
        return this.rejectLargeValueUpload(uploadId, converted);
      throw converted;
    }
    if (missing.length > 0) return { kind: "Missing", missing };
    try {
      await this.validateCompletedLargeValue(valueRef);
    } catch (error) {
      if (!isRetryableUploadError(error)) return this.rejectLargeValueUpload(uploadId, error);
      throw error;
    }
    let staged: StagedLargeValue | undefined;
    try {
      staged = requireExisting
        ? await this.finalizeLargeValueUploadIfCurrent(uploadId, valueRef)
        : await this.finalizeLargeValueUpload(uploadId, valueRef);
    } catch (error) {
      if (isRetryableUploadError(error)) throw error;
      return this.rejectLargeValueUpload(uploadId, error);
    }
    return staged && { kind: "Staged", staged };
  }

  async rejectLargeValueUpload(this: Database, uploadId: Bytes, error: unknown): Promise<never> {
    await this.evictPendingLargeValueUpload(uploadId);
    throw error;
  }

  /** Recheck the complete final logical scalar before a remote upload becomes publishable. */
  async validateCompletedLargeValue(this: Database, value: LargeValueRef): Promise<void> {
    await this.validateLargeValueEditTail(value);
    try {
      const validator = new lv.LogicalValueValidator(value);
      let offset = 0;
      while (offset < value.byteLength) {
        const end = Math.min(offset + lv.LEAF_MIN_BYTES, value.byteLength);
        const bytes = await this.readLargeValueRange(value, offset, end);
        lv.recordFinalizeValidationBytes(bytes.length);
        validator.push(bytes);
        offset = end;
      }
      validator.finish(value);
    } catch (error) {
      throw DbError.from(error);
    }
  }

  /** Run a chunk-demanding attempt, fetching each blocked request's chunks. */
  async attemptWithChunks<T>(
    this: Database,
    inputs: EvaluationInputs,
    attempt: (inputs: EvaluationInputs) => T,
    onBytes?: (bytes: any) => void,
  ): Promise<T> {
    const provider = this.ivmRuntime.chunkProvider();
    for (;;) {
      try {
        return attempt(inputs);
      } catch (error: any) {
        if (error?.kind !== "EvaluationBlocked") throw ivm(error);
        const requests = inputs.takeMissingChunks();
        if (requests.length === 0) throw ivm(error);
        for (const request of requests) {
          let bytes;
          try {
            bytes = await provider.get(request);
          } catch (e) {
            throw ivm(e);
          }
          onBytes?.(bytes);
          inputs.installChunkFromProvider(request, bytes);
        }
      }
    }
  }

  /** Replay a descriptor's edit tail against its immutable base tree. */
  async validateLargeValueEditTail(this: Database, value: LargeValueRef): Promise<void> {
    await this.attemptWithChunks(
      new EvaluationInputs(),
      (inputs) => lv.validateEditTailAttempt(value, inputs),
      (bytes) => lv.recordFinalizeValidationBytes(bytes.bytes().length),
    );
  }

  /** Finalize a streamed push upload into an opaque persisted staging receipt. */
  async finalizeLargeValueUpload(this: Database, uploadId: Bytes, valueRef: LargeValueRef) {
    const staged = await this.finalizeLargeValueUploadWithPresence(uploadId, valueRef, {
      kind: "Full",
    });
    if (!staged) throw lvMeta("pending upload is missing");
    return staged;
  }

  finalizeLargeValueUploadIfCurrent(this: Database, uploadId: Bytes, valueRef: LargeValueRef) {
    return this.finalizeLargeValueUploadWithPresence(uploadId, valueRef, { kind: "Full" });
  }

  async finalizeLargeValueUploadWithPresence(
    this: Database,
    uploadId: Bytes,
    valueRef: LargeValueRef,
    validation: UploadValidation,
  ): Promise<StagedLargeValue | undefined> {
    const release = await this.largeValueLifecycle.lock();
    try {
      const done = await completedLargeValueUpload(this.storage, uploadId, valueRef);
      if (done) return done;
      const key = pendingLargeValueUploadKey(uploadId);
      const encoded = await get(this.storage, key);
      if (!encoded) return undefined;
      const upload = decodePendingLargeValueUploadAtKey(key, encoded);
      if (upload.descriptor && !sameRef(upload.descriptor, valueRef))
        throw lvMeta("pending upload is bound to a different descriptor");
      const uploadedChunks = upload.chunks;
      // JSON validity is whole-document, and an inactive base cannot pass on validity.
      const derived =
        validation.kind === "DerivedFrom" &&
        upload.descriptor !== undefined &&
        valueRef.kind !== "Json" &&
        (await largeValueRootIsActive(this.storage, validation.base.root)) &&
        (await lv
          .validateDerivedUpload(valueRef, validation.base, this.localChunkReader(), uploadedChunks)
          .catch((error: unknown) => {
            throw DbError.from(error);
          }));
      if (derived) await this.validateLargeValueEditTail(valueRef);
      else {
        await lv
          .validateFinalizedUpload(
            valueRef,
            this.localChunkReader(),
            uploadedChunks,
            upload.descriptor !== undefined,
          )
          .catch((error: unknown) => {
            throw DbError.from(error);
          });
        await this.validateCompletedLargeValue(valueRef);
      }

      // Persist the exact descriptor and retry receipt before promotion.
      const receiptId = upload.receiptId ?? uuidV4();
      if (!upload.descriptor || !upload.receiptId) {
        const bound = { ...upload, descriptor: valueRef, receiptId };
        await dbTry(this.storage.writeMany([set(key, encodePendingLargeValueUpload(bound))]));
      }

      // Publishing the receipt and consuming its journal are one durable transition.
      const promotionState = StagedWriteState.from(
        await pendingLargeValueUploadReleaseOperations(this.storage, key, upload),
      );
      const promotionOverlay = new StagedWriteOverlay(this.storage, promotionState);
      const [staged, registration] = await stagedLargeValueRegistrationOperations(
        promotionOverlay,
        receiptId,
        valueRef,
        upload.accounting,
      );
      promotionState.extend(registration);
      const completed = encodeCompletedLargeValueUpload({
        id: uploadId,
        receiptId,
        descriptor: staged.valueRef,
        accounting: staged.accounting,
      });
      promotionState.extend([
        set(completedLargeValueUploadKey(uploadId), completed),
        set(completedLargeValueReceiptKey(receiptId), uploadId),
      ]);
      await dbTry(this.storage.writeMany(promotionState.intoOperations()));
      return staged;
    } finally {
      release();
    }
  }

  async releasePendingLargeValueUpload(
    this: Database,
    key: Bytes,
    upload: PendingLargeValueUpload,
  ) {
    await dbTry(
      this.storage.writeMany(
        await pendingLargeValueUploadReleaseOperations(this.storage, key, upload),
      ),
    );
  }

  /** Return restart-persistent incomplete push uploads for host expiry policy. */
  async pendingLargeValueUploads(this: Database): Promise<PendingLargeValueUpload[]> {
    return (await scanMetadata(this.storage, "upload/")).map(([k, v]) =>
      decodePendingLargeValueUploadAtKey(k, v),
    );
  }

  /** Idempotently expire one incomplete push upload. */
  async evictPendingLargeValueUpload(this: Database, id: Bytes): Promise<boolean> {
    const release = await this.largeValueLifecycle.lock();
    try {
      const key = pendingLargeValueUploadKey(id);
      const encoded = await get(this.storage, key);
      if (!encoded) return false;
      await this.releasePendingLargeValueUpload(
        key,
        decodePendingLargeValueUploadAtKey(key, encoded),
      );
      return true;
    } finally {
      release();
    }
  }

  /** Return persisted opaque staging receipts for host rate/expiry policy. */
  async stagedLargeValues(this: Database): Promise<StagedLargeValue[]> {
    return (await scanMetadata(this.storage, "staged/")).map(([k, v]) =>
      decodeStagedLargeValueAtKey(k, v),
    );
  }

  /** Idempotently evict one unaccepted staging root. */
  async evictStagedLargeValue(this: Database, id: Bytes): Promise<boolean> {
    const stagedKey = stagedLargeValueKey(id);
    // Waiting for a lock this database holds for resident publications would deadlock.
    if (this.largeValueLifecycleHeld.value) return false;
    if (!(await get(this.residentStorage(), stagedKey))) return false;
    const release = await this.largeValueLifecycle.lock();
    try {
      const encoded = await get(this.storage, stagedKey);
      if (!encoded) return false;
      const staged = decodeStagedLargeValueAtKey(stagedKey, encoded);
      const rootKey = largeValueRootKey(staged.valueRef.root);
      const rootEncoded = await get(this.storage, rootKey);
      if (!rootEncoded) throw lvMeta("staged root count is missing");
      const references = decodeLargeValueRootReferences(rootEncoded);
      if (references.staged === 0) throw lvMeta("staged root count underflow");
      references.staged -= 1;
      const deactivateRoot =
        references.staged === 0 && references.durable === 0 && references.nodeActive;
      if (deactivateRoot) references.nodeActive = false;
      const operations = [del(stagedKey), set(rootKey, encodeLargeValueRootReferences(references))];
      if (deactivateRoot)
        operations.push(
          ...(await largeValueNodeTransitionOperations(
            this.storage,
            new SortedMap<NodeRef, LargeValueNodeReferences>(cmpNodeRef),
            [[staged.valueRef.root, -1]],
            false,
          )),
        );
      operations.push(...(await completedLargeValueCleanupOperations(this.storage, id)));
      await dbTry(this.storage.writeMany(operations));
      return true;
    } finally {
      release();
    }
  }

  /** Drain work authorized by durable reference counts; resident metadata only vetoes. */
  async reclaimOrphanedLargeValueChunks(this: Database, limit: number): Promise<number> {
    const release = this.largeValueLifecycleHeld.value
      ? undefined
      : await this.largeValueLifecycle.lock();
    try {
      // Treat the whole provider request/lease population as one coarse retainer.
      const guard = this.ivmRuntime.chunkProvider().tryBeginReclamation();
      if (!guard) return 0;
      try {
        const resident = this.residentStorage();
        const scan = await dbTry(this.storage.scan(scanPrefix(md, utf8("reclaim/"))));
        let reclaimed = 0;
        for (let batch; (batch = await dbTry(scan.nextBatch())); ) {
          for (const [queueKey, encodedRef] of batch) {
            if (reclaimed >= limit) return reclaimed;
            const nodeRef = largeValueReclaimNodeRefFromKey(queueKey);
            let fromValue: NodeRef;
            try {
              fromValue = decodeNodeRef(encodedRef);
            } catch (error) {
              throw lvMeta(`cannot decode reclaim entry: ${error}`);
            }
            if (cmpNodeRef(nodeRef, fromValue) !== 0)
              throw lvMeta("reclaim entry key and value identify different nodes");
            const nodeKey = largeValueNodeKey(nodeRef);
            const point = resident.stagedPointValue(md, nodeKey);
            if (point.kind === "Delete") continue;
            const residentOverride =
              point.kind === "Set" ? decodeLargeValueNodeReferences(point.value) : undefined;
            const durableEncoded = await get(this.storage, nodeKey);
            if (!durableEncoded) continue;
            const durable = decodeLargeValueNodeReferences(durableEncoded);
            switch (largeValueReclaimDecision(durable, residentOverride ?? durable)) {
              case "ClearStaleQueue":
                await dbTry(this.storage.delete(md, queueKey));
                continue;
              case "WaitForDurableZero":
              case "ResidentVeto":
                continue;
              case "Delete":
                break;
            }
            try {
              await this.chunkStorage.delete(nodeRef.locator, nodeRef.objectHash);
            } catch (error) {
              throw ivm(ChunkError.from(error));
            }
            await dbTry(this.storage.writeMany([del(queueKey), del(nodeKey)]));
            reclaimed += 1;
          }
        }
        return reclaimed;
      } finally {
        guard.release?.();
      }
    } finally {
      release?.();
    }
  }

  // ---- large-value edits and reads ----

  /** Consolidate a bounded edit tail inside one resumable preparation. */
  async consolidateLargeValue(this: Database, value: LargeValueRef): Promise<any> {
    let continuation;
    try {
      continuation = new lv.ConsolidationContinuation(value);
    } catch (error) {
      throw ivm(error);
    }
    const inputs = new EvaluationInputs();
    for (;;) {
      const prepared = await this.attemptWithChunks(inputs, (i) => continuation.step(i));
      if (prepared) return prepared;
    }
  }

  async consolidateAndStageLargeValue(this: Database, value: LargeValueRef) {
    return this.stageDerivedLargeValuePreparation(value, await this.consolidateLargeValue(value));
  }

  /** Prepare an append, consolidating when the canonical tail limit is crossed. */
  async appendLargeValue(this: Database, value: LargeValueRef, bytes: Bytes): Promise<any> {
    let outcome;
    try {
      outcome = lv.appendTail(value, bytes);
    } catch (error) {
      throw ivm(error);
    }
    return outcome.kind === "Updated"
      ? { valueRef: outcome.valueRef, stagedChunks: [] }
      : this.consolidateLargeValue(outcome.transient);
  }

  async appendAndStageLargeValue(this: Database, value: LargeValueRef, bytes: Bytes) {
    return this.stageDerivedLargeValuePreparation(value, await this.appendLargeValue(value, bytes));
  }

  /** Prepare an arbitrary byte-coordinate splice. */
  async editLargeValue(
    this: Database,
    value: LargeValueRef,
    offset: number,
    deleteLength: number,
    insertBytes: Bytes,
  ): Promise<any> {
    const outcome = await this.attemptWithChunks(new EvaluationInputs(), (inputs) =>
      lv.replaceTailAttempt(value, offset, deleteLength, insertBytes, inputs),
    );
    return outcome.kind === "Updated"
      ? { valueRef: outcome.valueRef, stagedChunks: [] }
      : this.consolidateLargeValue(outcome.transient);
  }

  async editAndStageLargeValue(
    this: Database,
    value: LargeValueRef,
    offset: number,
    deleteLength: number,
    insertBytes: Bytes,
  ) {
    const prepared = await this.editLargeValue(value, offset, deleteLength, insertBytes);
    return this.stageDerivedLargeValuePreparation(value, prepared);
  }

  /** Read one byte-coordinate range from the final logical scalar. */
  readLargeValueRange(
    this: Database,
    value: LargeValueRef,
    start: number,
    end: number,
  ): Promise<Bytes> {
    return this.attemptWithChunks(new EvaluationInputs(), (inputs) =>
      lv.byteRangeAttempt(value, start, end, inputs),
    );
  }

  /** Consume the next bounded window of a sequential logical-value cursor. */
  async readLargeValueCursorNext(this: Database, cursor: any): Promise<Bytes | undefined> {
    const range = cursor.nextRange();
    if (!range) return undefined;
    const bytes = await this.readLargeValueRange(cursor.value(), range.start, range.end);
    cursor.advanceTo(range.end);
    return bytes;
  }

  /** Compute a BLAKE3 checksum without materializing the complete logical value. */
  async checksumLargeValueStreaming(
    this: Database,
    value: LargeValueRef,
    windowBytes: number,
    maxBytesPerTurn: number,
  ): Promise<[Bytes, any]> {
    try {
      const operator = new lv.StreamingChecksum(value, windowBytes, maxBytesPerTurn);
      while (operator.cursor().remainingBytes() !== 0) {
        const range = operator.cursor().nextRange();
        if (!range) throw new Error("non-complete cursor has a next range");
        const window = await this.readLargeValueRange(
          operator.cursor().value(),
          range.start,
          range.end,
        );
        if (operator.consumeWindow(window)) {
          await cooperativeYieldOnce();
          operator.recordYield();
        }
      }
      return operator.finish();
    } catch (error) {
      throw DbError.from(error);
    }
  }

  /** Read a UTF-16 code-unit range from a large string or JSON value. */
  async readLargeTextUtf16Range(
    this: Database,
    value: LargeValueRef,
    start: number,
    end: number,
  ): Promise<string> {
    const bytes = await this.attemptWithChunks(new EvaluationInputs(), (inputs) =>
      lv.utf16RangeAttempt(value, start, end, inputs),
    );
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    } catch {
      throw ivm(lv.Error.InvalidUtf8);
    }
  }

  /** Resolve a final logical UTF-16 position to its byte position. */
  largeTextUtf16OffsetToByte(
    this: Database,
    value: LargeValueRef,
    offset: number,
  ): Promise<number> {
    return this.attemptWithChunks(new EvaluationInputs(), (inputs) =>
      lv.utf16OffsetToByteAttempt(value, offset, inputs),
    );
  }

  /** Resolve a JSON Pointer against literal validated JSON source. */
  async readLargeJsonPointer(
    this: Database,
    value: LargeValueRef,
    pointer: string,
  ): Promise<unknown> {
    if (value.kind !== "Json") throw ivm(lv.Error.InvalidJson);
    // JSON validity is a write-admission invariant; parse only the demanded source.
    let source = new Uint8Array();
    const append = (b: Bytes) => {
      const next = new Uint8Array(source.length + b.length);
      next.set(source);
      next.set(b, source.length);
      source = next;
    };
    let offset = 0;
    while (offset < value.byteLength) {
      const end = Math.min(offset + lv.LEAF_MIN_BYTES, value.byteLength);
      append(await this.readLargeValueRange(value, offset, end));
      let prefix;
      try {
        prefix = lv.jsonPointerPrefix(source, pointer);
      } catch (error) {
        throw ivm(error);
      }
      if (prefix.kind === "Found") return prefix.value;
      if (prefix.kind === "NeedMore") offset = end;
      else {
        if (end < value.byteLength)
          append(await this.readLargeValueRange(value, end, value.byteLength));
        break;
      }
    }
    let json: unknown;
    try {
      json = JSON.parse(new TextDecoder().decode(source));
    } catch {
      throw ivm(lv.Error.InvalidJson);
    }
    return jsonPointer(json, pointer);
  }

  // ---- runtime stats and stores ----

  setTickRuntimeStatsEnabled(this: Database, enabled: boolean) {
    this.ivmRuntime.setTickRuntimeStatsEnabled(enabled);
  }
  runtimeStats(this: Database) {
    return this.ivmRuntime.stats();
  }
  executionLayoutStats(this: Database) {
    return this.ivmRuntime.executionLayoutStats();
  }
  durableIndicesStoreWithStorage(
    this: Database,
    storage: OrderedKvStorage,
    descriptor: RecordDescriptor,
  ) {
    return new RecordStore(storage, "indices", descriptor);
  }
  openBatch(this: Database): DatabaseBatch {
    return new DatabaseBatch();
  }
  preparedBindingSourceIsBound(this: Database, shape: string): boolean {
    return this.ivmRuntime.preparedBindingSourceIsBound(shape);
  }

  /** Write a direct metadata batch while advancing retained query work. */
  async writeDirectRecordsWithProgress(
    this: Database,
    name: string,
    operations: DirectRecordStoreWrite[],
    progressWaker?: Waker,
  ): Promise<void> {
    this.ensureNotPoisoned();
    const encoded = this.directRecordStore(name).encodeWrites(operations);
    let settled: { error?: unknown } | undefined;
    let wake: Waker | undefined;
    this.storage.writeMany(encoded).then(
      () => ((settled = {}), wake?.()),
      (error) => ((settled = { error }), wake?.()),
    );
    await pollFn<void>((cx) => {
      if (settled) {
        if (settled.error !== undefined) throw DbError.from(settled.error);
        return undefined;
      }
      // A chunk install retained by IVM can hold the backend mutation gate;
      // drive that owner while waiting.
      wake = cx;
      const bridge: Waker = progressWaker
        ? () => {
            cx();
            progressWaker();
          }
        : cx;
      this.pollProgress(bridge);
      return PENDING_VOID;
    });
  }

  /** Return a typed handle for a schema-declared direct record store. */
  directRecordStore(this: Database, name: string): DirectRecordStore {
    const schema = this.directRecordStoreSchema(name);
    return new DirectRecordStore(
      this.storage,
      schema.name,
      schema.keyDescriptor(),
      schema.valueDescriptor(),
    );
  }
}

const PENDING_VOID = PENDING as any;

function jsonPointer(json: any, pointer: string): unknown {
  if (pointer === "") return json;
  if (!pointer.startsWith("/")) return undefined;
  let target = json;
  for (const raw of pointer.slice(1).split("/")) {
    const token = raw.replace(/~1/g, "/").replace(/~0/g, "~");
    if (target === null || typeof target !== "object" || !(token in target)) return undefined;
    target = target[token];
  }
  return target;
}
