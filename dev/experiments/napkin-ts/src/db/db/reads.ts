// Prepared, one-shot, relation, and result-tree read APIs.
import * as X from "../../_extern/db-core.js";
import { SortedSet, cmpStr } from "../../support/collections.js";
import { yieldNow } from "../../support/runtime.js";
import type { AuthorSubject, CurrentRow, SyncMessage, TxId, Value } from "../../_extern/db-core.js";
import type { Db, QueryAttachment, ReadOpts } from "../db.js";
import type { ResultTree } from "../result_tree.js";
import {
  PreparedQuery,
  dbError,
  ensureDefaultReadView,
  ensureSupportedReadView,
  materializeResultTree,
  readCoverageTier,
  remoteSubscriptionTier,
  shouldInstallPreparedPlan,
  type DbError,
  type SubscriptionEvent,
} from "../db.js";

/**
 * Binding-only classification for a value whose referenced immutable content
 * has not arrived locally yet.
 */
export type BindingHydrationError =
  | { kind: "RetryableChunkUnavailable"; retryAfterMs: number }
  | { kind: "Error"; error: DbError };

class HydrationFailure extends Error {
  constructor(public outcome: BindingHydrationError) {
    super("binding hydration failed");
  }
}

/** Coverage attachment released through `release` when dropped. */
class SerializedReadCoverage {
  constructor(
    public attachment: QueryAttachment | undefined,
    public release: ((a: QueryAttachment) => void) | undefined,
  ) {}
  drop(): void {
    const attachment = this.attachment;
    const release = this.release;
    this.attachment = this.release = undefined;
    if (attachment && release) release(attachment);
  }
}

export function bindingHydrationError(error: X.NodeError): BindingHydrationError {
  const retryAfterMs = X.nodeErrorRetryableChunkAfterMs(error);
  if (retryAfterMs !== undefined) return { kind: "RetryableChunkUnavailable", retryAfterMs };
  if (X.nodeErrorIsChunkUnavailable(error))
    return {
      kind: "Error",
      error: dbError("NotObserved", "large-value chunk is permanently unavailable"),
    };
  return { kind: "Error", error: X.apiErrorFromNode(error) };
}

const durGe = (a: X.DurabilityTier, b: X.DurabilityTier) =>
  X.durabilityRank(a) >= X.durabilityRank(b);

/** Resolve once `poll` yields a value; re-polls on each scheduler turn. */
async function pollUntil<T>(poll: () => T | undefined): Promise<T> {
  for (;;) {
    const v = poll();
    if (v !== undefined) return v;
    await yieldNow();
  }
}

async function withNode<T>(db: Db, f: (node: X.NodeState) => Promise<T> | T): Promise<T> {
  const mutex = db.node.node;
  const node = await mutex.lock();
  try {
    return await f(node);
  } finally {
    mutex.unlock();
  }
}

function borrowNode(db: Db): X.NodeState {
  const n = db.node.node.tryLock();
  if (!n) throw new Error("synchronous node operation reentered a suspended operation");
  return n;
}

const decodeQuery = (query: Uint8Array): X.Query => {
  try {
    return X.decodePostcardExactQuery(query);
  } catch (error) {
    throw dbError("Query", `decode query: ${error}`);
  }
};

const coverageTimeout = () => dbError("NotObserved", "Timed out waiting for query coverage");

export type SerializedReadResult =
  | { kind: "Rows"; rows: CurrentRow[] }
  | { kind: "Relation"; snapshot: X.RelationSnapshot };

type RequestScope = [AuthorSubject, Map<string, Value>] | undefined;

export const readMethods = {
  /** Start a query rooted at `table`. */
  table(this: Db, table: string): X.Query {
    return X.Query.from(table);
  },

  /** Prepare a query for repeated reads or subscriptions. */
  prepareQuery(this: Db, query: X.Query): PreparedQuery {
    return this.prepareQueryBound(query, new Map());
  },

  /** Prepare a query after asynchronously acquiring the node owner. */
  prepareQueryAsync(this: Db, query: X.Query): Promise<PreparedQuery> {
    return this.prepareQueryAsyncFor(query, this.schemaViewIsFixed);
  },

  prepareQueryForOpenSchemaAsync(this: Db, query: X.Query): Promise<PreparedQuery> {
    return this.prepareQueryAsyncFor(query, true);
  },

  async prepareQueryAsyncFor(
    this: Db,
    query: X.Query,
    openSchema: boolean,
  ): Promise<PreparedQuery> {
    this.ensureOpenSchemaAdmitted();
    return withNode(this, async (node) => {
      let schema: X.JazzSchema;
      let schemaVersion: X.SchemaVersionId;
      if (openSchema) {
        schema = this.schema;
        schemaVersion = this.schemaVersionId;
      } else {
        const current = node.currentWriteSchema();
        if (X.schemaVersionEq(current.schema, this.schemaVersionId)) schema = this.schema;
        else {
          const entry = node.catalogueSchemas().get(current.schema);
          if (!entry) throw dbError("Schema", "current write schema is missing from catalogue");
          schema = entry.schema;
        }
        schemaVersion = current.schema;
      }
      const shape = query.validateWithSchemaVersion(schema, schemaVersion);
      const binding = shape.bind(new Map());
      const [localPlan, globalPlan] = await preparedPlans(node, shape, binding);
      return new PreparedQuery(
        undefined,
        shape,
        binding,
        localPlan,
        globalPlan,
        node.grooveRuntimeToken(),
      );
    });
  },

  /** Normalize relation syntax and prepare it. */
  async prepareRelationQueryAsync(this: Db, query: X.RelationQuery): Promise<PreparedQuery> {
    return this.prepareQueryAsync(X.relationQueryToQuery(query));
  },

  /** Decode and prepare the canonical serialized query accepted by host bindings. */
  async prepareSerializedQueryAsync(
    this: Db,
    query: Uint8Array,
    requestScope: RequestScope,
  ): Promise<PreparedQuery> {
    const decoded = decodeQuery(query);
    const prepared: PreparedQuery = await this.prepareQueryAsync(decoded);
    return requestScope ? prepared.withIdentityClaims(requestScope[0], requestScope[1]) : prepared;
  },

  /** Execute the complete serialized-query path for a host binding. */
  async allSerializedQuery(
    this: Db,
    query: Uint8Array,
    optsIn: ReadOpts,
    openTx: X.OpenTransactionId | undefined,
    requestScope: RequestScope,
    author: AuthorSubject | undefined,
    requireCoverage: boolean,
    coverageExpired: () => boolean,
    releaseCoverage: (a: QueryAttachment) => void,
  ): Promise<SerializedReadResult> {
    const coverageTier = readCoverageTier(optsIn);
    const firstLoad = optsIn.firstLoad;
    const opts: ReadOpts = { ...optsIn, firstLoad: "Deliver" };
    const gated =
      openTx === undefined &&
      author === undefined &&
      opts.tier !== "LocalOnly" &&
      coverageTier === "Local";
    const waitTimeout =
      gated &&
      typeof firstLoad === "object" &&
      firstLoad.kind === "WaitForRemote" &&
      firstLoad.timeoutMs > 0
        ? firstLoad.timeoutMs
        : undefined;
    if (waitTimeout !== undefined) {
      return this.readLocalFirstWithin(
        waitTimeout,
        () =>
          this.allSerializedQueryOnce(
            query,
            opts,
            coverageTier,
            undefined,
            requestScope,
            undefined,
            requireCoverage,
            coverageExpired,
            releaseCoverage,
          ),
        () =>
          this.allSerializedQueryOnce(
            query,
            { ...opts },
            "Global",
            undefined,
            requestScope,
            undefined,
            true,
            coverageExpired,
            releaseCoverage,
          ),
      );
    }
    return this.allSerializedQueryOnce(
      query,
      opts,
      coverageTier,
      openTx,
      requestScope,
      author,
      requireCoverage,
      coverageExpired,
      releaseCoverage,
    );
  },

  /** Wait until a serialized read's coverage attachment is covered. */
  waitForSerializedReadCoverage(
    this: Db,
    coverage: SerializedReadCoverage,
    coverageExpired: () => boolean,
  ): Promise<void> {
    const attachment = coverage.attachment;
    if (!attachment) throw new Error("live serialized read coverage");
    return pollUntil(() => {
      if (this.queryAttachmentIsCovered(attachment)) return true;
      if (coverageExpired()) throw coverageTimeout();
      return undefined;
    }).then(() => undefined);
  },

  /**
   * Order a Global (remote) read's open after the local writes it must
   * observe, so a remote read sees this node's earlier writes.
   */
  async awaitPrecedingLocalWritesOnWire(this: Db, query: X.Query): Promise<void> {
    const outbox = this.node.outbox;
    const queued: [TxId, SyncMessage | undefined][] = outbox
      .iter()
      .filter((p: any) => outbox.awaitsWire(p.txId))
      .map((p: any) => [p.txId, p.unit]);
    const settling: [TxId, Iterable<string>][] = this.node.pendingLocalPublicationTables();
    if (queued.length === 0 && settling.length === 0) return;
    const epoch = this.node.remoteLink.armUntilLoss();
    if (epoch === undefined) return;
    const readTable = this.readFootprintTable(query);
    const waiting: [TxId, SortedSet<string>][] = [];
    await withNode(this, async (node) => {
      for (const [txId, storageTables] of settling) {
        const tables = new SortedSet<string>(cmpStr);
        for (const t of node.logicalTableNamesForStorageTables([...storageTables])) tables.add(t);
        if (readFootprintOverlaps(readTable, tables)) waiting.push([txId, tables]);
      }
      for (const [txId, unit0] of queued) {
        let unit = unit0;
        if (!unit) {
          try {
            unit = await node.commitUnitFor(txId);
          } catch {
            unit = undefined;
          }
        }
        if (unit) {
          const tables = new SortedSet<string>(cmpStr);
          for (const version of X.uploadedVersions(unit)) tables.add(version.table());
          if (readFootprintOverlaps(readTable, tables)) waiting.push([txId, tables]);
        } else {
          // An unreadable commit may write anything the read reads.
          waiting.push([txId, new SortedSet<string>(cmpStr)]);
        }
      }
    });
    if (waiting.length === 0) return;
    let lastProgress: [number, number, number] | undefined;
    const onWire = pollUntil<true>(() => {
      let held = 0;
      const heldTables = new SortedSet<string>(cmpStr);
      for (const [txId, tables] of waiting) {
        const reason = outbox.uploadFailure(txId);
        if (reason !== undefined) throw localWritesNotUploaded(tables, reason);
        if (outbox.awaitsWire(txId) || this.node.isPendingLocalPublication(txId)) {
          held++;
          for (const t of tables) heldTables.add(t);
        }
      }
      if (held === 0) return true;
      const progress: number = outbox.uploadProgress();
      const now: number = this.node.uploadRetryClock.nowMs();
      if (lastProgress && lastProgress[0] === progress && lastProgress[1] === held) {
        if (Math.max(0, now - lastProgress[2]) >= LOCAL_WRITE_UPLOAD_STALL_MS)
          throw localWritesUploadStalled(heldTables);
      } else lastProgress = [progress, held, now];
      return undefined;
    });
    await this.raceRemoteAnswer(epoch, undefined, onWire);
  },

  async precedingLocalWritesOnWire(this: Db, query: Uint8Array): Promise<void> {
    return this.awaitPrecedingLocalWritesOnWire(decodeQuery(query));
  },

  /** The one table a read's result depends on, or undefined when every table counts. */
  readFootprintTable(this: Db, query: X.Query): string | undefined {
    const table = plainReadTable(query);
    if (table === undefined) return undefined;
    let schema: any;
    try {
      schema = this.tableSchema(table);
    } catch {
      return undefined;
    }
    const policy = schema.readPolicy;
    if (policy && plainReadTable(policy) !== table) return undefined;
    return table;
  },

  async allSerializedQueryOnce(
    this: Db,
    query: Uint8Array,
    opts: ReadOpts,
    coverageTier: X.DurabilityTier,
    openTx: X.OpenTransactionId | undefined,
    requestScope: RequestScope,
    author: AuthorSubject | undefined,
    requireCoverage: boolean,
    coverageExpired: () => boolean,
    releaseCoverage: (a: QueryAttachment) => void,
  ): Promise<SerializedReadResult> {
    if (requireCoverage && openTx === undefined && durGe(coverageTier, "Global"))
      await this.precedingLocalWritesOnWire(query);
    {
      let expired = false;
      const admission = this.awaitOpenSchemaForCoverage(coverageTier);
      const timeout = pollUntil(() => (coverageExpired() ? ((expired = true), true) : undefined));
      await Promise.race([admission, timeout]);
      if (expired) throw coverageTimeout();
    }
    const decoded = decodeQuery(query);
    const isRelation = decoded.relation !== undefined;
    if (openTx !== undefined && isRelation)
      throw dbError("Query", "relation reads inside a transaction are not supported");
    let prepared: PreparedQuery = await this.prepareQueryAsync(decoded);
    if (requestScope) prepared = prepared.withIdentityClaims(requestScope[0], requestScope[1]);
    const attachCoverage = async (
      q: PreparedQuery,
      tier: X.DurabilityTier,
      tx: X.OpenTransactionId | undefined,
    ) => {
      const attachment = await this.attachQueryWithCoverageAsync(q, { ...opts }, tier, tx, author);
      return new SerializedReadCoverage(attachment, releaseCoverage);
    };
    const strictClientOneShot =
      !isRelation &&
      openTx === undefined &&
      author === undefined &&
      prepared.shape().query().arraySubqueries.length === 0 &&
      durGe(coverageTier, "Global") &&
      opts.readView.isDefault() &&
      !opts.includeDeleted;
    if (requireCoverage && (isRelation || strictClientOneShot)) {
      const localCoverage =
        coverageTier === "Local"
          ? await attachCoverage(prepared, coverageTier, undefined)
          : undefined;
      const stream = await this.subscribeOneShotWithCoverage(
        prepared,
        { ...opts },
        author,
        coverageTier,
      );
      const strictCoverage = strictClientOneShot
        ? await attachCoverage(prepared, coverageTier, undefined)
        : undefined;
      let outcome: SerializedReadResult | undefined;
      let failure: unknown;
      try {
        await pollUntil(() => {
          if (coverageExpired()) throw coverageTimeout();
          for (const c of [localCoverage, strictCoverage]) {
            if (!c) continue;
            if (!c.attachment) throw new Error("live read coverage");
            if (!this.queryAttachmentIsCovered(c.attachment)) return undefined;
          }
          return true;
        });
        const event: SubscriptionEvent | undefined = await stream.nextEvent();
        if (event && event.kind === "Delta" && event.reset) {
          const snapshot = stream.settledReceiverLocalSnapshot();
          await this.hydrateRelationSnapshotForBinding(snapshot);
          outcome =
            prepared.shape().query().arraySubqueries.length === 0
              ? { kind: "Rows", rows: snapshot.rows.slice(0, snapshot.rootCount) }
              : { kind: "Relation", snapshot };
        } else if (event && event.kind === "Rejected") {
          throw dbError("Query", `query subscription rejected: ${X.debugFmt(event.reason)}`);
        } else if (!event || event.kind === "Closed") {
          throw dbError("NotObserved", "query subscription ended before its published result");
        } else {
          throw dbError("Protocol", "query subscription did not open with a reset");
        }
      } catch (e) {
        failure = e;
      }
      localCoverage?.drop();
      strictCoverage?.drop();
      try {
        await stream.close();
      } catch (e) {
        failure ??= e;
      }
      if (failure !== undefined) throw failure;
      return outcome!;
    }
    // Exclusive reads on a partial node hydrate through the authority at the
    // frozen snapshot (INV-TX-13).
    const exclusiveSnapshotRead =
      openTx !== undefined && this.node.receivesCommitsAsLocal()
        ? (await this.transactionIsExclusive(openTx)) && opts.tier !== "LocalOnly"
        : false;
    const sourceQueries: X.Query[] =
      openTx !== undefined && exclusiveSnapshotRead
        ? await this.exclusiveSourceHydrationQueries(openTx, prepared, opts.includeDeleted)
        : [];
    let coverage: SerializedReadCoverage | undefined;
    const hydrate = exclusiveSnapshotRead && !durGe(coverageTier, "Global");
    const tableCoverage: SerializedReadCoverage[] = [];
    try {
      const epoch0 = hydrate ? this.node.remoteLink.armUntilLoss() : undefined;
      if (hydrate && epoch0 !== undefined) {
        const hydration = await attachCoverage(prepared, "Global", openTx);
        const covered = this.waitForSerializedReadCoverage(hydration, coverageExpired);
        const result = await this.raceRemoteAnswer(epoch0, undefined, covered);
        if (result !== undefined) coverage = hydration;
        // The authority became unreachable: drop the pending hydration and read the replica.
        else hydration.drop();
      }
      if (coverage === undefined && requireCoverage) {
        const required = await attachCoverage(prepared, coverageTier, openTx);
        try {
          await this.waitForSerializedReadCoverage(required, coverageExpired);
        } catch (e) {
          required.drop();
          throw e;
        }
        coverage = required;
      }
      if (exclusiveSnapshotRead && coverage !== undefined && openTx !== undefined) {
        for (const q of sourceQueries) {
          const epoch = this.node.remoteLink.armUntilLoss();
          if (epoch === undefined) break;
          const sourceQuery: PreparedQuery = await this.prepareQueryAsync(q);
          const hydration = await attachCoverage(sourceQuery, "Global", openTx);
          const covered = this.waitForSerializedReadCoverage(hydration, coverageExpired);
          const result = await this.raceRemoteAnswer(epoch, undefined, covered);
          if (result === undefined) {
            hydration.drop();
            break;
          }
          tableCoverage.push(hydration);
        }
      }

      const mode: X.QueryAuthorizationMode =
        author !== undefined ? "TrustedServing" : "ClientLocal";
      if (prepared.shape().query().arraySubqueries.length > 0) {
        const snapshot: X.RelationSnapshot =
          openTx !== undefined
            ? await this.relationSnapshotInOpenTransaction(openTx, prepared, opts, author)
            : await this.relationSnapshotWithCoverage(
                prepared,
                opts,
                coverageTier,
                author ?? this.identity.author,
                mode,
              );
        if (openTx === undefined) await this.hydrateRelationSnapshotForBinding(snapshot);
        return { kind: "Relation", snapshot };
      }
      const rows: CurrentRow[] =
        openTx !== undefined
          ? await this.allInOpenTransaction(openTx, prepared, opts, author)
          : await this.allForIdentityInAuthorizationMode(
              prepared,
              opts,
              coverageTier,
              author ?? this.identity.author,
              mode,
            );
      await this.hydrateRowsForBinding(rows);
      return { kind: "Rows", rows };
    } finally {
      for (const c of tableCoverage) c.drop();
      coverage?.drop();
    }
  },

  /** Prepare a query with explicit parameter bindings. */
  prepareQueryBound(this: Db, query: X.Query, params: Map<string, Value>): PreparedQuery {
    const [schema, schemaVersion] = this.currentWriteSchemaForQuery();
    return this.prepareQueryBoundForSchema(query, params, schema, schemaVersion);
  },

  prepareQueryBoundForSchema(
    this: Db,
    query: X.Query,
    params: Map<string, Value>,
    schema: X.JazzSchema,
    schemaVersion: X.SchemaVersionId,
  ): PreparedQuery {
    this.ensureOpenSchemaAdmitted();
    const shape = query.validateWithSchemaVersion(schema, schemaVersion);
    const binding = shape.bind(params);
    const node = borrowNode(this);
    const [localPlan, globalPlan] = X.blockOn(preparedPlans(node, shape, binding));
    return new PreparedQuery(
      undefined,
      shape,
      binding,
      localPlan,
      globalPlan,
      node.grooveRuntimeToken(),
    );
  },

  /** Synchronously read rows for a prepared query (local preview). */
  read(this: Db, prepared: PreparedQuery): CurrentRow[] {
    this.ensureOpenSchemaAdmitted();
    const node = borrowNode(this);
    const token = node.grooveRuntimeToken();
    return X.blockOn(
      node.queryRowsLocalPreview(
        prepared.shape(),
        prepared.binding(),
        prepared.planForTier("Local", token),
      ),
    );
  },

  /** Synchronously read rows and attribute work inside the node query path. */
  readProfiled(this: Db, prepared: PreparedQuery): [CurrentRow[], X.QueryReadProfile] {
    this.ensureOpenSchemaAdmitted();
    const node = borrowNode(this);
    const token = node.grooveRuntimeToken();
    return X.blockOn(
      node.queryRowsLocalPreviewProfiled(
        prepared.shape(),
        prepared.binding(),
        prepared.planForTier("Local", token),
      ),
    );
  },

  /** Synchronously read exactly one local row if present. */
  one(this: Db, prepared: PreparedQuery): CurrentRow | undefined {
    return this.read(prepared)[0];
  },

  /** Resolve creator/updater provenance for a row returned by this database. */
  rowProvenance(this: Db, row: CurrentRow): X.RowProvenance | undefined {
    this.ensureOpenSchemaAdmitted();
    return borrowNode(this).rowProvenance(row);
  },

  async rowProvenanceAsync(this: Db, row: CurrentRow): Promise<X.RowProvenance | undefined> {
    this.ensureOpenSchemaAdmitted();
    return withNode(this, (node) => node.rowProvenance(row));
  },

  /** Read local settled history at an exact global timestamp cut. */
  at(this: Db, position: X.GlobalTime, prepared: PreparedQuery): CurrentRow[] {
    return this.atPrepared(position, prepared);
  },

  atPrepared(this: Db, position: X.GlobalTime, prepared: PreparedQuery): CurrentRow[] {
    this.ensureOpenSchemaAdmitted();
    return X.blockOn(borrowNode(this).at(position).read(prepared.shape(), prepared.binding()));
  },

  /** Tier-gated one-shot read. */
  all(this: Db, prepared: PreparedQuery, opts: ReadOpts): Promise<CurrentRow[]> {
    return this.allForIdentityInAuthorizationMode(
      prepared,
      opts,
      readCoverageTier(opts),
      this.identity.author,
      "ClientLocal",
    );
  },

  /** Tier-gated one-shot read evaluated by a trusted host as `author`. */
  allForIdentity(
    this: Db,
    prepared: PreparedQuery,
    opts: ReadOpts,
    author: AuthorSubject,
  ): Promise<CurrentRow[]> {
    return this.allForIdentityInAuthorizationMode(
      prepared,
      opts,
      readCoverageTier(opts),
      author,
      "TrustedServing",
    );
  },

  async allForIdentityInAuthorizationMode(
    this: Db,
    prepared: PreparedQuery,
    opts: ReadOpts,
    coverageTier: X.DurabilityTier,
    author: AuthorSubject,
    authorizationMode: X.QueryAuthorizationMode,
  ): Promise<CurrentRow[]> {
    await this.awaitOpenSchemaForCoverage(coverageTier);
    const tier =
      authorizationMode === "ClientLocal"
        ? this.clientAuthorityReadTier(coverageTier)
        : coverageTier;
    return withNode(this, async (owner) => {
      const node = prepared.scopedNode(owner, author);
      if (opts.readView.source.kind !== "Current") {
        ensureSupportedReadView(opts);
        if (opts.includeDeleted)
          throw dbError("Query", "branch views do not support include_deleted yet");
        const snapshot =
          authorizationMode === "TrustedServing"
            ? await node.queryRelationSnapshotForServingInReadView(
                prepared.shape(),
                prepared.binding(),
                tier,
                author,
                opts.readView,
              )
            : await node.queryRelationSnapshotForClient(
                prepared.shape(),
                prepared.binding(),
                tier,
                author,
                opts.readView,
              );
        return snapshot.rows.slice(0, snapshot.rootCount);
      }
      if (opts.includeDeleted)
        return node.queryRowsIncludingDeletedInAuthorizationMode(
          prepared.shape(),
          prepared.binding(),
          tier,
          undefined,
          author,
          authorizationMode,
        );
      if (authorizationMode === "TrustedServing")
        return node.queryRowsWithPreparedPlanForIdentity(
          prepared.shape(),
          prepared.binding(),
          tier,
          undefined,
          author,
        );
      // A client consumes identity-scoped rows emitted by its trusted upstream;
      // local reads must not apply policy again.
      return node.queryRowsForClient(prepared.shape(), prepared.binding(), tier, author);
    });
  },

  /** Resolve physical indirect scalars before a subscription event crosses a binding boundary. */
  async hydrateSubscriptionEventForBinding(this: Db, event: SubscriptionEvent): Promise<void> {
    try {
      await this.hydrateSubscriptionEventForBindingOutcome(event);
    } catch (e) {
      if (!(e instanceof HydrationFailure)) throw e;
      if (e.outcome.kind === "RetryableChunkUnavailable")
        throw dbError("NotObserved", "large-value chunk is temporarily unavailable");
      throw e.outcome.error;
    }
  },

  /** Like hydrateSubscriptionEventForBinding, but throws HydrationFailure carrying the retryable cause. */
  async hydrateSubscriptionEventForBindingOutcome(
    this: Db,
    event: SubscriptionEvent,
  ): Promise<void> {
    try {
      this.ensureOpenSchemaAdmitted();
    } catch (error) {
      throw new HydrationFailure({ kind: "Error", error: error as DbError });
    }
    if (event.kind !== "Delta") return;
    const { added, updated, terminalOperations } = event;
    const hydrate = async (p: Promise<unknown>) => {
      try {
        await p;
      } catch (error) {
        throw new HydrationFailure(bindingHydrationError(error));
      }
    };
    await withNode(this, async (node) => {
      if (terminalOperations.length === 0) {
        for (const output of [...added, ...updated])
          await hydrate(node.hydrateCurrentRows([output.row]));
      } else {
        // Structured terminal edits replace the row batches at this binding.
        for (const operation of terminalOperations) {
          const k = operation.edit.kind;
          if (k === "Remove" || k === "Move") continue;
          const descriptor = terminalOperationValueDescriptor(operation);
          await hydrate(node.hydrateEncodedRecord(descriptor, operation.edit.value));
        }
      }
    });
  },

  /** Resolve physical indirect scalars in ordinary row output. */
  async hydrateRowsForBinding(this: Db, rows: CurrentRow[]): Promise<void> {
    this.ensureOpenSchemaAdmitted();
    await withNode(this, (node) => node.hydrateCurrentRows(rows));
  },

  hydrateRelationSnapshotForBinding(this: Db, snapshot: X.RelationSnapshot): Promise<void> {
    return this.hydrateRowsForBinding(snapshot.rows);
  },

  /** Use the host-selected authority tier, matching subscription registration. */
  clientAuthorityReadTier(this: Db, tier: X.DurabilityTier): X.DurabilityTier {
    return durGe(tier, "Global")
      ? remoteSubscriptionTier(tier, this.node.upstreamDurabilityFloor)
      : tier;
  },

  /** Tier-gated one-shot relation read evaluated as the database identity. */
  allRelationSnapshot(
    this: Db,
    prepared: PreparedQuery,
    opts: ReadOpts,
  ): Promise<X.RelationSnapshot> {
    return this.relationSnapshotWithCoverage(
      prepared,
      opts,
      readCoverageTier(opts),
      this.identity.author,
      "ClientLocal",
    );
  },

  allRelationSnapshotForIdentity(
    this: Db,
    prepared: PreparedQuery,
    opts: ReadOpts,
    author: AuthorSubject,
  ): Promise<X.RelationSnapshot> {
    return this.relationSnapshotWithCoverage(
      prepared,
      opts,
      readCoverageTier(opts),
      author,
      "TrustedServing",
    );
  },

  async relationSnapshotWithCoverage(
    this: Db,
    prepared: PreparedQuery,
    opts: ReadOpts,
    coverageTier: X.DurabilityTier,
    author: AuthorSubject,
    authorizationMode: X.QueryAuthorizationMode,
  ): Promise<X.RelationSnapshot> {
    await this.awaitOpenSchemaForCoverage(coverageTier);
    ensureSupportedReadView(opts);
    if (opts.includeDeleted)
      throw dbError("Query", "relation snapshots do not support include_deleted yet");
    return withNode(this, (owner) => {
      const node = prepared.scopedNode(owner, author);
      return authorizationMode === "ClientLocal"
        ? node.queryRelationSnapshotForClient(
            prepared.shape(),
            prepared.binding(),
            this.clientAuthorityReadTier(coverageTier),
            author,
            opts.readView,
          )
        : node.queryRelationSnapshotForServingInReadView(
            prepared.shape(),
            prepared.binding(),
            coverageTier,
            author,
            opts.readView,
          );
    });
  },

  /** Tier-gated canonical structured result read. */
  async allResultTree(this: Db, prepared: PreparedQuery, opts: ReadOpts): Promise<ResultTree> {
    const snapshot = await this.allRelationSnapshot(prepared, opts);
    return materializeResultTree(prepared.shape().query(), snapshot);
  },

  /** Tier-gated one-shot output-changing relation read evaluated as the database identity. */
  async allRelationQuery(
    this: Db,
    query: X.RelationQuery,
    opts: ReadOpts,
  ): Promise<X.RelationSnapshot> {
    await this.awaitOpenSchemaForCoverage(readCoverageTier(opts));
    ensureDefaultReadView(opts);
    const prepared = await this.prepareRelationQueryAsync(query);
    const rows: CurrentRow[] = await this.all(prepared, opts);
    return { rootCount: rows.length, rows, edges: [] };
  },
};

/** Install the `[local, global]` system plans a prepared query reuses. */
async function preparedPlans(
  node: X.NodeState,
  shape: X.ValidatedQuery,
  binding: X.Binding,
): Promise<[X.PreparedQueryPlanHandle | undefined, X.PreparedQueryPlanHandle | undefined]> {
  if (!shouldInstallPreparedPlan(shape) || node.usesSchemaProjectedRead(shape))
    return [undefined, undefined];
  const plan = (tier: X.DurabilityTier) =>
    node.preparedQueryPlan(shape, binding, tier, X.AuthorSubjectT.SYSTEM);
  return [await plan("Local"), await plan("Global")];
}

/** Follow a terminal operation's path to the descriptor which owns `edit.value`. */
function terminalOperationValueDescriptor(operation: X.TerminalOperation): X.RecordDescriptor {
  const fail = (msg: string) =>
    new HydrationFailure({ kind: "Error", error: dbError("Protocol", msg) });
  let descriptor = operation.rootDescriptor;
  let expectCollection = true;
  for (const segment of operation.path) {
    if (expectCollection && segment.kind === "Collection") {
      const field = descriptor.fields().find((f: any) => f.name === segment.name);
      if (!field) throw fail("terminal operation references an unknown collection field");
      if (field.valueType.kind !== "Array")
        throw fail("terminal operation collection field is not an array");
      const element = field.valueType.element;
      if (element.kind !== "Record")
        throw fail("terminal operation collection does not contain records");
      descriptor = element.descriptor;
      expectCollection = false;
    } else if (!expectCollection && segment.kind === "Key") expectCollection = true;
    else if (expectCollection) throw fail("terminal operation path starts with a key");
    else throw fail("terminal operation path is missing a child key");
  }
  return descriptor;
}

/** The one table a plain read consults, or undefined when its shape reaches further. */
function plainReadTable(query: X.Query): string | undefined {
  const plain =
    query.joins.length === 0 &&
    query.flatJoin === undefined &&
    query.policyBranches.length === 0 &&
    query.reachable.length === 0 &&
    query.inherits.length === 0 &&
    query.includes.length === 0 &&
    query.arraySubqueries.length === 0 &&
    query.aggregate === undefined &&
    query.relation === undefined;
  return plain ? query.table : undefined;
}

const readFootprintOverlaps = (readTable: string | undefined, written: SortedSet<string>) =>
  readTable === undefined ? true : written.has(readTable);

/** How long a Global read waits for the local writes it must observe to make upload progress. */
const LOCAL_WRITE_UPLOAD_STALL_MS = 15_000;

const localWritesUploadStalled = (tables: SortedSet<string>) =>
  dbError(
    "NotObserved",
    `Timed out waiting for local writes to ${quotedTables(tables)} to upload before a remote read: no upload progress for ${LOCAL_WRITE_UPLOAD_STALL_MS / 1_000} s`,
  );

const quotedTables = (tables: SortedSet<string>) =>
  tables.size === 0 ? "its tables" : [...tables].map((t) => `\`${t}\``).join(", ");

const localWritesNotUploaded = (tables: SortedSet<string>, reason: string) =>
  dbError(
    "NotObserved",
    `Remote read waits on local writes to ${quotedTables(tables)} that could not be uploaded (${reason})`,
  );
