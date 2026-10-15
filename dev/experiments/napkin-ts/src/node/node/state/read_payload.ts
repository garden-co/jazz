// Schema/table lookup, lens path routing, node aliases, row-version payload repair and the HLC clock.
import {
  type NodeUuid,
  type RowUuid,
  type SchemaVersionId,
  type TxId,
  type TxTime,
  NodeAliasRowRecord,
  RowVersionRef,
  TxIdNew,
  TxTimeOps,
  VersionRecord,
  expandVersionCarriers,
  schemaIdKey,
  transactionWithoutPermissionSubject,
  txIdKey,
  uuidEquals,
  validateReceivedViewBundleGlobalTimeDurability,
  validateVersionRecords,
} from "../../../_extern/node-state.js";
import { toHex } from "../../../support/bytes.js";
import {
  type CompiledLensOp,
  type CompiledLensPath,
  type NodeState,
  LensPathDirection,
  invalidCatalogue,
  invalidStored,
  nodeErr,
  pushCompiledForwardLensOp,
  pushCompiledReverseLensOp,
} from "../index.js";

const sameTx = (time: TxTime, node: NodeUuid, txId: TxId) =>
  TxTimeOps.cmp(time, txId.time) === 0 && uuidEquals(node, txId.node);
const incomingKey = (tx: TxId, row: RowUuid, table: number, branch: Uint8Array) =>
  `${txIdKey(tx)}|${toHex(row)}|${table}|${toHex(branch)}`;

export const readPayloadMethods = {
  table(this: NodeState, table: string): any {
    const found = this.catalogue.schema.tables.find((t: any) => t.name === table);
    if (!found) throw nodeErr("TableNotFound", table);
    return found;
  },
  tableInSchema(this: NodeState, table: string, schemaVersion: SchemaVersionId): any {
    return this.tableInSchemaRef(table, schemaVersion);
  },
  tableInSchemaRef(this: NodeState, table: string, schemaVersion: SchemaVersionId): any {
    const c = this.catalogue;
    if (uuidEquals(schemaVersion, c.activeSchema.schema)) {
      const found = c.activeSchema.compiled.tables.find((t: any) => t.name === table);
      if (!found) throw nodeErr("TableNotFound", table);
      return found;
    }
    const found =
      c.catalogueSchemas
        .get(schemaIdKey(schemaVersion))
        ?.schema.tables.find((t: any) => t.name === table) ??
      (uuidEquals(schemaVersion, c.localSchemaVersionId)
        ? c.schema.tables.find((t: any) => t.name === table)
        : undefined);
    if (!found) throw nodeErr("TableNotFound", table);
    return found;
  },
  shortestLensPathCached(this: NodeState, source: SchemaVersionId, target: SchemaVersionId) {
    const key = `${schemaIdKey(source)}>${schemaIdKey(target)}`;
    const cache = this.catalogue.lensPathCache;
    if (cache.has(key)) return cache.get(key);
    const path = this.shortestLensPath(source, target);
    cache.set(key, path);
    return path;
  },
  shortestLensPath(
    this: NodeState,
    source: SchemaVersionId,
    target: SchemaVersionId,
  ): [any, LensPathDirection][] | undefined {
    if (uuidEquals(source, target)) return [];
    const targetKey = schemaIdKey(target);
    // Preserve ancestor-authored columns when a forward route exists.
    for (const forwardOnly of [true, false]) {
      const seen = new Set([schemaIdKey(source)]);
      const queue: [SchemaVersionId, [any, LensPathDirection][]][] = [[source, []]];
      while (queue.length) {
        const [schema, path] = queue.shift()!;
        for (const [lens, direction] of this.orderedLensEdges(schema)) {
          if (forwardOnly && direction === LensPathDirection.Reverse) continue;
          const next = direction === LensPathDirection.Forward ? lens.target : lens.source;
          const nextKey = schemaIdKey(next);
          if (seen.has(nextKey)) continue;
          const nextPath: [any, LensPathDirection][] = [...path, [lens.id, direction]];
          if (nextKey === targetKey) return nextPath;
          seen.add(nextKey);
          queue.push([next, nextPath]);
        }
      }
    }
    return undefined;
  },
  compiledLensPath(
    this: NodeState,
    source: SchemaVersionId,
    target: SchemaVersionId,
    table: string,
  ): CompiledLensPath | undefined {
    const key = `${schemaIdKey(source)}>${schemaIdKey(target)}>${table}`;
    const cache = this.catalogue.compiledLensCache;
    if (cache.has(key)) return cache.get(key);
    const steps = this.shortestLensPathCached(source, target);
    if (!steps) {
      cache.set(key, undefined);
      return undefined;
    }
    let currentTable = table;
    const ops: CompiledLensOp[] = [];
    for (const [lensId, direction] of steps) {
      const lens = this.catalogue.catalogueLenses.get(schemaIdKey(lensId));
      if (!lens) throw invalidCatalogue("lens chain is unknown");
      const forward = direction === LensPathDirection.Forward;
      const tableLens = lens.tableLenses.find(
        (t: any) => (forward ? t.sourceTable : t.targetTable) === currentTable,
      );
      if (!tableLens) {
        cache.set(key, undefined);
        return undefined;
      }
      if (forward) {
        for (const op of tableLens.ops) pushCompiledForwardLensOp(op, ops);
        currentTable = tableLens.targetTable;
      } else {
        for (const op of [...tableLens.ops].reverse()) pushCompiledReverseLensOp(op, ops);
        currentTable = tableLens.sourceTable;
      }
    }
    const path = { targetTable: currentTable, ops };
    cache.set(key, path);
    return path;
  },
  orderedLensEdges(this: NodeState, schema: SchemaVersionId): [any, LensPathDirection][] {
    const key = schemaIdKey(schema);
    const edges: [any, LensPathDirection][] = [];
    for (const lens of this.catalogue.catalogueLenses.values()) {
      if (schemaIdKey(lens.source) === key) edges.push([lens, LensPathDirection.Forward]);
      else if (schemaIdKey(lens.target) === key) edges.push([lens, LensPathDirection.Reverse]);
    }
    const sortKey = ([lens, d]: [any, LensPathDirection]) =>
      `${schemaIdKey(d === LensPathDirection.Forward ? lens.target : lens.source)}|${schemaIdKey(lens.id)}`;
    return edges.sort((a, b) => (sortKey(a) < sortKey(b) ? -1 : sortKey(a) > sortKey(b) ? 1 : 0));
  },
  nodeForAlias(this: NodeState, alias: number): NodeUuid | undefined {
    return this.nodeAliases.nodeForAlias(alias);
  },
  async resolveNodeAlias(this: NodeState, alias: number): Promise<NodeUuid | undefined> {
    const known = this.nodeForAlias(alias);
    if (known !== undefined) return known;
    for (const raw of await this.database.primaryKeyScanRaw("jazz_nodes", [])) {
      const record = raw.record();
      if (Number(record.getU64(NodeAliasRowRecord.FIELD_ID_IDX)) !== alias) continue;
      const node = record.getUuid(NodeAliasRowRecord.FIELD_UUID_IDX);
      this.nodeAliases.insert(node, alias);
      if (uuidEquals(node, this.nodeUuidValue)) this.selfNodeAlias = alias;
      return node;
    }
    return undefined;
  },
  versionTxId(this: NodeState, version: any): TxId {
    const node = this.nodeForAlias(version.txNodeAlias());
    if (node === undefined) throw invalidStored("history tx node alias must exist");
    return TxIdNew(version.txTime(), node);
  },
  versionRecordFromRow(this: NodeState, version: any): any {
    const schemaVersion = this.schemaVersionForAlias(version.schemaVersionAlias());
    if (schemaVersion === undefined) throw invalidStored("history schema version alias must exist");
    const table = this.tableInSchemaRef(version.table(), schemaVersion);
    return VersionRecord.fromStored(
      version,
      table,
      schemaVersion,
      this.authoredColumnsForVersion(version),
    );
  },

  async applyRowVersionPayloadsForRequests(
    this: NodeState,
    requests: any[],
    versionBundles: any[],
  ): Promise<any[]> {
    for (const bundle of versionBundles) {
      try {
        validateVersionRecords(bundle.versions);
      } catch {
        throw nodeErr("MalformedViewUpdate", "malformed version receipt");
      }
    }
    const requested = requests.map((request) => {
      const tableIds = new Set<number>();
      for (const mapping of this.catalogue.physicalMappings.values()) {
        const t = mapping.tables.get(request.table);
        if (t) tableIds.add(t.tableId);
      }
      if (!tableIds.size) throw nodeErr("TableNotFound", String(request.table));
      return {
        rowUuid: request.rowUuid,
        txTime: request.txTime,
        txNode: request.txNodeId,
        tableIds,
      };
    });
    const tableIdOf = (version: any): number | undefined => {
      try {
        return this.physicalTableIdForSchema(version.schemaVersion(), version.table());
      } catch {
        return undefined;
      }
    };
    // One repair frame: preflight and filter every bundle before ingesting the first.
    const prevalidated: any[] = [];
    for (const bundle of versionBundles) {
      validateReceivedViewBundleGlobalTimeDurability(bundle.globalTime, bundle.durability);
      const matches = (r: (typeof requested)[number], version: any) =>
        uuidEquals(r.rowUuid, version.rowUuid()) && sameTx(r.txTime, r.txNode, bundle.tx.txId);
      const versions = bundle.versions.filter((version: any) => {
        const tableId = tableIdOf(version);
        return (
          tableId !== undefined &&
          requested.some((r) => matches(r, version) && r.tableIds.has(tableId))
        );
      });
      if (!versions.length) continue;
      for (const r of requested) {
        const matched = new Set<number>();
        for (const version of versions) {
          if (!matches(r, version)) continue;
          const tableId = tableIdOf(version);
          if (tableId !== undefined && r.tableIds.has(tableId)) matched.add(tableId);
        }
        if (matched.size > 1)
          throw invalidStored("repair response maps to multiple physical tables");
      }
      this.validateViewPayloadVersions(versions);
      prevalidated.push({ ...bundle, versions });
    }
    const applied: any[] = [];
    for (const bundle of prevalidated) {
      // Transport never selects the durable policy capability.
      bundle.tx = transactionWithoutPermissionSubject(bundle.tx);
      if (bundle.scope === "ViewScoped")
        // A row repair is still a partial transaction carrier.
        await this.ingestViewScopedTransactionWithCurrentIndexes(
          bundle.tx,
          bundle.versions.slice(),
          bundle.fate,
          bundle.globalTime,
          bundle.durability,
        );
      else
        await this.ingestKnownTransaction(
          bundle.tx,
          bundle.versions.slice(),
          bundle.fate,
          bundle.globalTime,
          bundle.durability,
        );
      applied.push(bundle);
    }
    return applied;
  },

  async missingKnownStateRowVersionRefs(this: NodeState, message: any): Promise<any[]> {
    if (message.kind !== "ViewUpdate") return [];
    const payload = message.payload;
    // Pending is not a row snapshot: let normal admission reject attached rows.
    if (payload.peerPayloadInventory.openingPending) return [];
    let bundles: any[];
    try {
      bundles = expandVersionCarriers(payload.versionCarriers);
    } catch {
      throw nodeErr("UnsupportedSyncMessage", "malformed version-bundle run");
    }
    // Index inline witnesses once; per-row search would make initial hydration quadratic.
    const incoming = new Set<string>();
    for (const bundle of bundles)
      for (const version of bundle.versions) {
        let table: number;
        try {
          table = this.physicalTableIdForSchema(version.schemaVersion(), version.table());
        } catch {
          continue;
        }
        incoming.add(
          incomingKey(
            bundle.tx.txId,
            version.rowUuid(),
            table,
            version.branchKey().canonicalBytes(),
          ),
        );
      }
    // A late update may race a local unsubscribe; ignore it.
    const shape = this.registeredShape(payload.subscription.shapeId);
    if (!shape) return [];
    const resultSchema = shape.schemaVersion();
    const tableNames = new Map<string, string>();
    const mapping = this.catalogue.physicalMappings.get(schemaIdKey(resultSchema));
    if (mapping)
      for (const [name, identity] of mapping.identities.tables)
        tableNames.set(String(identity.id), name);
    const missing = new Map<string, any>();
    // Every referenced native body must be available, including evicted retained rows.
    for (const row of payload.supportingRows.addedRows()) {
      const txId = row.version.tx;
      const ref = RowVersionRef.new(String(row.versionTable), row.row, txId);
      if (this.inlineVersionBundleCovers(ref, resultSchema, row.version, incoming)) continue;
      const table = tableNames.get(String(row.physicalTable));
      const resident =
        table !== undefined
          ? await this.localSupportingRowVersion(row, resultSchema, table)
          : undefined;
      const exists = resident !== undefined ? await this.transactionExists(txId) : undefined;
      if (resident === undefined || exists === false) missing.set(ref.key(), ref);
    }
    return [...missing.entries()]
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([, r]) => r);
  },

  /** A logical name may be reused by another physical lineage, so name equality is never sufficient evidence. */
  inlineVersionBundleCovers(
    this: NodeState,
    request: any,
    resultSchema: SchemaVersionId,
    version: any,
    incoming: Set<string>,
  ): boolean {
    let tableId: number;
    try {
      tableId = this.physicalTableIdForSchema(resultSchema, request.table);
    } catch (e: any) {
      if (e?.kind !== "TableNotFound") throw e;
      // Contributor facts name their authored table; cover it only through a unique physical lineage.
      const candidates = new Set<number>();
      for (const mapping of this.catalogue.physicalMappings.values()) {
        const t = mapping.tables.get(request.table);
        if (t) candidates.add(t.tableId);
      }
      if (!candidates.size) throw nodeErr("TableNotFound", String(request.table));
      if (candidates.size !== 1) return false;
      tableId = [...candidates][0];
    }
    return incoming.has(
      incomingKey(
        request.txId(),
        request.rowUuid,
        tableId,
        version.branchOrPrefix ?? new Uint8Array(),
      ),
    );
  },
  async localSupportingRowVersion(
    this: NodeState,
    row: any,
    readSchema: SchemaVersionId,
    _table: string,
  ) {
    return this.coveredInputVersion(row, readSchema);
  },
  mintTxTime(this: NodeState, nowMs: number): TxTime {
    const clock = this.clock;
    const base =
      TxTimeOps.cmp(clock.txTime, clock.reservationHighWater.value) >= 0
        ? clock.txTime
        : clock.reservationHighWater.value;
    const madeAt = TxTimeOps.tick(base, nowMs);
    clock.txTime = madeAt;
    clock.reservationHighWater.value = madeAt;
    return madeAt;
  },
  mergeTxTime(this: NodeState, observed: TxTime): void {
    const clock = this.clock;
    if (TxTimeOps.cmp(observed, clock.txTime) > 0) clock.txTime = observed;
    if (TxTimeOps.cmp(observed, clock.reservationHighWater.value) > 0)
      clock.reservationHighWater.value = observed;
  },
};
