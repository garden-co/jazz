// Client cache classification and explicit cold-state eviction.
import type { NodeState } from "./index.js";
import { historyPrimaryKey, cmpTxId, type TxId } from "../../_extern/node-state.js";

const CLIENT_CACHE_LOW_WATER_NUMERATOR = 9;
const CLIENT_CACHE_LOW_WATER_DENOMINATOR = 10;

export enum ClientCacheClass {
  Evictable,
  Pinned,
}

export type EvictColdReport = {
  rowVersionsEvictable: number;
  rowVersionsPinned: number;
  parkedFamiliesPinned: number;
  fatePendingTxsPinned: number;
};

export class ClientCacheBudget {
  constructor(readonly maxBytes: number) {}
  lowWaterBytes(): number {
    return Math.floor(
      (this.maxBytes * CLIENT_CACHE_LOW_WATER_NUMERATOR) / CLIENT_CACHE_LOW_WATER_DENOMINATOR,
    );
  }
}

export type ClientCacheBudgetReport = {
  beforeBytes: number;
  afterBytes: number;
  lowWaterBytes: number;
  evicted: EvictColdReport;
};

export const evictionMethods = {
  /** Globally accepted row versions can be refetched; everything else stays pinned. */
  async classifyRowVersionForEviction(this: NodeState, txId: TxId): Promise<ClientCacheClass> {
    const tx = await this.queryTransaction(txId);
    if (!tx || tx.fate.kind === "Pending") return ClientCacheClass.Pinned;
    return tx.fate.kind === "Accepted" && tx.durability === "Global"
      ? ClientCacheClass.Evictable
      : ClientCacheClass.Pinned;
  },

  /** Manually drop evictable cold client-cache state. */
  async evictCold(this: NodeState): Promise<EvictColdReport> {
    return (await this.evictColdInner(undefined)).evicted;
  },

  async enforceClientCacheBudget(
    this: NodeState,
    budget: ClientCacheBudget,
  ): Promise<ClientCacheBudgetReport | undefined> {
    const beforeBytes = await this.clientCacheMeteredBytes();
    if (beforeBytes === undefined || beforeBytes <= budget.maxBytes) return undefined;
    const report = await this.evictColdInner(budget.lowWaterBytes());
    report.beforeBytes = beforeBytes;
    report.afterBytes = (await this.clientCacheMeteredBytes()) ?? report.afterBytes;
    return report;
  },

  async clientCacheMeteredBytes(this: NodeState): Promise<number | undefined> {
    const families = await this.clientCacheMeteredColumnFamilies();
    return this.database.approximateClassBytesForLogicalFamilies(families);
  },

  async evictColdInner(
    this: NodeState,
    lowWaterBytes: number | undefined,
  ): Promise<ClientCacheBudgetReport> {
    const report: EvictColdReport = {
      rowVersionsEvictable: 0,
      rowVersionsPinned: 0,
      parkedFamiliesPinned:
        this.parking.parkedCommitUnits.size +
        this.parking.parkedCatalogueCommitUnits.size +
        this.parking.parkedShapeRegistrations.size,
      fatePendingTxsPinned: 0,
    };
    const beforeBytes = (await this.clientCacheMeteredBytes()) ?? 0;
    let remainingBytes = beforeBytes;
    const seenPinned = new Set<string>();
    const evictable: { txId: TxId; version: any }[] = [];
    for (const table of [...this.catalogue.schema.tables]) {
      for (const version of await this.queryTableVersions(table.name)) {
        const txId = this.versionTxId(version);
        if ((await this.classifyRowVersionForEviction(txId)) === ClientCacheClass.Evictable) {
          evictable.push({ txId, version });
        } else if (!seenPinned.has(String(txId))) {
          seenPinned.add(String(txId));
          report.rowVersionsPinned++;
          if ((await this.queryTransaction(txId))?.fate.kind === "Pending")
            report.fatePendingTxsPinned++;
        }
      }
    }
    evictable.sort((a, b) => cmpTxId(a.txId, b.txId));
    // INV-SYNC-27: no fast known-state cursor may survive body deletion.
    if (evictable.length) this.invalidateSubscriptionScopes();
    if (lowWaterBytes === undefined) {
      let last: TxId | undefined;
      for (const c of evictable) {
        if (last !== undefined && cmpTxId(last, c.txId) === 0) continue;
        this.invalidateTxVersionTablesCache(c.txId);
        last = c.txId;
      }
    }
    if (evictable.length) await this.purgeSubscriptionWatermarks();
    let batch = this.database.openBatch();
    let batchDeletes = 0;
    for (const candidate of evictable) {
      if (lowWaterBytes !== undefined && remainingBytes <= lowWaterBytes) break;
      report.rowVersionsEvictable++;
      if (lowWaterBytes !== undefined) this.invalidateTxVersionTablesCache(candidate.txId);
      const historyTable = this.versionStorageTableForRow(candidate.version);
      batch.delete(historyTable, historyPrimaryKey(candidate.version));
      batchDeletes++;
      if (lowWaterBytes !== undefined) {
        await this.applyAndPersistNodeBatch(batch);
        remainingBytes = (await this.clientCacheMeteredBytes()) ?? remainingBytes;
        batch = this.database.openBatch();
      }
    }
    if (batchDeletes > 0 && lowWaterBytes === undefined) await this.applyAndPersistNodeBatch(batch);
    const afterBytes = (await this.clientCacheMeteredBytes()) ?? remainingBytes;
    return { beforeBytes, afterBytes, lowWaterBytes: lowWaterBytes ?? 0, evicted: report };
  },

  async clientCacheMeteredColumnFamilies(this: NodeState): Promise<string[]> {
    const families = new Set<string>();
    for (const table of [...this.catalogue.schema.tables])
      for (const version of await this.queryTableVersions(table.name))
        families.add(String(this.versionStorageTableForRow(version)));
    return [...families].sort();
  },
};
