import type { WasmRow, WasmSchema } from "../drivers/types.js";
import { JazzClient, type PersistenceTier } from "../runtime/client.js";
import type { QueryBuilder, TableProxy } from "../runtime/db.js";
import { translateQuery } from "../runtime/query-adapter.js";
import { transformRows } from "../runtime/row-transformer.js";
import { SubscriptionManager, type SubscriptionDelta } from "../runtime/subscription-manager.js";
import { toUpdateRecord, toValueArray } from "../runtime/value-converter.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";

export interface DbConfig {
  appId: string;
  serverUrl?: string;
  env?: string;
  userBranch?: string;
  dataPath?: string;
  jwtToken?: string;
  adminSecret?: string;
  tier?: PersistenceTier;
}

type IncludeSpec = {
  [relationName: string]: boolean | IncludeSpec;
};

interface BuiltQuery {
  includes?: IncludeSpec;
}

export class Db {
  private readonly clients = new Map<string, JazzClient>();

  constructor(private readonly config: DbConfig) {}

  private getClient(schema: WasmSchema): JazzClient {
    const key = JSON.stringify(schema);

    if (!this.clients.has(key)) {
      const runtime = createJazzRnRuntime({
        schema,
        appId: this.config.appId,
        env: this.config.env,
        userBranch: this.config.userBranch,
        tier: this.config.tier,
        dataPath: this.config.dataPath,
      });

      const client = JazzClient.connectWithRuntime(runtime, {
        appId: this.config.appId,
        schema,
        serverUrl: this.config.serverUrl,
        env: this.config.env,
        userBranch: this.config.userBranch,
        jwtToken: this.config.jwtToken,
        adminSecret: this.config.adminSecret,
        tier: this.config.tier,
      });

      this.clients.set(key, client);
    }

    return this.clients.get(key)!;
  }

  insert<T, Init>(table: TableProxy<T, Init>, data: Init): string {
    const client = this.getClient(table._schema);
    const values = toValueArray(data as Record<string, unknown>, table._schema, table._table);
    return client.create(table._table, values);
  }

  async insertPersisted<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    tier: PersistenceTier,
  ): Promise<string> {
    const client = this.getClient(table._schema);
    const values = toValueArray(data as Record<string, unknown>, table._schema, table._table);
    return client.createPersisted(table._table, values, tier);
  }

  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void {
    const client = this.getClient(table._schema);
    const updates = toUpdateRecord(data as Record<string, unknown>, table._schema, table._table);
    client.update(id, updates);
  }

  async updatePersisted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    tier: PersistenceTier,
  ): Promise<void> {
    const client = this.getClient(table._schema);
    const updates = toUpdateRecord(data as Record<string, unknown>, table._schema, table._table);
    await client.updatePersisted(id, updates, tier);
  }

  deleteFrom<T, Init>(table: TableProxy<T, Init>, id: string): void {
    const client = this.getClient(table._schema);
    client.delete(id);
  }

  async deleteFromPersisted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    tier: PersistenceTier,
  ): Promise<void> {
    const client = this.getClient(table._schema);
    await client.deletePersisted(id, tier);
  }

  async all<T>(query: QueryBuilder<T>, settledTier?: PersistenceTier): Promise<T[]> {
    const client = this.getClient(query._schema);
    const builderJson = query._build();
    const builtQuery = JSON.parse(builderJson) as BuiltQuery;
    const wasmQuery = translateQuery(builderJson, query._schema);
    const rows = await client.query(wasmQuery, settledTier);
    return transformRows<T>(rows, query._schema, query._table, builtQuery.includes ?? {});
  }

  async one<T>(query: QueryBuilder<T>, settledTier?: PersistenceTier): Promise<T | null> {
    const results = await this.all(query, settledTier);
    return results[0] ?? null;
  }

  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    settledTier?: PersistenceTier,
  ): () => void {
    const manager = new SubscriptionManager<T>();
    const client = this.getClient(query._schema);
    const builderJson = query._build();
    const builtQuery = JSON.parse(builderJson) as BuiltQuery;
    const wasmQuery = translateQuery(builderJson, query._schema);

    const transform = (row: WasmRow): T => {
      return transformRows<T>([row], query._schema, query._table, builtQuery.includes ?? {})[0];
    };

    const subId = client.subscribe(
      wasmQuery,
      (delta) => {
        const typedDelta = manager.handleDelta(delta, transform);
        callback(typedDelta);
      },
      settledTier,
    );

    return () => {
      client.unsubscribe(subId);
      manager.clear();
    };
  }

  async shutdown(): Promise<void> {
    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();
  }
}

export async function createDb(config: DbConfig): Promise<Db> {
  return new Db(config);
}
