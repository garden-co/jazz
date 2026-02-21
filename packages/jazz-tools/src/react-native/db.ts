import type { WasmRow, WasmSchema } from "../drivers/types.js";
import { JazzClient, type PersistenceTier } from "../runtime/client.js";
import type { QueryBuilder, TableProxy } from "../runtime/db.js";
import { translateQuery } from "../runtime/query-adapter.js";
import { transformRows } from "../runtime/row-transformer.js";
import { SubscriptionManager, type SubscriptionDelta } from "../runtime/subscription-manager.js";
import { toUpdateRecord, toValueArray } from "../runtime/value-converter.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";

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
  table?: string;
  conditions?: Array<{ column: string; op: string; value: unknown }>;
  includes?: IncludeSpec;
  orderBy?: Array<[string, "asc" | "desc"]>;
  limit?: number;
  offset?: number;
  hops?: string[];
  gather?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };
}

type NormalizedBuiltQuery = {
  table: string;
  conditions: Array<{ column: string; op: string; value: unknown }>;
  includes: IncludeSpec;
  orderBy: Array<[string, "asc" | "desc"]>;
  limit?: number;
  offset?: number;
  hops: string[];
  gather?:
    | {
        max_depth: number;
        step_table: string;
        step_current_column: string;
        step_conditions: Array<{ column: string; op: string; value: unknown }>;
        step_hops: string[];
      }
    | undefined;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeBuiltQuery(raw: BuiltQuery, fallbackTable: string): NormalizedBuiltQuery {
  const table = typeof raw.table === "string" ? raw.table : fallbackTable;
  const conditions = Array.isArray(raw.conditions)
    ? raw.conditions.filter(
        (condition): condition is { column: string; op: string; value: unknown } =>
          isPlainObject(condition) &&
          typeof condition.column === "string" &&
          typeof condition.op === "string",
      )
    : [];
  const includes = isPlainObject(raw.includes) ? (raw.includes as IncludeSpec) : {};
  const orderBy = Array.isArray(raw.orderBy)
    ? raw.orderBy.filter(
        (entry): entry is [string, "asc" | "desc"] =>
          Array.isArray(entry) &&
          entry.length === 2 &&
          typeof entry[0] === "string" &&
          (entry[1] === "asc" || entry[1] === "desc"),
      )
    : [];
  const hops = Array.isArray(raw.hops)
    ? raw.hops.filter((hop): hop is string => typeof hop === "string")
    : [];
  const gather =
    isPlainObject(raw.gather) &&
    Number.isInteger(raw.gather.max_depth) &&
    raw.gather.max_depth > 0 &&
    typeof raw.gather.step_table === "string" &&
    typeof raw.gather.step_current_column === "string" &&
    Array.isArray(raw.gather.step_conditions) &&
    Array.isArray(raw.gather.step_hops)
      ? {
          max_depth: raw.gather.max_depth,
          step_table: raw.gather.step_table,
          step_current_column: raw.gather.step_current_column,
          step_conditions: raw.gather.step_conditions.filter(
            (condition): condition is { column: string; op: string; value: unknown } =>
              isPlainObject(condition) &&
              typeof condition.column === "string" &&
              typeof condition.op === "string",
          ),
          step_hops: raw.gather.step_hops.filter((hop): hop is string => typeof hop === "string"),
        }
      : undefined;

  return {
    table,
    conditions,
    includes,
    orderBy,
    limit: typeof raw.limit === "number" ? raw.limit : undefined,
    offset: typeof raw.offset === "number" ? raw.offset : undefined,
    hops,
    gather,
  };
}

function resolveHopOutputTable(
  schema: WasmSchema,
  startTable: string,
  hops: readonly string[],
): string {
  if (hops.length === 0) {
    return startTable;
  }
  const relations = analyzeRelations(schema);
  let currentTable = startTable;
  for (const hopName of hops) {
    const candidates = relations.get(currentTable) ?? [];
    const relation = candidates.find((candidate) => candidate.name === hopName);
    if (!relation) {
      throw new Error(`Unknown relation "${hopName}" on table "${currentTable}"`);
    }
    currentTable = relation.toTable;
  }
  return currentTable;
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
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson) as BuiltQuery, query._table);
    const rows = await client.query(translateQuery(builderJson, query._schema), settledTier);
    const outputTable =
      builtQuery.hops.length > 0
        ? resolveHopOutputTable(query._schema, builtQuery.table, builtQuery.hops)
        : query._table;
    const outputIncludes = builtQuery.hops.length > 0 ? {} : builtQuery.includes;
    return transformRows<T>(rows, query._schema, outputTable, outputIncludes);
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
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson) as BuiltQuery, query._table);
    const outputTable =
      builtQuery.hops.length > 0
        ? resolveHopOutputTable(query._schema, builtQuery.table, builtQuery.hops)
        : query._table;
    const outputIncludes = builtQuery.hops.length > 0 ? {} : builtQuery.includes;
    const wasmQuery = translateQuery(builderJson, query._schema);

    const transform = (row: WasmRow): T => {
      return transformRows<T>([row], query._schema, outputTable, outputIncludes)[0];
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
