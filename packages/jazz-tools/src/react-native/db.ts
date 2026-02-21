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

function toBuilderJson(query: NormalizedBuiltQuery): string {
  return JSON.stringify({
    table: query.table,
    conditions: query.conditions,
    includes: query.includes,
    orderBy: query.orderBy,
    limit: query.limit,
    offset: query.offset,
  });
}

function includeTreeFromHops(hops: readonly string[]): IncludeSpec {
  const root: IncludeSpec = {};
  let cursor: IncludeSpec = root;

  for (let i = 0; i < hops.length; i += 1) {
    const hop = hops[i];
    if (i === hops.length - 1) {
      cursor[hop] = true;
      break;
    }
    const next: IncludeSpec = {};
    cursor[hop] = next;
    cursor = next;
  }

  return root;
}

function mergeIncludes(base: IncludeSpec, extra: IncludeSpec): IncludeSpec {
  const merged: IncludeSpec = { ...base };
  for (const [key, value] of Object.entries(extra)) {
    const existing = merged[key];
    if (isPlainObject(existing) && isPlainObject(value)) {
      merged[key] = mergeIncludes(existing as IncludeSpec, value as IncludeSpec);
      continue;
    }
    merged[key] = value as boolean | IncludeSpec;
  }
  return merged;
}

function flattenHopPath(
  rows: Record<string, unknown>[],
  hops: readonly string[],
): Record<string, unknown>[] {
  let frontier: unknown[] = rows;
  for (const hop of hops) {
    const next: unknown[] = [];
    for (const item of frontier) {
      if (!isPlainObject(item)) {
        continue;
      }
      const value = item[hop];
      if (Array.isArray(value)) {
        next.push(...value);
      } else if (value !== undefined && value !== null) {
        next.push(value);
      }
    }
    frontier = next;
  }
  return frontier.filter(isPlainObject) as Record<string, unknown>[];
}

function dedupeRowsById(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  const seen = new Set<string>();
  const deduped: Record<string, unknown>[] = [];
  for (const row of rows) {
    const id = row.id;
    if (typeof id !== "string" || seen.has(id)) {
      continue;
    }
    seen.add(id);
    deduped.push(row);
  }
  return deduped;
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

    if (builtQuery.gather) {
      const gather = builtQuery.gather;
      const seenIds = new Set<string>();
      let frontier: string[] = [];

      const seedQuery: NormalizedBuiltQuery = {
        ...builtQuery,
        includes: {},
        orderBy: [],
        limit: undefined,
        offset: undefined,
        hops: [],
        gather: undefined,
      };
      const seedRows = await client.query(
        translateQuery(toBuilderJson(seedQuery), query._schema),
        settledTier,
      );
      const seedObjects = transformRows<Record<string, unknown>>(
        seedRows,
        query._schema,
        query._table,
        {},
      );
      for (const row of seedObjects) {
        if (typeof row.id !== "string" || seenIds.has(row.id)) {
          continue;
        }
        seenIds.add(row.id);
        frontier.push(row.id);
      }

      const stepInclude = includeTreeFromHops(gather.step_hops);
      for (let depth = 1; depth <= gather.max_depth && frontier.length > 0; depth += 1) {
        const stepQuery: NormalizedBuiltQuery = {
          table: gather.step_table,
          conditions: [
            ...gather.step_conditions,
            { column: gather.step_current_column, op: "in", value: frontier },
          ],
          includes: stepInclude,
          orderBy: [],
          limit: undefined,
          offset: undefined,
          hops: [],
          gather: undefined,
        };
        const stepRows = await client.query(
          translateQuery(toBuilderJson(stepQuery), query._schema),
          settledTier,
        );
        const stepObjects = transformRows<Record<string, unknown>>(
          stepRows,
          query._schema,
          gather.step_table,
          stepInclude,
        );
        const nextObjects = dedupeRowsById(flattenHopPath(stepObjects, gather.step_hops));

        const nextFrontier: string[] = [];
        for (const row of nextObjects) {
          const id = row.id;
          if (typeof id !== "string" || seenIds.has(id)) {
            continue;
          }
          seenIds.add(id);
          nextFrontier.push(id);
        }
        frontier = nextFrontier;
      }

      if (seenIds.size === 0) {
        return [];
      }

      const finalQuery: NormalizedBuiltQuery = {
        table: query._table,
        conditions: [{ column: "id", op: "in", value: [...seenIds] }],
        includes: builtQuery.includes,
        orderBy: builtQuery.orderBy,
        limit: builtQuery.limit,
        offset: builtQuery.offset,
        hops: [],
        gather: undefined,
      };
      const finalRows = await client.query(
        translateQuery(toBuilderJson(finalQuery), query._schema),
        settledTier,
      );
      return transformRows<T>(finalRows, query._schema, query._table, builtQuery.includes);
    }

    if (builtQuery.hops.length > 0) {
      const hopIncludes = includeTreeFromHops(builtQuery.hops);
      const mergedIncludes = mergeIncludes(builtQuery.includes, hopIncludes);
      const sourceQuery: NormalizedBuiltQuery = {
        ...builtQuery,
        includes: mergedIncludes,
        hops: [],
        gather: undefined,
      };
      const sourceRows = await client.query(
        translateQuery(toBuilderJson(sourceQuery), query._schema),
        settledTier,
      );
      const sourceObjects = transformRows<Record<string, unknown>>(
        sourceRows,
        query._schema,
        builtQuery.table,
        mergedIncludes,
      );
      return dedupeRowsById(flattenHopPath(sourceObjects, builtQuery.hops)) as T[];
    }

    const rows = await client.query(
      translateQuery(toBuilderJson(builtQuery), query._schema),
      settledTier,
    );
    return transformRows<T>(rows, query._schema, query._table, builtQuery.includes);
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
    if (builtQuery.hops.length > 0 || builtQuery.gather) {
      throw new Error("subscribeAll(...) does not yet support hopTo(...) or gather(...).");
    }
    const wasmQuery = translateQuery(toBuilderJson(builtQuery), query._schema);

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
