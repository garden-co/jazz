import { JazzClient, type Runtime } from "../client.js";
import { createDbFromClient, type QueryBuilder } from "../db.js";
import type { WasmRow, WasmSchema } from "../../drivers/types.js";

const DEFAULT_TABLE_COUNT = 24;
const DEFAULT_COLUMNS_PER_TABLE = 24;
const DEFAULT_WARMUP_ITERATIONS = 20;
const DEFAULT_MEASURED_ITERATIONS = 120;
const TARGET_TABLE = "bench_records";

export interface SchemaMarshallingBenchOptions {
  label: string;
  runtime: Runtime;
  schema: WasmSchema;
  tableName?: string;
  warmupIterations?: number;
  measuredIterations?: number;
}

export interface BenchTimingSummary {
  calls: number;
  totalMs: number;
  avgMs: number;
  p50Ms: number;
  p95Ms: number;
}

export interface SchemaMarshallingBenchResult {
  label: string;
  schema: {
    tableCount: number;
    columnsPerTable: number;
    approxJsonBytes: number;
  };
  directGetSchema: BenchTimingSummary;
  dbAll: BenchTimingSummary & {
    rowsPerQuery: number;
    getSchemaCalls: number;
    getSchemaCallsPerIteration: number;
    getSchemaTotalMs: number;
    getSchemaAvgMsPerCall: number;
    getSchemaAvgMsPerIteration: number;
  };
}

export function createSyntheticRuntimeSchema(options?: {
  tableCount?: number;
  columnsPerTable?: number;
}): WasmSchema {
  const tableCount = options?.tableCount ?? DEFAULT_TABLE_COUNT;
  const columnsPerTable = options?.columnsPerTable ?? DEFAULT_COLUMNS_PER_TABLE;
  const schema: WasmSchema = {};

  for (let tableIndex = 0; tableIndex < tableCount; tableIndex += 1) {
    const tableName = tableIndex === 0 ? TARGET_TABLE : `bench_aux_${tableIndex}`;
    schema[tableName] = {
      columns: Array.from({ length: columnsPerTable }, (_unused, columnIndex) => ({
        name: `col_${columnIndex}`,
        column_type: { type: "Text" as const },
        nullable: false,
      })),
    };
  }

  return schema;
}

function createQuery(schema: WasmSchema, tableName: string): QueryBuilder<Record<string, string>> {
  return {
    _table: tableName,
    _schema: schema,
    _rowType: {} as Record<string, string>,
    _build() {
      return JSON.stringify({
        table: tableName,
        conditions: [],
        includes: {},
        orderBy: [],
      });
    },
  };
}

function createRows(schema: WasmSchema, tableName: string): WasmRow[] {
  const table = schema[tableName];
  if (!table) {
    throw new Error(`Missing benchmark table "${tableName}" in runtime schema.`);
  }

  return [
    {
      id: "bench-row-1",
      values: table.columns.map((column, index) => ({
        type: "Text" as const,
        value: `${column.name}-value-${index}`,
      })),
    },
  ];
}

function cloneRows(rows: readonly WasmRow[]): WasmRow[] {
  return rows.map((row) => ({
    id: row.id,
    values: row.values.map((value) => ({ ...value })),
  }));
}

function percentile(sortedValues: readonly number[], ratio: number): number {
  if (sortedValues.length === 0) {
    return 0;
  }

  const index = Math.min(
    sortedValues.length - 1,
    Math.max(0, Math.ceil(ratio * sortedValues.length) - 1),
  );
  return sortedValues[index] ?? 0;
}

function summarizeTimings(samples: readonly number[]): BenchTimingSummary {
  const sorted = [...samples].sort((left, right) => left - right);
  const totalMs = samples.reduce((sum, value) => sum + value, 0);

  return {
    calls: samples.length,
    totalMs,
    avgMs: samples.length > 0 ? totalMs / samples.length : 0,
    p50Ms: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
  };
}

async function measureAsyncIterations(
  iterations: number,
  run: () => Promise<void>,
): Promise<BenchTimingSummary> {
  const samples: number[] = [];

  for (let index = 0; index < iterations; index += 1) {
    const startedAt = performance.now();
    await run();
    samples.push(performance.now() - startedAt);
  }

  return summarizeTimings(samples);
}

function measureSyncIterations(iterations: number, run: () => void): BenchTimingSummary {
  const samples: number[] = [];

  for (let index = 0; index < iterations; index += 1) {
    const startedAt = performance.now();
    run();
    samples.push(performance.now() - startedAt);
  }

  return summarizeTimings(samples);
}

export async function runSchemaMarshallingBench(
  options: SchemaMarshallingBenchOptions,
): Promise<SchemaMarshallingBenchResult> {
  const tableName = options.tableName ?? TARGET_TABLE;
  const warmupIterations = options.warmupIterations ?? DEFAULT_WARMUP_ITERATIONS;
  const measuredIterations = options.measuredIterations ?? DEFAULT_MEASURED_ITERATIONS;
  const rows = createRows(options.schema, tableName);
  let getSchemaCalls = 0;
  let getSchemaTotalMs = 0;

  const runtime = new Proxy(options.runtime, {
    get(target, property, receiver) {
      if (property === "getSchema") {
        return () => {
          const startedAt = performance.now();
          const value = target.getSchema();
          getSchemaCalls += 1;
          getSchemaTotalMs += performance.now() - startedAt;
          return value;
        };
      }

      if (property === "query") {
        return async () => cloneRows(rows);
      }

      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  }) as Runtime;

  const client = JazzClient.connectWithRuntime(runtime, {
    appId: `schema-marshalling-bench-${options.label}`,
    schema: options.schema,
  });
  const db = createDbFromClient({ appId: `schema-marshalling-db-${options.label}` }, client);
  const query = createQuery(options.schema, tableName);

  for (let index = 0; index < warmupIterations; index += 1) {
    options.runtime.getSchema();
    await db.all(query);
  }

  getSchemaCalls = 0;
  getSchemaTotalMs = 0;

  const directGetSchema = measureSyncIterations(measuredIterations, () => {
    options.runtime.getSchema();
  });
  const dbAll = await measureAsyncIterations(measuredIterations, async () => {
    await db.all(query);
  });

  const firstTable = Object.values(options.schema)[0];

  return {
    label: options.label,
    schema: {
      tableCount: Object.keys(options.schema).length,
      columnsPerTable: firstTable?.columns.length ?? 0,
      approxJsonBytes: JSON.stringify(options.schema).length,
    },
    directGetSchema,
    dbAll: {
      ...dbAll,
      rowsPerQuery: rows.length,
      getSchemaCalls,
      getSchemaCallsPerIteration: measuredIterations > 0 ? getSchemaCalls / measuredIterations : 0,
      getSchemaTotalMs,
      getSchemaAvgMsPerCall: getSchemaCalls > 0 ? getSchemaTotalMs / getSchemaCalls : 0,
      getSchemaAvgMsPerIteration:
        measuredIterations > 0 ? getSchemaTotalMs / measuredIterations : 0,
    },
  };
}
