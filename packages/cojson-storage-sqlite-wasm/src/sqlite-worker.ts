import sqlite3InitModule, {
  type Database,
  type SqlValue,
} from "@sqlite.org/sqlite-wasm";

export interface WorkerRequest {
  id: number;
  type: string;
  sql?: string;
  params?: unknown[];
  filename?: string;
  version?: number;
}

export interface WorkerResponse {
  id: number;
  success: boolean;
  result?: unknown;
  error?: string;
}

// Type-safe reference to the worker global scope
const workerScope: {
  onmessage: ((event: MessageEvent) => void) | null;
  postMessage(message: WorkerResponse): void;
} = self as never;

let db: Database;

async function handleInitialize(
  filename: string,
): Promise<{ opfsAvailable: boolean }> {
  const sqlite3 = await sqlite3InitModule();

  try {
    const poolUtil = await sqlite3.installOpfsSAHPoolVfs({});
    db = new poolUtil.OpfsSAHPoolDb(filename);
    return { opfsAvailable: true };
  } catch {
    console.warn(
      "OPFS SAH pool not available in worker, falling back to in-memory storage",
    );
  }

  db = new sqlite3.oo1.DB(":memory:");
  return { opfsAvailable: false };
}

function handleRun(sql: string, params: unknown[]): void {
  db.exec(sql, { bind: params as SqlValue[] });
}

function handleQuery(sql: string, params: unknown[]): unknown[] {
  return db.exec(sql, {
    bind: params as SqlValue[],
    returnValue: "resultRows",
    rowMode: "object",
  }) as unknown[];
}

function handleGetMigrationVersion(): number {
  const rows = db.exec("PRAGMA user_version", {
    returnValue: "resultRows",
    rowMode: "object",
  }) as Array<Record<string, SqlValue>>;
  const row = rows[0];
  return typeof row?.["user_version"] === "number" ? row["user_version"] : 0;
}

workerScope.onmessage = async (event: MessageEvent<WorkerRequest>) => {
  const { id, type } = event.data;

  try {
    let result: unknown;

    switch (type) {
      case "initialize":
        result = await handleInitialize(event.data.filename!);
        break;
      case "run":
        handleRun(event.data.sql!, event.data.params!);
        break;
      case "query":
        result = handleQuery(event.data.sql!, event.data.params!);
        break;
      case "get": {
        const rows = handleQuery(event.data.sql!, event.data.params!);
        result = rows[0];
        break;
      }
      case "beginTransaction":
        db.exec("BEGIN TRANSACTION");
        break;
      case "commitTransaction":
        db.exec("COMMIT");
        break;
      case "rollbackTransaction":
        db.exec("ROLLBACK");
        break;
      case "closeDb":
        db.close();
        break;
      case "getMigrationVersion":
        result = handleGetMigrationVersion();
        break;
      case "saveMigrationVersion":
        db.exec(`PRAGMA user_version = ${event.data.version}`);
        break;
      default:
        throw new Error(`Unknown message type: ${type}`);
    }

    workerScope.postMessage({ id, success: true, result });
  } catch (error) {
    workerScope.postMessage({
      id,
      success: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
};
