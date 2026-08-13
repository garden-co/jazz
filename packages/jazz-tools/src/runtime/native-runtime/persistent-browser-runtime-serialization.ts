import type { NativeRowDelta, InsertValues, Value, WasmSchema } from "../../drivers/types.js";
import type { OpenBatchId } from "../client.js";
import type {
  PersistentBrowserSubscriptionMessage,
  PersistentBrowserWriteRequest,
} from "./persistent-browser-protocol.js";
import { encodeCellsForRow } from "./native-runtime-adapter.js";

type CompletedTxState = "committed" | "rolled_back";

/**
 * Boundary translations between the browser worker protocol and the Runtime
 * interface. Keeping these pure conversions together makes worker transport
 * changes independent from the runtime's lifecycle and command queue.
 */
export function nativeDeltaFromFrame(
  message: Extract<PersistentBrowserSubscriptionMessage, { frame: unknown }>,
): NativeRowDelta {
  if (message.frame.kind !== "native-row-delta") {
    throw new Error(`Unknown persistent browser subscription frame ${message.frame.kind}`);
  }
  return {
    __jazzNativeRowDelta: true,
    reset: message.frame.reset,
    added: new Uint8Array(message.frame.added),
    removed: new Uint8Array(message.frame.removed),
    updated: new Uint8Array(message.frame.updated),
    addedCount: message.frame.addedCount,
    removedCount: message.frame.removedCount,
    updatedCount: message.frame.updatedCount,
    terminalLayouts: message.frame.terminalLayouts,
    terminalOperations: message.frame.terminalOperations,
  };
}

export function subscriptionDebugName(queryJson: string): string {
  try {
    const query = JSON.parse(queryJson) as {
      table?: unknown;
      relation_ir?: { table?: unknown };
      debugName?: unknown;
    };
    if (typeof query.debugName === "string" && query.debugName.trim()) {
      return query.debugName;
    }
    const table = typeof query.table === "string" ? query.table : query.relation_ir?.table;
    if (typeof table === "string" && table.trim()) return table;
  } catch {
    // Fall through to the bounded raw query label below.
  }
  return queryJson.length > 120 ? `${queryJson.slice(0, 117)}...` : queryJson;
}

export function openBatchIdFromReadOptions(
  optionsJson: string | null | undefined,
): OpenBatchId | undefined {
  if (!optionsJson) return undefined;
  try {
    const parsed = JSON.parse(optionsJson) as { transaction_batch_id?: unknown };
    return typeof parsed.transaction_batch_id === "string"
      ? (parsed.transaction_batch_id as OpenBatchId)
      : undefined;
  } catch {
    return undefined;
  }
}

export function normalizeWriteSetupMessage(message: string): string {
  const missingRequiredColumn = /^missing required column ([A-Za-z_$][\w$]*)$/.exec(message);
  if (missingRequiredColumn) {
    return `missing required field \`${missingRequiredColumn[1]}\``;
  }
  return message;
}

export function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (error && typeof error === "object") {
    const message = (error as { message?: unknown }).message;
    if (typeof message === "string" && message.trim()) return message;
  }
  return String(error);
}

export function txStateMessage(
  openBatchId: string,
  completedBatches: Map<string, CompletedTxState>,
): string {
  if (completedBatches.get(openBatchId) === "committed") {
    return `open batch ${openBatchId} is already committed`;
  }
  return `open batch ${openBatchId} has already been completed or was never opened`;
}

export function commitTransactionMessage(
  openBatchId: string,
  completedBatches: Map<string, CompletedTxState>,
): string {
  const message = txStateMessage(openBatchId, completedBatches);
  return completedBatches.get(openBatchId) === "committed"
    ? `Write error: ${message}`
    : `Commit transaction failed: Write error: ${message}`;
}

export function rollbackTransactionMessage(
  openBatchId: string,
  completedBatches: Map<string, CompletedTxState>,
): string {
  const message = txStateMessage(openBatchId, completedBatches);
  return completedBatches.get(openBatchId) === "committed"
    ? `Write error: ${message}`
    : `Rollback transaction failed: Write error: ${message}`;
}

export function writeOperationName(method: PersistentBrowserWriteRequest["method"]): string {
  switch (method) {
    case "insert":
    case "restore":
      return "Insert";
    case "update":
    case "upsert":
      return "Update";
    case "delete":
      return "Delete";
  }
}

export function valuesForRow(schema: WasmSchema, table: string, values: InsertValues): Value[] {
  const definition = tableDefinition(schema, table);
  encodeCellsForRow(definition, values);
  return definition.columns.map(
    (column) => values[column.name] ?? column.default ?? { type: "Null" },
  );
}

export function tableDefinition(schema: WasmSchema, table: string): WasmSchema[string] {
  const definition = schema[table];
  if (!definition) throw new Error(`unknown table ${table}`);
  return definition;
}
