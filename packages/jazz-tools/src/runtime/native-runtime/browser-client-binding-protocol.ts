import type { TransactionalRuntime } from "../client.js";
import type { RuntimeSubscriptionDelta, WasmRow } from "../../drivers/types.js";
import type { BrowserRelayError } from "./browser-worker-protocol.js";

/** Versioned, ephemeral host binding. These messages are never peer sync or durable storage. */
export const CLIENT_BINDING_VERSION = 1;

type Methods = Pick<
  TransactionalRuntime,
  | "query"
  | "insert"
  | "restore"
  | "update"
  | "upsert"
  | "delete"
  | "beginTransaction"
  | "commitTransaction"
  | "rollbackTransaction"
  | "waitForTransaction"
> & {
  [M in
    | "streamingMutation"
    | "updateLargeValues"
    | "requestInsertPermissionAdvice"
    | "requestReadPermissionAdvice"
    | "requestUpdatePermissionAdvice"
    | "requestDeletePermissionAdvice"]-?: NonNullable<TransactionalRuntime[M]>;
};
export type ClientBindingMethod = keyof Methods;
export type ClientBindingArgs<M extends ClientBindingMethod> = Parameters<Methods[M]>;
export type ClientBindingResult<M extends ClientBindingMethod> = Awaited<ReturnType<Methods[M]>>;
export type ClientBindingCall = {
  [M in ClientBindingMethod]: { method: M; args: ClientBindingArgs<M> };
}[ClientBindingMethod];
export type ClientBindingRequest = { version: 1; id: number } & (
  | { type: "client-call"; call: ClientBindingCall }
  | { type: "client-subscribe"; args: Parameters<TransactionalRuntime["createSubscription"]> }
  | { type: "client-unsubscribe"; handle: number }
  | { type: "client-close" }
);
export type ClientBindingEvent = { version: 1; id: number } & (
  | { type: "client-result"; value?: unknown; error?: BrowserRelayError }
  | { type: "client-delta"; value?: RuntimeSubscriptionDelta; error?: BrowserRelayError }
  | { type: "client-failed"; error: BrowserRelayError; value?: never }
);

// Native rows deliberately hide valuesByColumn from application enumeration.
// Structured clone ignores non-enumerable properties; make this one field
// explicit for the trip and restore its descriptor on the receiving side.
function rowForPort(row: WasmRow): WasmRow {
  return { ...row, ...(row.valuesByColumn ? { valuesByColumn: row.valuesByColumn } : {}) };
}
function rowFromPort(row: WasmRow): WasmRow {
  if (row.valuesByColumn)
    Object.defineProperty(row, "valuesByColumn", {
      value: row.valuesByColumn,
      enumerable: false,
      configurable: true,
    });
  return row;
}
export function queryRowsForPort(value: unknown): unknown {
  return Array.isArray(value) ? value.map(rowForPort) : value;
}
export function queryRowsFromPort(value: unknown): unknown {
  return Array.isArray(value) ? value.map(rowFromPort) : value;
}
function mapDeltaRows(
  delta: RuntimeSubscriptionDelta,
  row: (value: WasmRow) => WasmRow,
): RuntimeSubscriptionDelta {
  return {
    ...delta,
    added: delta.added.map((entry) => ({ ...entry, row: row(entry.row) })),
    updated: delta.updated.map((entry) => ({
      ...entry,
      ...(entry.row ? { row: row(entry.row) } : {}),
    })),
    terminalOperations: delta.terminalOperations?.map((operation) => ({
      ...operation,
      edit:
        "Insert" in operation.edit
          ? { Insert: { ...operation.edit.Insert, row: row(operation.edit.Insert.row) } }
          : "Update" in operation.edit
            ? { Update: { ...operation.edit.Update, row: row(operation.edit.Update.row) } }
            : operation.edit,
    })),
  };
}
export const deltaForPort = (delta: RuntimeSubscriptionDelta): RuntimeSubscriptionDelta =>
  mapDeltaRows(delta, rowForPort);
export const deltaFromPort = (delta: RuntimeSubscriptionDelta): RuntimeSubscriptionDelta =>
  mapDeltaRows(delta, rowFromPort);

export function transactionInContext(json?: string | null): string | undefined {
  if (!json) return undefined;
  const value: unknown = JSON.parse(json);
  if (!value || typeof value !== "object") throw new Error("Invalid transaction context");
  const id = (value as { transaction_id?: unknown }).transaction_id;
  if (id === undefined) return undefined;
  if (typeof id !== "string" || id.length === 0) throw new Error("Invalid transaction identity");
  return id;
}
