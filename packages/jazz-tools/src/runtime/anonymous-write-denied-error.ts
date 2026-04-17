export type AnonymousWriteOperation = "insert" | "update" | "delete";

export interface AnonymousWriteDeniedInfo {
  table: string;
  operation: AnonymousWriteOperation;
}

export class AnonymousWriteDeniedError extends Error {
  readonly table: string;
  readonly operation: AnonymousWriteOperation;

  constructor(info: AnonymousWriteDeniedInfo, cause?: unknown) {
    super(`anonymous session cannot ${info.operation} on table ${info.table}`, { cause });
    this.name = "JazzAnonymousWriteDeniedError";
    this.table = info.table;
    this.operation = info.operation;
  }
}

const MESSAGE_PREFIX = "anonymous session cannot ";
const WASM_NAME = "JazzAnonymousWriteDeniedError";

export function isAnonymousWriteDenied(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const maybeName = (error as { name?: unknown }).name;
  if (maybeName === WASM_NAME) return true;
  const maybeMessage = (error as { message?: unknown }).message;
  if (typeof maybeMessage === "string" && maybeMessage.startsWith(MESSAGE_PREFIX)) {
    return true;
  }
  return false;
}

export function normalizeRuntimeWriteError(error: unknown): unknown {
  if (error instanceof AnonymousWriteDeniedError) return error;
  if (!isAnonymousWriteDenied(error)) return error;
  const info = extractInfo(error);
  return new AnonymousWriteDeniedError(info, error);
}

function extractInfo(error: unknown): AnonymousWriteDeniedInfo {
  if (error && typeof error === "object") {
    const table = (error as { table?: unknown }).table;
    const operation = (error as { operation?: unknown }).operation;
    if (typeof table === "string" && isOperation(operation)) {
      return { table, operation };
    }
    const message = (error as { message?: unknown }).message;
    if (typeof message === "string") {
      const parsed = parseMessage(message);
      if (parsed) return parsed;
    }
  }
  return { table: "unknown", operation: "insert" };
}

function isOperation(value: unknown): value is AnonymousWriteOperation {
  return value === "insert" || value === "update" || value === "delete";
}

function parseMessage(message: string): AnonymousWriteDeniedInfo | null {
  const match = /^anonymous session cannot (insert|update|delete) on table (.+)$/i.exec(message);
  if (!match) return null;
  return { operation: match[1].toLowerCase() as AnonymousWriteOperation, table: match[2] };
}
