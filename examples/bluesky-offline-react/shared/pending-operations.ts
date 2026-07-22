/**
 * Offline writes are stored first in Jazz's pendingOperations table. This module
 * serialises their payloads for storage and validates them before the BFF writes
 * the final desired state to ATProto.
 */
const operationKinds = ["post", "like", "repost"] as const;
const operationStates = ["queued", "sent", "failed"] as const;

type OperationKind = (typeof operationKinds)[number];
type OperationState = (typeof operationStates)[number];

type StrongRef = { uri: string; cid: string };

export type PostOperation = OperationBase & {
  kind: "post";
  payload: {
    text: string;
    createdAt: string;
    reply?: { root: StrongRef; parent: StrongRef };
  };
};

export type ReactionOperation = OperationBase & {
  kind: "like" | "repost";
  payload: {
    subjectUri: string;
    subjectCid: string;
    active: boolean;
    syncedActive?: boolean;
    createdAt: string;
  };
};

export type Operation = PostOperation | ReactionOperation;

type OperationBase = {
  id: string;
  ownerDid: string;
  rkey: string;
  state: OperationState;
  error?: string | null;
  createdAt: string;
};

export class InvalidOperationError extends Error {
  constructor(
    message: string,
    readonly status: 400 | 403 = 400,
  ) {
    super(message);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isOperationKind(value: unknown): value is OperationKind {
  return operationKinds.some((kind) => kind === value);
}

function isOperationState(value: unknown): value is OperationState {
  return operationStates.some((state) => state === value);
}

function requiredString(value: unknown, label: string) {
  if (typeof value !== "string" || !value) throw new InvalidOperationError(`Invalid ${label}`);
  return value;
}

function strongRef(value: unknown, label: string): StrongRef {
  if (!isRecord(value)) throw new InvalidOperationError(`Invalid ${label}`);
  return { uri: requiredString(value.uri, label), cid: requiredString(value.cid, label) };
}

function operationBase(row: Record<string, unknown>): OperationBase {
  const state = row.state;
  if (!isOperationState(state)) throw new InvalidOperationError("Invalid operation state");
  return {
    id: requiredString(row.id, "operation id"),
    ownerDid: requiredString(row.ownerDid, "operation owner"),
    rkey: requiredString(row.rkey, "operation record key"),
    state,
    error: typeof row.error === "string" ? row.error : undefined,
    createdAt: requiredString(row.createdAt, "operation timestamp"),
  };
}

function operationPayload(value: unknown) {
  try {
    const payload: unknown = JSON.parse(requiredString(value, "operation payload"));
    if (!isRecord(payload)) throw new InvalidOperationError("Invalid operation payload");
    return payload;
  } catch (error) {
    if (error instanceof InvalidOperationError) throw error;
    throw new InvalidOperationError("Invalid operation payload");
  }
}

function postPayload(payload: Record<string, unknown>): PostOperation["payload"] {
  const text = requiredString(payload.text, "post operation");
  const createdAt = requiredString(payload.createdAt, "post operation");
  if (payload.reply === undefined) return { text, createdAt };
  if (!isRecord(payload.reply)) throw new InvalidOperationError("Invalid reply operation");
  return {
    text,
    createdAt,
    reply: {
      root: strongRef(payload.reply.root, "reply root"),
      parent: strongRef(payload.reply.parent, "reply parent"),
    },
  };
}

function reactionPayload(
  kind: ReactionOperation["kind"],
  payload: Record<string, unknown>,
): ReactionOperation["payload"] {
  if (typeof payload.active !== "boolean") {
    throw new InvalidOperationError(`Invalid ${kind} operation`);
  }
  return {
    subjectUri: requiredString(payload.subjectUri, `${kind} operation`),
    subjectCid: requiredString(payload.subjectCid, `${kind} operation`),
    active: payload.active,
    syncedActive: typeof payload.syncedActive === "boolean" ? payload.syncedActive : undefined,
    createdAt: requiredString(payload.createdAt, `${kind} operation`),
  };
}

export function encodeOperationPayload(operation: Pick<Operation, "payload">) {
  return JSON.stringify(operation.payload);
}

export function operationRow(operation: Operation) {
  return {
    ownerDid: operation.ownerDid,
    kind: operation.kind,
    rkey: operation.rkey,
    payload: encodeOperationPayload(operation),
    state: operation.state,
    ...(operation.error !== undefined ? { error: operation.error } : {}),
    createdAt: operation.createdAt,
  };
}

export function decodeOperation(row: unknown): Operation {
  if (!isRecord(row)) throw new InvalidOperationError("Invalid operation");
  const kind = row.kind;
  if (!isOperationKind(kind)) {
    throw new InvalidOperationError(`Unsupported operation kind: ${String(kind)}`);
  }
  const base = operationBase(row);
  const payload = operationPayload(row.payload);
  if (kind === "post") return { ...base, kind, payload: postPayload(payload) };
  return {
    ...base,
    kind,
    payload: reactionPayload(kind, payload),
  };
}

export function parseOperationBatch(value: unknown, ownerDid: string) {
  if (!Array.isArray(value) || value.length > 100)
    throw new InvalidOperationError("invalid operations");
  const operations = value.map(decodeOperation);
  if (operations.some((operation) => operation.ownerDid !== ownerDid)) {
    throw new InvalidOperationError("owner mismatch", 403);
  }
  return operations;
}
