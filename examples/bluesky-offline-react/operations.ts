export const operationKinds = ["post", "like", "repost"] as const;
export const operationStates = ["queued", "sent", "failed"] as const;

export type OperationKind = (typeof operationKinds)[number];
export type OperationState = (typeof operationStates)[number];

export type StrongRef = { uri: string; cid: string };

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
  constructor(message: string, readonly status: 400 | 403 = 400) {
    super(message);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
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

export function encodeOperationPayload(operation: Pick<Operation, "payload">) {
  return JSON.stringify(operation.payload);
}

export function decodeOperation(row: unknown): Operation {
  if (!isRecord(row)) throw new InvalidOperationError("Invalid operation");
  const kind = row.kind;
  if (!isOperationKind(kind)) {
    throw new InvalidOperationError(`Unsupported operation kind: ${String(kind)}`);
  }
  const state = row.state;
  if (!isOperationState(state)) throw new InvalidOperationError("Invalid operation state");
  let payload: unknown;
  try {
    payload = JSON.parse(requiredString(row.payload, "operation payload"));
  } catch (error) {
    if (error instanceof InvalidOperationError) throw error;
    throw new InvalidOperationError("Invalid operation payload");
  }
  if (!isRecord(payload)) throw new InvalidOperationError("Invalid operation payload");
  const base: OperationBase = {
    id: requiredString(row.id, "operation id"),
    ownerDid: requiredString(row.ownerDid, "operation owner"),
    rkey: requiredString(row.rkey, "operation record key"),
    state,
    error: typeof row.error === "string" ? row.error : undefined,
    createdAt: requiredString(row.createdAt, "operation timestamp"),
  };
  if (kind === "post") {
    const text = requiredString(payload.text, "post operation");
    const createdAt = requiredString(payload.createdAt, "post operation");
    if (payload.reply !== undefined && !isRecord(payload.reply)) {
      throw new InvalidOperationError("Invalid reply operation");
    }
    const reply = payload.reply === undefined ? undefined : {
      root: strongRef(payload.reply.root, "reply root"),
      parent: strongRef(payload.reply.parent, "reply parent"),
    };
    return { ...base, kind, payload: { text, createdAt, reply } };
  }
  const active = payload.active;
  if (typeof active !== "boolean") throw new InvalidOperationError(`Invalid ${kind} operation`);
  return {
    ...base,
    kind,
    payload: {
      subjectUri: requiredString(payload.subjectUri, `${kind} operation`),
      subjectCid: requiredString(payload.subjectCid, `${kind} operation`),
      active,
      syncedActive: typeof payload.syncedActive === "boolean" ? payload.syncedActive : undefined,
      createdAt: requiredString(payload.createdAt, `${kind} operation`),
    },
  };
}

export function parseOperationBatch(value: unknown, ownerDid: string) {
  if (!Array.isArray(value) || value.length > 100) throw new InvalidOperationError("invalid operations");
  const operations = value.map(decodeOperation);
  if (operations.some((operation) => operation.ownerDid !== ownerDid)) {
    throw new InvalidOperationError("owner mismatch", 403);
  }
  return operations;
}
