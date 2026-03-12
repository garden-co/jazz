import type { MutationRejectCode } from "./object-outcomes.js";

const PERSISTED_MUTATION_ERROR_CAUSE_PREFIX = "__jazzPersistedMutationError__:";

export type MutationOperation = "insert" | "update" | "delete";

export interface PersistedMutationErrorData {
  mutationId: string;
  rootMutationId: string;
  objectId: string;
  branchName: string;
  operation: MutationOperation;
  commitIds: string[];
  previousCommitIds: string[];
  code: MutationRejectCode;
  reason: string;
  rejectedAtMicros: number;
}

export class PersistedMutationError extends Error implements PersistedMutationErrorData {
  readonly mutationId: string;
  readonly rootMutationId: string;
  readonly objectId: string;
  readonly branchName: string;
  readonly operation: MutationOperation;
  readonly commitIds: string[];
  readonly previousCommitIds: string[];
  readonly code: MutationRejectCode;
  readonly reason: string;
  readonly rejectedAtMicros: number;
  readonly acknowledge: () => Promise<void>;

  constructor(
    data: PersistedMutationErrorData,
    acknowledgeImpl: (mutationId: string) => Promise<void> | void = async () => {},
    cause?: unknown,
  ) {
    super(`mutation ${data.mutationId} rejected: ${data.reason}`);
    this.name = "PersistedMutationError";
    this.mutationId = data.mutationId;
    this.rootMutationId = data.rootMutationId;
    this.objectId = data.objectId;
    this.branchName = data.branchName;
    this.operation = data.operation;
    this.commitIds = [...data.commitIds];
    this.previousCommitIds = [...data.previousCommitIds];
    this.code = data.code;
    this.reason = data.reason;
    this.rejectedAtMicros = data.rejectedAtMicros;
    this.acknowledge = () => Promise.resolve(acknowledgeImpl(this.mutationId));

    if (cause !== undefined) {
      (this as { cause?: unknown }).cause = cause;
    }
  }
}

export function isPersistedMutationError(error: unknown): error is PersistedMutationError {
  return error instanceof PersistedMutationError;
}

export function normalizePersistedMutationError(
  error: unknown,
  acknowledgeImpl: (mutationId: string) => Promise<void> | void = async () => {},
): PersistedMutationError | null {
  if (error instanceof PersistedMutationError) {
    return error;
  }

  const payload =
    parsePersistedMutationErrorData(error) ??
    parsePersistedMutationErrorData(asRecord(error)?.jazzPersistedMutationError) ??
    parsePersistedMutationErrorData(asRecord(error)?.cause) ??
    parsePersistedMutationErrorDataFromMessage(asRecord(error)?.message) ??
    parsePersistedMutationErrorDataFromMessage(asRecord(asRecord(error)?.cause)?.message);

  if (!payload) {
    return null;
  }

  return new PersistedMutationError(payload, acknowledgeImpl, error);
}

function parsePersistedMutationErrorData(value: unknown): PersistedMutationErrorData | null {
  const record = asRecord(value);
  if (!record) {
    return null;
  }

  const mutationId = asString(record.mutationId);
  const rootMutationId = asString(record.rootMutationId);
  const objectId = asString(record.objectId);
  const branchName = asString(record.branchName);
  const operation = asMutationOperation(record.operation);
  const code = asMutationRejectCode(record.code);
  const reason = asString(record.reason);
  const rejectedAtMicros = asNumber(record.rejectedAtMicros);
  const commitIds = asStringArray(record.commitIds);
  const previousCommitIds = asStringArray(record.previousCommitIds);

  if (
    !mutationId ||
    !rootMutationId ||
    !objectId ||
    !branchName ||
    !operation ||
    !code ||
    !reason ||
    rejectedAtMicros === null ||
    !commitIds ||
    !previousCommitIds
  ) {
    return null;
  }

  return {
    mutationId,
    rootMutationId,
    objectId,
    branchName,
    operation,
    commitIds,
    previousCommitIds,
    code,
    reason,
    rejectedAtMicros,
  };
}

function parsePersistedMutationErrorDataFromMessage(
  message: unknown,
): PersistedMutationErrorData | null {
  if (typeof message !== "string" || !message.startsWith(PERSISTED_MUTATION_ERROR_CAUSE_PREFIX)) {
    return null;
  }

  try {
    return parsePersistedMutationErrorData(
      JSON.parse(message.slice(PERSISTED_MUTATION_ERROR_CAUSE_PREFIX.length)),
    );
  } catch {
    return null;
  }
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null ? (value as Record<string, unknown>) : null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function asStringArray(value: unknown): string[] | null {
  return Array.isArray(value) && value.every((entry) => typeof entry === "string")
    ? [...value]
    : null;
}

function asMutationRejectCode(value: unknown): MutationRejectCode | null {
  switch (value) {
    case "permission_denied":
    case "session_required":
    case "catalogue_write_denied":
      return value;
    default:
      return null;
  }
}

function asMutationOperation(value: unknown): MutationOperation | null {
  switch (value) {
    case "insert":
    case "update":
    case "delete":
      return value;
    default:
      return null;
  }
}
