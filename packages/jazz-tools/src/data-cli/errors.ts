/**
 * Stable error contract for `jazz-tools sql` and the data-backed `schema`
 * subcommands.
 *
 * Every failure carries a machine-readable code plus a documented exit code, so
 * callers never have to match on human prose. Text mode prints `CODE: message`;
 * `--format json|jsonl` emits `{"error":{"code","message","hint","exitCode"}}`.
 */
export type DataErrorCode =
  | "USAGE"
  | "SQL_SYNTAX"
  | "UNKNOWN_TABLE"
  | "UNKNOWN_COLUMN"
  | "UNSUPPORTED"
  | "TYPE_MISMATCH"
  | "READ_ONLY"
  | "SCHEMA_NOT_FOUND"
  | "AUTH_REQUIRED"
  | "AUTH_CONFLICT"
  | "DENIED"
  | "ALREADY_EXISTS"
  | "TIMEOUT"
  | "INTERNAL";

/**
 * Documented exit-code contract, shared by every data command:
 * 0 success, 1 unexpected internal failure, 2 usage/invalid input,
 * 3 missing or ambiguous credentials, 4 denied by the server, 5 timeout,
 * 6 the target row already exists (a previous attempt committed).
 */
export const EXIT_CODE_BY_ERROR: Record<DataErrorCode, number> = {
  USAGE: 2,
  SQL_SYNTAX: 2,
  UNKNOWN_TABLE: 2,
  UNKNOWN_COLUMN: 2,
  UNSUPPORTED: 2,
  TYPE_MISMATCH: 2,
  READ_ONLY: 2,
  SCHEMA_NOT_FOUND: 2,
  AUTH_REQUIRED: 3,
  AUTH_CONFLICT: 3,
  DENIED: 4,
  ALREADY_EXISTS: 6,
  TIMEOUT: 5,
  INTERNAL: 1,
};

export interface DataErrorOptions {
  hint?: string;
  cause?: unknown;
}

export class DataError extends Error {
  readonly code: DataErrorCode;
  readonly hint: string | undefined;
  readonly exitCode: number;

  constructor(code: DataErrorCode, message: string, options: DataErrorOptions = {}) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "DataError";
    this.code = code;
    this.hint = options.hint;
    this.exitCode = EXIT_CODE_BY_ERROR[code];
  }
}

export function isDataError(error: unknown): error is DataError {
  return error instanceof DataError;
}

// Best-effort classification of SDK/server failures that do not already carry a
// code. 401-style failures mean "no usable credential" (3); 403-style failures
// mean "credential accepted, operation refused" (4).
const DENIED_PATTERN = /\b403\b|forbidden|denied|not allowed|violates policy|rejected/i;
const AUTH_PATTERN =
  /\b401\b|unauthori[sz]ed|identity_not_assigned|invalid (?:admin|backend) secret|permissions head fetch failed/i;

export function classifyError(error: unknown): DataError {
  if (isDataError(error)) return error;
  const message = error instanceof Error ? error.message : String(error);
  const name = error instanceof Error ? error.name : "";
  if (name === "PersistedWriteRejectedError")
    return new DataError("DENIED", message, { cause: error });
  // A seeded INSERT that is retried after a committed attempt lands here, which
  // is how a caller learns the earlier attempt succeeded (never duplicated).
  if (/object already exists|already exists/i.test(message))
    return new DataError("ALREADY_EXISTS", message, {
      cause: error,
      hint: "The earlier attempt appears to have committed; read the row before retrying.",
    });
  if (DENIED_PATTERN.test(message)) return new DataError("DENIED", message, { cause: error });
  if (AUTH_PATTERN.test(message)) return new DataError("AUTH_REQUIRED", message, { cause: error });
  return new DataError("INTERNAL", message, { cause: error });
}

function editDistance(a: string, b: string): number {
  // Full matrix, because adjacent transpositions ("txet" -> "text") need the
  // two-rows-back cell. They are the most common typo and would otherwise never
  // earn a suggestion.
  const rows: number[][] = [Array.from({ length: b.length + 1 }, (_unused, j) => j)];
  for (let i = 1; i <= a.length; i++) rows.push([i, ...Array.from({ length: b.length }, () => 0)]);
  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      let best = Math.min(rows[i - 1]![j]! + 1, rows[i]![j - 1]! + 1, rows[i - 1]![j - 1]! + cost);
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1])
        best = Math.min(best, rows[i - 2]![j - 2]! + 1);
      rows[i]![j] = best;
    }
  }
  return rows[a.length]![b.length]!;
}

/** Nearest candidate within an edit-distance budget, for "did you mean" hints. */
export function nearestName(input: string, candidates: readonly string[]): string | undefined {
  const needle = input.toLowerCase();
  let best: { name: string; distance: number } | undefined;
  for (const candidate of candidates) {
    const distance = editDistance(needle, candidate.toLowerCase());
    if (best === undefined || distance < best.distance) best = { name: candidate, distance };
  }
  if (best === undefined) return undefined;
  const budget = Math.max(1, Math.floor(input.length / 3));
  return best.distance <= budget ? best.name : undefined;
}
