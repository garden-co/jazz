/**
 * The stable code a core Jazz error carries across the native boundary.
 *
 * NAPI, WASM and React Native bindings throw a core error as an `Error` whose
 * `message` is the unchanged Rust display text (for example
 * `"NotObserved: …"`) and whose `code` is the snake_case core `ErrorCode`
 * (for example `"not_observed"`). The Rust `ErrorCode::as_str` table is the
 * single source of these strings.
 *
 * NAPI keeps its historical status name (`"GenericFailure"`) as the `code` of
 * errors that are not core errors, so a present code is not by itself proof
 * that an error came from core: compare against a specific core code.
 */
export type NativeCoreErrorCode =
  | "schema"
  | "query"
  | "write_rejected"
  | "transaction_conflict"
  | "storage"
  | "protocol"
  | "backpressure"
  | "not_observed"
  | "historical_read_requires_server"
  | "unsupported_storage_codecs";

/** Read the string `code` of a thrown value, or `undefined` when it has none. */
export function nativeErrorCode(error: unknown): string | undefined {
  if (!error || typeof error !== "object") return undefined;
  const code = (error as { code?: unknown }).code;
  return typeof code === "string" ? code : undefined;
}

const NATIVE_CORE_ERROR_CODES: ReadonlySet<string> = new Set<NativeCoreErrorCode>([
  "schema",
  "query",
  "write_rejected",
  "transaction_conflict",
  "storage",
  "protocol",
  "backpressure",
  "not_observed",
  "historical_read_requires_server",
  "unsupported_storage_codecs",
]);

/** The core `ErrorCode` a thrown value carries, or `undefined` for any other value. */
export function nativeCoreErrorCode(error: unknown): NativeCoreErrorCode | undefined {
  const code = nativeErrorCode(error);
  return code !== undefined && NATIVE_CORE_ERROR_CODES.has(code)
    ? (code as NativeCoreErrorCode)
    : undefined;
}

/**
 * The stable reason a core Jazz error carries across the native boundary,
 * finer than its code: the Rust `Reason::as_str` name, set as `error.reason`
 * by WASM, by NAPI promise rejections and by React Native. A NAPI synchronous
 * throw carries only its code, since napi-rs creates it from a code and a
 * message alone; that includes a pollable NAPI read or `waitForPendingWrites`
 * whose `poll()` throws.
 */
export type NativeCoreErrorReason =
  | "authorization_denied"
  | "client_clock_too_far_ahead"
  | "serialization_conflict"
  | "row_already_deleted"
  | "row_already_exists"
  | "row_not_deleted"
  | "missing_required"
  | "branch_move"
  | "missing_chunks"
  | "permissions_head_missing"
  | "predecessor_rejected"
  | "identity_mismatch";

/** Every reason this build knows; a test checks it against the engine's `Reason::ALL`. */
export const NATIVE_CORE_ERROR_REASONS: ReadonlySet<string> = new Set<NativeCoreErrorReason>([
  "authorization_denied",
  "client_clock_too_far_ahead",
  "serialization_conflict",
  "row_already_deleted",
  "row_already_exists",
  "row_not_deleted",
  "missing_required",
  "branch_move",
  "missing_chunks",
  "permissions_head_missing",
  "predecessor_rejected",
  "identity_mismatch",
]);

/** The core `Reason` a thrown value carries, or `undefined` when it has none this build knows. */
export function nativeCoreErrorReason(error: unknown): NativeCoreErrorReason | undefined {
  if (!error || typeof error !== "object") return undefined;
  const reason = (error as { reason?: unknown }).reason;
  return typeof reason === "string" && NATIVE_CORE_ERROR_REASONS.has(reason)
    ? (reason as NativeCoreErrorReason)
    : undefined;
}
