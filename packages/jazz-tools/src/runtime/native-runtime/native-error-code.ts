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
  | "historical_read_requires_server";

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
]);

/** The core `ErrorCode` a thrown value carries, or `undefined` for any other value. */
export function nativeCoreErrorCode(error: unknown): NativeCoreErrorCode | undefined {
  const code = nativeErrorCode(error);
  return code !== undefined && NATIVE_CORE_ERROR_CODES.has(code)
    ? (code as NativeCoreErrorCode)
    : undefined;
}
