/** Stable `code` of a changed-authority refusal on every platform and binding. */
export const AUTHORITY_CHANGED_ERROR_CODE = "authority_changed";

/**
 * The server serves another history than the one this client's local store
 * follows, so the client stopped syncing with it.
 *
 * Each server store has an authority namespace, minted once when the store
 * is created. A server whose store was recreated (its data directory
 * deleted, an in-memory dev server restarted, a new environment behind the
 * same URL) has a new one, and so does another app's server. The local
 * store's rows, sync positions and unsynced writes belong to the old
 * history, so the link stops: `db.getSyncStatus().link` is
 * `{ state: "offline", reason: "authority-changed" }`, remote subscriptions
 * end with this error, and waits for the server (`wait({ tier: "global" })`,
 * `waitForSync()`) fail with it instead of hanging. Local reads and writes
 * keep working, and unsynced writes are kept.
 *
 * Nothing is reset automatically. To sync with the new server, reset the
 * local store: in the browser, `await db.deleteClientStorage()` (the client
 * reconnects on a fresh store); in Node, delete the client's data directory
 * and open the client again. Writes that never reached the
 * old server are lost with the store. If the server was replaced by
 * mistake, bring the old one back and call `db.reconnect()` instead.
 *
 * Class identity does not survive native bindings or browser worker
 * messages, so detect it with {@link isAuthorityChangedError}, which checks
 * the stable `code`.
 */
export class AuthorityChangedError extends Error {
  override readonly name = "AuthorityChangedError";
  readonly code = AUTHORITY_CHANGED_ERROR_CODE;

  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
  }
}

/**
 * Whether `error` is a changed-authority refusal, by its stable `code`: an
 * {@link AuthorityChangedError}, or an error a native binding or worker relay
 * threw with code `"authority_changed"`.
 */
export function isAuthorityChangedError(
  error: unknown,
): error is Error & { readonly code: typeof AUTHORITY_CHANGED_ERROR_CODE } {
  if (!error || typeof error !== "object") return false;
  return (error as { code?: unknown }).code === AUTHORITY_CHANGED_ERROR_CODE;
}

/**
 * The refusal a binding's `Transport.upstreamRefusal()` reported, as an
 * error: an {@link AuthorityChangedError} for a changed authority, an
 * `Error` keeping the core `code` otherwise, and `null` for no refusal.
 */
export function upstreamRefusalError(refusal: unknown): Error | null {
  if (refusal === null || refusal === undefined) return null;
  const message =
    typeof refusal === "object" && typeof (refusal as { message?: unknown }).message === "string"
      ? (refusal as { message: string }).message
      : String(refusal);
  if (isAuthorityChangedError(refusal)) {
    return new AuthorityChangedError(message, { cause: refusal });
  }
  if (refusal instanceof Error) return refusal;
  const error = new Error(message);
  const code = typeof refusal === "object" ? (refusal as { code?: unknown }).code : undefined;
  if (typeof code === "string") Object.assign(error, { code });
  return error;
}
