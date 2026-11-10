/** Stable `code` of a {@link StoreHeldByAnotherTabError}. */
export const STORE_HELD_BY_ANOTHER_TAB_ERROR_CODE = "store_held_by_another_tab";

/**
 * Another tab of this browser holds this account's local store under
 * another sign-in, so this tab cannot open it yet.
 *
 * This happens when a guest signs up (or in) in one tab while another tab is
 * still the guest: the store is the same account's, and one store serves one
 * sign-in at a time. The other tab normally follows the sign-up within
 * moments, or closes, and this tab joins on its own. Until then its sync
 * status is `connecting`.
 *
 * If the other tab does not let go for a while (it is frozen in the
 * background, or runs an app build that does not follow sign-ups), the
 * status becomes `{ state: "offline", reason: "held-by-another-tab",
 * retrying: true }`, and reads and waits that were waiting for the store
 * fail with this error, so the app can ask the user to close or reload the
 * other tab. The tab keeps trying in the background and joins as soon as
 * the other tab lets go. Writes made in the meantime stay in this tab's
 * memory until it joins: a reload before then loses them.
 *
 * A wait or read that failed with this error stays failed: it doesn't
 * resume when the tab joins. The writes behind a failed `wait()` are not
 * lost; they go to the store once the tab joins. Call `wait()` (or the
 * read) again once the sync status is no longer `held-by-another-tab`.
 * Calling it while the store is still held fails at once with this error.
 * Live queries that read only this tab's own data keep running throughout.
 *
 * Detect it with {@link isStoreHeldByAnotherTabError}, which checks the
 * stable `code`.
 */
export class StoreHeldByAnotherTabError extends Error {
  override readonly name = "StoreHeldByAnotherTabError";
  readonly code = STORE_HELD_BY_ANOTHER_TAB_ERROR_CODE;

  constructor(
    message = "Another tab holds this account's local store under another sign-in: close or reload that other tab. This tab joins once it lets go; reloading this tab first loses its unsynced writes",
    options?: { cause?: unknown },
  ) {
    super(message, options);
  }
}

/** Whether `error` is a {@link StoreHeldByAnotherTabError}, by its stable `code`. */
export function isStoreHeldByAnotherTabError(
  error: unknown,
): error is Error & { readonly code: typeof STORE_HELD_BY_ANOTHER_TAB_ERROR_CODE } {
  if (!error || typeof error !== "object") return false;
  return (error as { code?: unknown }).code === STORE_HELD_BY_ANOTHER_TAB_ERROR_CODE;
}
