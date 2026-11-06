/**
 * Why a Jazz session stopped using a `Db`: its user signed out or switched
 * accounts, or the session swapped in another client for the same account.
 */
export type DbReplacedReason = "signed-out" | "replaced";

/**
 * The `Db` was replaced by its Jazz session, so this handle is closed: its
 * live queries end with this error and its writes throw it.
 *
 * A session swaps its client when its user signs out or switches accounts.
 * (The upgrade to the local-first client keeps the same `Db`.) Read the current `Db` from
 * `session.getSnapshot().client` inside `session.subscribe(...)` and subscribe
 * again there. The framework providers do this for you.
 */
export class DbReplacedError extends Error {
  readonly reason: DbReplacedReason;

  constructor(reason: DbReplacedReason) {
    super(
      `This Db was replaced by its Jazz session (${
        reason === "signed-out"
          ? "its user signed out or switched accounts"
          : "it swapped in another client"
      }), so it no longer reads or writes. Read the current Db from session.getSnapshot().client inside session.subscribe(...), and subscribe to your queries again there.`,
    );
    this.name = "DbReplacedError";
    this.reason = reason;
  }
}
