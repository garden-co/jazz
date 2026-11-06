/**
 * Why a Jazz session stopped using a `Db`: it moved to the local-first client
 * (the background upgrade), or its user signed out or switched accounts.
 */
export type DbReplacedReason = "upgraded" | "signed-out";

/**
 * The `Db` was replaced by its Jazz session, so this handle is closed: its
 * live queries end with this error and its writes throw it.
 *
 * A session swaps its client when it upgrades to the local-first client, and
 * when its user signs out or switches accounts. Read the current `Db` from
 * `session.getSnapshot().client` inside `session.subscribe(...)` and subscribe
 * again there. The framework providers do this for you.
 */
export class DbReplacedError extends Error {
  readonly reason: DbReplacedReason;

  constructor(reason: DbReplacedReason) {
    super(
      `This Db was replaced by its Jazz session (${
        reason === "upgraded"
          ? "it upgraded to the local-first client"
          : "its user signed out or switched accounts"
      }), so it no longer reads or writes. Read the current Db from session.getSnapshot().client inside session.subscribe(...), and subscribe to your queries again there.`,
    );
    this.name = "DbReplacedError";
    this.reason = reason;
  }
}
