import type { AccountIdentity } from "../accounts/state.js";
import { AccountAuthError } from "../accounts/registry-client.js";
import type { JazzSession } from "./state.js";

const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";
const SYSTEM_ISSUER = "urn:jazz:system";

export interface BackendWatchEvents {
  /** Call `check` when the page or app comes back to the foreground. */
  onFocus(check: () => void): () => void;
  /** Tell other tabs this one changed account; call `check` when another tab says so. */
  crossTab?: { post(): void; listen(check: () => void): () => void };
  /** Whether the page or app is in the foreground now (its cookies are fresh). */
  isVisible?(): boolean;
}

/**
 * Who the backend says is signed in. `hasAccount: false` means the core holds
 * no account for them yet (they just signed up, or they are an existing user
 * of the app signing in to Jazz for the first time), so a local-first guest
 * on this device links to them and keeps its data, unless `linkGuest` is
 * off. Left out, the user is taken to have one.
 */
export type BackendUser = AccountIdentity & { hasAccount?: boolean };

export interface BackendWatch {
  /** The sync server refused this client's sign-in: ask the backend whether it still holds. */
  authRefused(): void;
  /** The sync link is connected again (resets the reconnect backoff). */
  connected(): void;
  /**
   * The app's own auth changed (it signed in, out or as someone else): ask the
   * backend now, and follow its answer without waiting for a second one.
   * Resolves once the session follows that answer; rejects when the backend
   * can't be asked (a network error or a 5xx) or following it fails. A
   * session in error for the same user retries.
   */
  reauthenticate(): Promise<void>;
  /**
   * The guest link this watch started and that is still running, if any, and
   * the account its last link ended on. An app that calls `linkWithBackend`
   * itself after its sign-up can then join the link the watch already made.
   */
  readonly linking: { done: Promise<void>; result: unknown } | undefined;
  stop(): void;
}

/**
 * Keep a session in step with the app's backend: ask it who is signed in on
 * start, when the page regains focus and when another tab signs in or out.
 *
 * - A different user signs in. Switching from one backend user to another
 *   signs the first out, so their client closes as on sign-out.
 * - A signed-out answer signs out a backend user, once a second answer
 *   `confirmMs` later agrees, so one failed session refresh in `authenticate`
 *   doesn't sign anyone out (or, with `clearOnSignOut`, wipe their writes).
 * - A local-first guest stays a guest until someone signs in. When the user
 *   who signs in has no account yet (they just signed up), the guest links to
 *   them (`linkWithBackend`), so everything it wrote stays theirs, however
 *   the session found out: the app's `reauthenticate`, focus after a popup or
 *   the first check after a sign-up redirect. A user who already has an
 *   account is signed in instead; the guest stays on this device. So is a
 *   user the guest's link was refused for (its key retired, or another app's
 *   guest), and once refused the watch never links that guest to that user
 *   again. With `linkGuest: false` the watch leaves such a guest to the app.
 *
 * The app calls `reauthenticate` after its own sign-in or sign-out, so the
 * session follows at once.
 *
 * A sync server refusing the sign-in (`authRefused`) asks the backend too, but
 * only to reconnect, with backoff, while it still names the same user. It
 * never signs anyone out: a background tab's cookie may just be stale, so a
 * sign-out waits for a check after focus returns (or for the app to call
 * `reauthenticate`). A probe that reports there is no backend (404) stops the
 * watch.
 */
export function watchBackendUser<Client>(
  session: JazzSession<Client>,
  probe: () => Promise<BackendUser | null>,
  events: BackendWatchEvents,
  options: {
    confirmMs?: number;
    /** Reconnect the current client, once the backend says its user is still signed in. */
    reconnect?: () => void;
    /** The first wait before reconnecting after a refusal; it doubles up to a minute. */
    reconnectBackoffMs?: number;
    /**
     * After a refusal the backend agrees with (no one signed in) while the
     * page is visible, wait this long and check again as on focus, so a
     * revoked user in a tab that stays focused is signed out. Default 30s.
     */
    refusedConfirmMs?: number;
    /**
     * Link a local-first guest to a signed-in user who has no account yet
     * (default true). Off, the watch leaves that guest as it is: the app
     * decides, with `linkWithBackend` (keep the guest's data) or
     * `loginWithBackend` (an account of the user's own).
     */
    linkGuest?: boolean;
  } = {},
): BackendWatch {
  const confirmMs = options.confirmMs ?? 2000;
  const firstBackoffMs = options.reconnectBackoffMs ?? 1000;
  let backoffMs = firstBackoffMs;
  let reconnectAt = 0;
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  // The sync link was refused and hasn't connected since.
  let refusedPending = false;
  let refusedConfirm: ReturnType<typeof setTimeout> | undefined;
  const refusedConfirmMs = options.refusedConfirmMs ?? 30_000;
  const resetRefusal = () => {
    refusedPending = false;
    backoffMs = firstBackoffMs;
    reconnectAt = 0;
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
    if (refusedConfirm) clearTimeout(refusedConfirm);
    refusedConfirm = undefined;
  };
  let stopped = false;
  const linkGuest = options.linkGuest ?? true;
  // The session's own link, not one a `decorate` (followUser) put over it:
  // that one joins the link this watch runs.
  const linkWithBackend = session.linkWithBackend;
  let linking: { done: Promise<void>; result: unknown } | undefined;
  // A guest whose link to a user was refused for good, so later checks sign
  // that user in instead of reopening the guest for the same refusal.
  let refusedLink: { account: unknown; issuer: string; subject: string } | undefined;
  // Links the backend ended as signed out although its probe named a user:
  // one re-check follows the newer answer, more would loop if they disagree.
  let signedOutLinks = 0;
  let checking: Promise<void> | undefined;
  // Checks asked for while one runs: a plain one (may sign out), a refused
  // one (only reconnects) and an asked one (the app's auth changed: no
  // confirming second answer) are kept apart.
  let again: { plain: boolean; refused: boolean; asked: boolean } | undefined;
  // `reauthenticate` callers not yet answered. An asked check takes the ones
  // waiting when it starts, so each is answered by a probe made after it
  // called, and settles them once it has followed (or failed to follow) it.
  type Waiter = { resolve(): void; reject(error: Error): void };
  let waiters: Waiter[] = [];
  let announced = session.getSnapshot().account;

  const settled = () => {
    const snapshot = session.getSnapshot();
    return snapshot.status === "ready" || snapshot.status === "signed-out";
  };
  // A failed sign-in leaves the session in error; the app asking again after
  // its auth changed is how it recovers.
  const askable = () => settled() || session.getSnapshot().status === "error";
  const quiet = (asked = false) => stopped || !(asked ? askable() : settled());
  async function ask(): Promise<BackendUser | null | { failed: unknown }> {
    try {
      return await probe();
    } catch (cause) {
      // A backend without the backend-user route (a createJazz older than
      // this client) answers 404 too, and stops the watch. Clients and the
      // backend upgrade in lockstep, so that is not supported.
      if (cause instanceof AccountAuthError && cause.status === 404) {
        // No backend to ask: reauthenticates still waiting fail like this one.
        const queued = waiters.splice(0);
        watch.stop();
        settle(queued, new Error("Jazz found no backend to ask who is signed in", { cause }));
      }
      return { failed: cause };
    }
  }
  /**
   * One check. An asked one throws when it can't follow the backend, and
   * returns "again" when the session started a transition meanwhile, so it
   * asks again once that settles.
   */
  async function once(refused: boolean, asked = false): Promise<"again" | void> {
    // Plain checks swallow a failed transition: the session shows it.
    const attempt = (run: Promise<unknown>) =>
      asked
        ? run.then(() => {})
        : run.then(
            () => {},
            () => {},
          );
    const busy = () => (asked && !stopped ? "again" : undefined);
    if (quiet(asked)) return busy();
    let signedIn = await ask();
    if (signedIn !== null && "failed" in signedIn) {
      if (!asked) return;
      throw new Error("Jazz could not ask your backend who is signed in", {
        cause: signedIn.failed,
      });
    }
    if (quiet(asked)) return busy();
    const current = session.getSnapshot().account?.identity;
    if (current?.issuer === SYSTEM_ISSUER) return;
    const backendUser = current !== undefined && current.issuer !== LOCAL_FIRST_ISSUER;
    if (signedIn) {
      if (current?.issuer === signedIn.issuer && current.subject === signedIn.subject) {
        // The same user, whose client failed to open: the app asking is
        // how it recovers.
        if (asked && session.getSnapshot().status === "error") return attempt(session.retry());
        // Still signed in: the refusal came from expired cookies on the old socket.
        if (refused || refusedPending) scheduleReconnect();
        return;
      }
      if (refused) return;
      if (backendUser) await attempt(session.logout());
      if (quiet(asked)) return busy();
      // A guest of this device and a user with no account yet: link them,
      // never sign the user in first (that would give them an account of
      // their own, and the guest could never link to them after).
      const guest = current?.issuer === LOCAL_FIRST_ISSUER;
      const account = session.getSnapshot().account;
      const refusedBefore =
        refusedLink !== undefined &&
        refusedLink.account === account &&
        refusedLink.issuer === signedIn.issuer &&
        refusedLink.subject === signedIn.subject;
      // Not linking on its own: the app decides what becomes of this guest.
      if (guest && !linkGuest && signedIn.hasAccount === false) return;
      if (guest && signedIn.hasAccount === false && !refusedBefore) {
        const link = { done: linkWithBackend(), result: undefined as unknown };
        linking = link;
        try {
          await link.done;
          link.result = session.getSnapshot().account;
          signedOutLinks = 0;
          return;
        } catch (cause) {
          if (linking === link) linking = undefined;
          if (stopped) return;
          if (signedOutMeanwhile(cause) && signedOutLinks++ === 0) {
            // Signed out while linking: follow the backend's new answer.
            if (asked) return "again";
            check();
            return;
          }
          if (!linkRefused(cause)) {
            // A network error, a 5xx, an expired challenge, or a backend
            // whose answers keep disagreeing: a later check tries again.
            if (asked) throw cause;
            return;
          }
          signedOutLinks = 0;
          refusedLink = { account, issuer: signedIn.issuer, subject: signedIn.subject };
          console.warn(
            "Jazz could not link this device's guest to the user who signed in, so it signs them in to their own account. The guest stays on this device.",
            cause,
          );
        }
        if (quiet(asked)) return busy();
      }
      return attempt(session.loginWithBackend());
    }
    signedOutLinks = 0;
    if (!backendUser) return;
    if (refused) {
      // Never sign out on a refusal alone (the cookie may be stale), but a
      // visible page's cookies are fresh: check again shortly as on focus.
      if (!refusedConfirm && (events.isVisible?.() ?? false))
        refusedConfirm = setTimeout(() => {
          refusedConfirm = undefined;
          check();
        }, refusedConfirmMs);
      return;
    }
    if (!asked) {
      await new Promise((resolve) => setTimeout(resolve, confirmMs));
      if (quiet()) return;
      signedIn = await ask();
      if (signedIn !== null || quiet()) return;
    }
    return attempt(session.logout());
  }
  /** Run `run` once the session is no longer mid-transition (or now). */
  function whenSettled(run: () => void) {
    if (stopped || askable()) return run();
    const stop = session.subscribe(() => {
      if (!stopped && !askable()) return;
      stop();
      run();
    });
  }
  function scheduleReconnect() {
    if (reconnectTimer || stopped) return;
    // Back off in case the socket keeps missing a cookie the probe gets
    // (its Path, Domain or SameSite differ).
    const wait = reconnectAt - Date.now();
    const reconnect = () => {
      reconnectTimer = undefined;
      if (stopped) return;
      reconnectAt = Date.now() + backoffMs;
      backoffMs = Math.min(backoffMs * 2, 60_000);
      options.reconnect?.();
    };
    if (wait <= 0) reconnect();
    else reconnectTimer = setTimeout(reconnect, wait);
  }
  function settle(done: Waiter[], error?: Error) {
    for (const waiter of done) {
      if (error) waiter.reject(error);
      else waiter.resolve();
    }
  }
  function check(refused = false, asked = false) {
    if (stopped) return settle(waiters.splice(0));
    if (checking) {
      again = {
        plain: (!refused && !asked) || !!again?.plain,
        refused: refused || !!again?.refused,
        asked: asked || !!again?.asked,
      };
      return;
    }
    const batch = asked ? waiters.splice(0) : [];
    let retry = false;
    let failure: Error | undefined;
    checking = once(refused, asked)
      .then(
        (outcome) => {
          retry = outcome === "again";
        },
        (cause: unknown) => {
          failure = cause instanceof Error ? cause : new Error(String(cause));
        },
      )
      .finally(() => {
        checking = undefined;
        const next = again;
        again = undefined;
        if (retry) waiters.unshift(...batch);
        else settle(batch, failure);
        // An asked check answers a plain one too. It waits for the session
        // to settle, so it follows the backend instead of finding the
        // session busy.
        if (retry || next?.asked) whenSettled(() => check(false, true));
        else if (next?.plain) check(false);
        if (next?.refused) check(true);
      });
  }
  const unsubscribe = session.subscribe(() => {
    const snapshot = session.getSnapshot();
    if (snapshot.status !== "ready" && snapshot.status !== "signed-out") return;
    if (snapshot.account === announced) return;
    announced = snapshot.account;
    resetRefusal();
    events.crossTab?.post();
  });
  const stopFocus = events.onFocus(() => check());
  const stopCrossTab = events.crossTab?.listen(() => check());
  // Ask on start, once the host has finished setting up.
  const start = setTimeout(() => check(), 0);
  const watch: BackendWatch = {
    authRefused() {
      refusedPending = true;
      check(true);
    },
    connected: resetRefusal,
    get linking() {
      return linking;
    },
    reauthenticate() {
      const done = new Promise<void>((resolve, reject) => waiters.push({ resolve, reject }));
      whenSettled(() => check(false, true));
      return done;
    },
    stop() {
      if (stopped) return;
      stopped = true;
      settle(waiters.splice(0));
      clearTimeout(start);
      resetRefusal();
      unsubscribe();
      stopFocus();
      stopCrossTab?.();
    },
  };
  return watch;
}

/** The backend says no one is signed in any more (its `authenticate` answered no user). */
function signedOutMeanwhile(cause: unknown): boolean {
  return cause instanceof AccountAuthError && cause.status === 401 && cause.code === "signed_out";
}

/**
 * The backend refused the link itself, and would again: the user belongs to
 * another account by now, or the guest's key was retired or belongs to
 * another app (`link_refused`, `invalid_local_first_proof`), or this guest
 * can't link here at all. Anything else (a network error, a 5xx, a timeout,
 * a challenge that expired while the tab slept) may pass on a retry.
 */
function linkRefused(cause: unknown): boolean {
  if (!(cause instanceof AccountAuthError)) return false;
  return (
    cause.code === "link_refused" ||
    cause.code === "invalid_local_first_proof" ||
    cause.code === "local_first_account_required" ||
    cause.code === "linked_account_mismatch"
  );
}

/** Focus, visibility and BroadcastChannel events for a browser page. */
export function browserBackendWatchEvents(channelName: string): BackendWatchEvents {
  const crossTab =
    typeof BroadcastChannel === "function"
      ? (() => {
          const channel = new BroadcastChannel(channelName);
          const listeners = new Set<() => void>();
          channel.onmessage = () => {
            for (const listener of listeners) listener();
          };
          return {
            post: () => channel.postMessage("account-changed"),
            listen(check: () => void) {
              listeners.add(check);
              return () => {
                listeners.delete(check);
                if (!listeners.size) channel.close();
              };
            },
          };
        })()
      : undefined;
  return {
    onFocus(check) {
      if (typeof window === "undefined" || typeof document === "undefined") return () => {};
      const onVisible = () => {
        if (document.visibilityState === "visible") check();
      };
      window.addEventListener("focus", check);
      document.addEventListener("visibilitychange", onVisible);
      return () => {
        window.removeEventListener("focus", check);
        document.removeEventListener("visibilitychange", onVisible);
      };
    },
    crossTab,
    isVisible: () => typeof document === "undefined" || document.visibilityState === "visible",
  };
}
