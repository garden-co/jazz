import type { AccountIdentity } from "../accounts/state.js";
import { AccountAuthError } from "../accounts/registry-client.js";
import { switchJazzSessionUser, type JazzSession } from "./state.js";

const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";
const SYSTEM_ISSUER = "urn:jazz:system";

/** What an app's `onSignedOut` hook decides (see `watchBackendUser`). */
export type SignedOutDecision = "sign-out" | "keep";

export interface BackendWatchEvents {
  /** Call `check` when the page or app comes back to the foreground. */
  onFocus(check: () => void): () => void;
  /** Tell other tabs this one changed account; call `check` when another tab says so. */
  crossTab?: { post(): void; listen(check: () => void): () => void };
  /** Whether the page or app is in the foreground now (its cookies are fresh). */
  isVisible?(): boolean;
  /**
   * Run `probe` so that the windows of one browser ask the backend one at a
   * time, never at once, and a window waiting its turn takes the answer of a
   * probe that started after it asked instead of asking again. Several
   * windows asking at once (on focus after a laptop wakes) each carry the
   * session cookie, and auth middleware on the route may refresh it once per
   * request: with single-use refresh tokens, all but one refresh fail.
   */
  singleFlight?<T>(probe: () => Promise<T>): Promise<T>;
}

/**
 * Who the backend says is signed in. `hasAccount: false` means the core holds
 * no account for them yet (they just signed up, or they are an existing user
 * of the app signing in to Jazz for the first time), so a local-first guest
 * on this device links to them and keeps its data, unless `linkGuest` is
 * off. Left out, the user is taken to have one.
 */
export type BackendUser = AccountIdentity & {
  hasAccount?: boolean;
  /**
   * The user's claims as the backend states them now. A session that opened
   * on a user this device remembered refreshes them when they differ.
   */
  claims?: Readonly<Record<string, unknown>>;
};

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
  /**
   * Settles once the first check that may sign someone in (the one on start,
   * or a focus or reauthenticate check that ran before it) has finished and
   * the session follows its answer, or that check failed, or the watch
   * stopped. A refused check doesn't count: it only reconnects. Never rejects.
   */
  readonly started: Promise<void>;
  stop(): void;
}

/**
 * Keep a session in step with the app's backend: ask it who is signed in on
 * start, when the page regains focus and when another tab signs in or out.
 *
 * - A different user signs in. Switching from one backend user to another
 *   signs the first out, so their client closes as on sign-out, in one
 *   transition: the session never shows `signed-out` between the two.
 * - A signed-out answer signs out a backend user, once a second answer
 *   `confirmMs` later agrees, so one failed session refresh in `authenticate`
 *   doesn't sign anyone out (or, with `clearOnSignOut`, wipe their writes).
 *   It signs out only the account the session held when the check started:
 *   one the app switched to meanwhile (say a guest after its own sign-out)
 *   stays.
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
 * A check that fails for a reason that passes (a network error, a timeout,
 * a 5xx), asking the backend or following its answer, is tried again with
 * backoff, so a blip leaves no tab where it was until focus returns.
 *
 * The app calls `reauthenticate` after its own sign-in or sign-out, so the
 * session follows at once.
 *
 * A user the sync server refuses whom the backend still names but says has
 * no account (the core lost it: a reset dev store, a new core) is signed in
 * again, once, which gives them a new account; reconnecting would only be
 * refused again.
 *
 * A sync server refusing the sign-in (`authRefused`) asks the backend too, but
 * only to reconnect, with backoff, while it still names the same user. It
 * never signs anyone out: a background tab's cookie may just be stale, so a
 * sign-out waits for a check after focus returns (or for the app to call
 * `reauthenticate`). A probe that reports there is no backend (404) is asked
 * again with backoff, since a deploy or a proxy may answer 404 for a moment;
 * only 404s that keep coming for a while (`notFoundRetryMs`, three answers)
 * stop the watch. A stopped watch rejects every later `reauthenticate`.
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
    /**
     * The first wait before asking again after a 404 (it doubles). The watch
     * stops once three answers in a row, over at least three times this
     * long, said 404. Default 5s.
     */
    notFoundRetryMs?: number;
    /**
     * The first wait before checking again after a check failed for a
     * reason that passes (a network error, a timeout, a 5xx); it doubles up
     * to a minute. Default 2s.
     */
    retryMs?: number;
    /**
     * Called when your backend says no one is signed in any more (confirmed
     * by a second answer a moment later), before the session signs the
     * user out. Return `"keep"` to stay signed in for now (your app refreshed
     * its session itself, or redirects to sign-in first): the session asks
     * your backend again on the next focus, refusal or `reauthenticate()`.
     * Return `"sign-out"` (or nothing) to sign out. A hook that throws signs
     * out.
     */
    onSignedOut?: () => SignedOutDecision | void | Promise<SignedOutDecision | void>;
  } = {},
): BackendWatch {
  const confirmMs = options.confirmMs ?? 2000;
  const notFoundRetryMs = options.notFoundRetryMs ?? 5_000;
  // 404s in a row since the first of them, the retries scheduled since, and
  // how long those retries have waited. Counting the waits rather than
  // reading the clock keeps "for a while" exact: timers and `Date.now()` can
  // disagree by a millisecond at the boundary.
  let notFound: { count: number; waited: number; retries: number } | undefined;
  let notFoundTimer: ReturnType<typeof setTimeout> | undefined;
  const clearNotFound = () => {
    notFound = undefined;
    if (notFoundTimer) clearTimeout(notFoundTimer);
    notFoundTimer = undefined;
  };
  // A check that failed for a reason that passes is tried again with backoff,
  // so a blip doesn't leave the session where it was until focus returns.
  const firstRetryMs = options.retryMs ?? 2_000;
  let retryMs = firstRetryMs;
  let retryTimer: ReturnType<typeof setTimeout> | undefined;
  let retries = 0;
  const retryLater = (cause: unknown) => {
    if (stopped || !passes(cause) || retryTimer) return;
    retries++;
    const wait = retryMs;
    retryMs = Math.min(retryMs * 2, 60_000);
    retryTimer = setTimeout(() => {
      retryTimer = undefined;
      check();
    }, wait);
  };
  const clearRetry = () => {
    retryMs = firstRetryMs;
    if (retryTimer) clearTimeout(retryTimer);
    retryTimer = undefined;
  };
  // Why the watch stopped: later reauthenticates reject with it.
  let stopReason: Error | undefined;
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
  // A link the backend ended as signed out although its probe named this
  // user: one re-check follows the newer answer, more would loop if they
  // disagree. Kept per user, so another user gets their own re-check.
  let signedOutLink: string | undefined;
  // A signed-in user the backend said has no account any more, signed in
  // again once: kept per user until the backend says they have one.
  let lostAccount: string | undefined;
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
  // Settles once a check that may sign someone in (not a refused one) has
  // followed the backend's answer, or the watch stopped.
  let markStarted!: () => void;
  const started = new Promise<void>((resolve) => (markStarted = resolve));
  let announced = session.getSnapshot().account;
  // A user the session opened on (one this device remembered): the
  // backend's first answer naming them refreshes their claims if they changed.
  let remembered: unknown = (() => {
    const issuer = session.getSnapshot().account?.identity.issuer;
    return issuer && issuer !== LOCAL_FIRST_ISSUER && issuer !== SYSTEM_ISSUER
      ? session.getSnapshot().account
      : undefined;
  })();

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
      const answer = await probe();
      clearNotFound();
      return answer;
    } catch (cause) {
      if (cause instanceof AccountAuthError && cause.status === 404) notFoundAnswer(cause);
      return { failed: cause };
    }
  }
  /**
   * A 404 may pass (a deploy swapping routes, a CDN or protection page):
   * ask again with backoff. A backend without the backend-user route (a
   * createJazz older than this client) keeps answering 404 and stops the
   * watch; clients and the backend upgrade in lockstep, so that is not
   * supported.
   */
  function notFoundAnswer(cause: AccountAuthError) {
    if (stopped) return;
    notFound ??= { count: 0, waited: 0, retries: 0 };
    notFound.count++;
    if (notFound.count >= 3 && notFound.waited >= notFoundRetryMs * 3) {
      // No backend to ask: reauthenticates still waiting, and later ones, fail.
      const error = new Error("Jazz found no backend to ask who is signed in", { cause });
      const queued = waiters.splice(0);
      stopReason = error;
      watch.stop();
      settle(queued, error);
      return;
    }
    if (notFoundTimer) return;
    const wait = notFoundRetryMs * 2 ** Math.min(notFound.retries++, 4);
    const counting = notFound;
    notFoundTimer = setTimeout(() => {
      notFoundTimer = undefined;
      counting.waited += wait;
      check();
    }, wait);
  }
  /**
   * One check. An asked one throws when it can't follow the backend, and
   * returns "again" when the session started a transition meanwhile, so it
   * asks again once that settles.
   */
  async function once(refused: boolean, asked = false): Promise<"again" | void> {
    // Plain checks swallow a failed transition (the session shows it), and
    // try again later when it may pass.
    const attempt = (run: Promise<unknown>) =>
      asked ? run.then(() => {}) : run.then(() => {}, retryLater);
    const busy = () => (asked && !stopped ? "again" : undefined);
    if (quiet(asked)) return busy();
    // Only this account is signed out on the answer this check gets.
    const startedWith = session.getSnapshot().account;
    let signedIn = await ask();
    if (signedIn !== null && "failed" in signedIn) {
      if (!asked) {
        // A 404 has its own retry (see `notFoundAnswer`).
        if (!(signedIn.failed instanceof AccountAuthError && signedIn.failed.status === 404))
          retryLater(signedIn.failed);
        return;
      }
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
        const who = `${signedIn.issuer}\0${signedIn.subject}`;
        // The sync server refused them, and the core holds no account for
        // them any more (a reset dev store, a new core): it refuses this
        // account for good, so reconnecting would loop. Sign in again, which
        // gives them a new one. Once per user, so a backend whose answers keep
        // saying no account doesn't sign in again and again.
        if (signedIn.hasAccount === false && (refused || refusedPending)) {
          if (lostAccount === who) return;
          lostAccount = who;
          remembered = undefined;
          // A sign-in that fails (a network error, a 5xx) is tried again on
          // the next refusal.
          return attempt(
            session.loginWithBackend().catch((cause: unknown) => {
              if (lostAccount === who) lostAccount = undefined;
              throw cause;
            }),
          );
        }
        if (signedIn.hasAccount !== false && lostAccount === who) lostAccount = undefined;
        const account = session.getSnapshot().account;
        if (account === remembered && !refused) {
          remembered = undefined;
          // Reopened from this device's memory: the claims are as of an
          // earlier sign-in. Sign in again (one transition, never signed
          // out) when the backend states others now. A refused check leaves
          // that to the next one.
          if (signedIn.claims && !sameClaims(account?.claims ?? {}, signedIn.claims))
            return attempt(session.loginWithBackend());
        }
        // Still signed in: the refusal came from expired cookies on the old socket.
        if (refused || refusedPending) scheduleReconnect();
        return;
      }
      if (refused) return;
      // Another backend user: sign them in in place of this one, in one
      // transition, so the session never says signed-out in between.
      if (backendUser) {
        const from = session.getSnapshot().account!;
        let switched = true;
        await attempt(switchJazzSessionUser(session, from).then((done) => (switched = done)));
        // The session moved on while an operation finished: ask again.
        return switched ? undefined : busy();
      }
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
          signedOutLink = undefined;
          return;
        } catch (cause) {
          if (linking === link) linking = undefined;
          if (stopped) return;
          const who = `${signedIn.issuer}\0${signedIn.subject}`;
          if (signedOutMeanwhile(cause) && signedOutLink !== who) {
            signedOutLink = who;
            // Signed out while linking: follow the backend's new answer.
            if (asked) return "again";
            check();
            return;
          }
          if (!linkRefused(cause)) {
            // A network error, a 5xx, an expired challenge, or a backend
            // whose answers keep disagreeing: a later check tries again.
            if (asked) throw cause;
            retryLater(cause);
            return;
          }
          signedOutLink = undefined;
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
    signedOutLink = undefined;
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
    // The app moved on meanwhile (signed out itself, continued as a guest,
    // signed someone in): that account is not the one this answer was about.
    if (session.getSnapshot().account !== startedWith) return busy();
    if ((await askSignedOut()) === "keep" || quiet(asked)) return;
    if (session.getSnapshot().account !== startedWith) return busy();
    return attempt(session.logout());
  }
  /** The app's `onSignedOut` decision; a hook that fails signs out. */
  async function askSignedOut(): Promise<SignedOutDecision> {
    if (!options.onSignedOut) return "sign-out";
    try {
      return (await options.onSignedOut()) === "keep" ? "keep" : "sign-out";
    } catch (cause) {
      console.error("Jazz: onSignedOut threw, so the session signs out.", cause);
      return "sign-out";
    }
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
    if (stopped) return settle(waiters.splice(0), stopReason);
    if (checking) {
      again = {
        plain: (!refused && !asked) || !!again?.plain,
        refused: refused || !!again?.refused,
        asked: asked || !!again?.asked,
      };
      return;
    }
    const batch = asked ? waiters.splice(0) : [];
    const retriesBefore = retries;
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
        // A check that followed the backend ends any retry streak.
        if (!refused && !retry && !failure && retries === retriesBefore) clearRetry();
        const next = again;
        again = undefined;
        if (retry) waiters.unshift(...batch);
        else settle(batch, failure);
        // A refused check only reconnects, and one that asks again hasn't
        // followed an answer yet: neither tells who is signed in.
        if (!refused && !retry) markStarted();
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
    started,
    reauthenticate() {
      // Never answer as if the session followed the backend when nothing asks it.
      if (stopped) return Promise.reject(stopReason);
      const done = new Promise<void>((resolve, reject) => waiters.push({ resolve, reject }));
      whenSettled(() => check(false, true));
      return done;
    },
    stop() {
      if (stopped) return;
      stopped = true;
      stopReason ??= new Error("Jazz stopped following your backend: the session is closed");
      // Reauthenticates still waiting end with the session: nothing more to follow.
      settle(waiters.splice(0));
      clearTimeout(start);
      clearNotFound();
      clearRetry();
      markStarted();
      resetRefusal();
      unsubscribe();
      stopFocus();
      stopCrossTab?.();
    },
  };
  return watch;
}

/** Claims compared by value, whatever order their keys came in. */
function sameClaims(a: Readonly<Record<string, unknown>>, b: Readonly<Record<string, unknown>>) {
  const canonical = (value: unknown): unknown =>
    Array.isArray(value)
      ? value.map(canonical)
      : value && typeof value === "object"
        ? Object.fromEntries(
            Object.keys(value)
              .sort()
              .map((key) => [key, canonical((value as Record<string, unknown>)[key])]),
          )
        : value;
  return JSON.stringify(canonical(a)) === JSON.stringify(canonical(b));
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
    singleFlight: browserSingleFlight(`${channelName}:probe`),
  };
}

/**
 * {@link BackendWatchEvents.singleFlight} for the windows of one browser: a
 * Web Lock takes turns, and the answer is left in `localStorage` before the
 * lock is let go, so the window that takes it next reads it at once. Where
 * either is missing, every probe runs on its own.
 */
export function browserSingleFlight(
  name: string,
): (<T>(probe: () => Promise<T>) => Promise<T>) | undefined {
  const locks = globalThis.navigator?.locks;
  const storage = (() => {
    try {
      return globalThis.localStorage ?? undefined;
    } catch {
      return undefined;
    }
  })();
  if (!locks || !storage) return undefined;
  const read = (): { startedAt: number; value: unknown } | undefined => {
    try {
      const stored = storage.getItem(name);
      return stored ? (JSON.parse(stored) as { startedAt: number; value: unknown }) : undefined;
    } catch {
      return undefined;
    }
  };
  return <T>(probe: () => Promise<T>) => {
    const askedAt = Date.now();
    return locks.request(name, async () => {
      // Another window's probe started after this one asked: its answer is
      // as fresh as this window's own would be.
      const latest = read();
      if (latest && latest.startedAt >= askedAt) return latest.value as T;
      const startedAt = Date.now();
      const value = await probe();
      try {
        storage.setItem(name, JSON.stringify({ startedAt, value }));
      } catch {
        // Storage full or off: this window's answer is its own.
      }
      return value;
    }) as Promise<T>;
  };
}

/** Whether a failed check may pass if asked again: the network, a timeout, or a 5xx. */
function passes(cause: unknown): boolean {
  if (cause instanceof TypeError) return true;
  if (!(cause instanceof AccountAuthError)) return false;
  if (cause.status === undefined) return cause.code === "account_request_timeout";
  return cause.status >= 500 || cause.status === 408 || cause.status === 429;
}
