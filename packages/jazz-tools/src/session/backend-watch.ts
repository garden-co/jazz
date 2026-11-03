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

export interface BackendWatch {
  /** The sync server refused this client's sign-in: ask the backend whether it still holds. */
  authRefused(): void;
  /** The sync link is connected again (resets the reconnect backoff). */
  connected(): void;
  /**
   * The app's own auth changed (it signed in, out or as someone else): ask the
   * backend now, and follow its answer without waiting for a second one.
   * Resolves once the session follows that answer.
   */
  reauthenticate(): Promise<void>;
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
 * - A local-first guest stays a guest until someone signs in.
 *
 * The app calls `reauthenticate` after its own sign-in or sign-out, so the
 * session follows at once.
 *
 * A sync server refusing the sign-in (`authRefused`) asks the backend too, but
 * only to reconnect, with backoff, while it still names the same user. It
 * never signs anyone out: a background tab's cookie may just be stale, so a
 * sign-out waits for a check after focus returns (or for the provider). A probe that reports there is no
 * backend (404) stops the watch.
 */
export function watchBackendUser<Client>(
  session: JazzSession<Client>,
  probe: () => Promise<AccountIdentity | null>,
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
  let checking: Promise<void> | undefined;
  // Checks asked for while one runs: a plain one (may sign out), a refused
  // one (only reconnects) and an asked one (the app's auth changed: no
  // confirming second answer) are kept apart.
  let again: { plain: boolean; refused: boolean; asked: boolean } | undefined;
  // `reauthenticate` callers, settled once no check runs or waits.
  let waiters: Array<() => void> = [];
  let announced = session.getSnapshot().account;

  const settled = () => {
    const snapshot = session.getSnapshot();
    return snapshot.status === "ready" || snapshot.status === "signed-out";
  };
  // A failed sign-in leaves the session in error; the app asking again after
  // its auth changed is how it recovers.
  const askable = () => settled() || session.getSnapshot().status === "error";
  const quiet = (asked = false) => stopped || !(asked ? askable() : settled());
  async function ask(): Promise<AccountIdentity | null | "unknown"> {
    try {
      return await probe();
    } catch (cause) {
      if (cause instanceof AccountAuthError && cause.status === 404) watch.stop();
      return "unknown";
    }
  }
  async function once(refused: boolean, asked = false) {
    if (quiet(asked)) return;
    let signedIn = await ask();
    if (signedIn === "unknown" || quiet(asked)) return;
    const current = session.getSnapshot().account?.identity;
    if (current?.issuer === SYSTEM_ISSUER) return;
    const backendUser = current !== undefined && current.issuer !== LOCAL_FIRST_ISSUER;
    if (signedIn) {
      if (current?.issuer === signedIn.issuer && current.subject === signedIn.subject) {
        // Still signed in: the refusal came from expired cookies on the old socket.
        if (refused || refusedPending) scheduleReconnect();
        return;
      }
      if (refused) return;
      if (backendUser) await session.logout().catch(() => {});
      if (quiet(asked)) return;
      await session.loginWithBackend().catch(() => {});
      return;
    }
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
    await session.logout().catch(() => {});
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
  function settle() {
    const done = waiters;
    waiters = [];
    for (const resolve of done) resolve();
  }
  function check(refused = false, asked = false) {
    if (stopped) return settle();
    if (checking) {
      again = {
        plain: (!refused && !asked) || !!again?.plain,
        refused: refused || !!again?.refused,
        asked: asked || !!again?.asked,
      };
      return;
    }
    checking = once(refused, asked).finally(() => {
      checking = undefined;
      const next = again;
      again = undefined;
      // An asked check answers a plain one too.
      if (next?.asked) check(false, true);
      else if (next?.plain) check(false);
      if (next?.refused) check(true);
      if (!checking) settle();
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
    reauthenticate() {
      const done = new Promise<void>((resolve) => waiters.push(resolve));
      whenSettled(() => check(false, true));
      return done;
    },
    stop() {
      if (stopped) return;
      stopped = true;
      settle();
      clearTimeout(start);
      resetRefusal();
      unsubscribe();
      stopFocus();
      stopCrossTab?.();
    },
  };
  return watch;
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
