import type { AccountIdentity } from "../accounts/state.js";
import { AccountAuthError } from "../accounts/registry-client.js";
import { hasAuthProviderConnection } from "./auth-provider.js";
import type { JazzSession } from "./state.js";

const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";
const SYSTEM_ISSUER = "urn:jazz:system";

export interface BackendWatchEvents {
  /** Call `check` when the page or app comes back to the foreground. */
  onFocus(check: () => void): () => void;
  /** Tell other tabs this one changed account; call `check` when another tab says so. */
  crossTab?: { post(): void; listen(check: () => void): () => void };
}

export interface BackendWatch {
  /** The sync server refused this client's sign-in: ask the backend whether it still holds. */
  authRefused(): void;
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
 * While a `user` prop or `setUser` drives the session, the watch stays quiet,
 * except that a sync server refusing the sign-in still asks the backend: it
 * reconnects if the same user is still signed in, and signs out if no one is. A probe that reports there is no
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
  } = {},
): BackendWatch {
  const confirmMs = options.confirmMs ?? 2000;
  let stopped = false;
  let checking: Promise<void> | undefined;
  let again: { refused: boolean } | undefined;
  let announced = session.getSnapshot().account;

  const settled = () => {
    const snapshot = session.getSnapshot();
    return snapshot.status === "ready" || snapshot.status === "signed-out";
  };
  const quiet = (refused: boolean) =>
    stopped || !settled() || (!refused && hasAuthProviderConnection(session));
  async function ask(): Promise<AccountIdentity | null | "unknown"> {
    try {
      return await probe();
    } catch (cause) {
      if (cause instanceof AccountAuthError && cause.status === 404) watch.stop();
      return "unknown";
    }
  }
  async function once(refused: boolean) {
    if (quiet(refused)) return;
    let signedIn = await ask();
    if (signedIn === "unknown" || quiet(refused)) return;
    const current = session.getSnapshot().account?.identity;
    if (current?.issuer === SYSTEM_ISSUER) return;
    const backendUser = current !== undefined && current.issuer !== LOCAL_FIRST_ISSUER;
    if (signedIn) {
      if (current?.issuer === signedIn.issuer && current.subject === signedIn.subject) {
        // Still signed in: the refusal came from expired cookies on the old socket.
        if (refused) options.reconnect?.();
        return;
      }
      // A provider's `user` decides who signs in; the watch only signs out.
      if (hasAuthProviderConnection(session)) return;
      if (backendUser) await session.logout().catch(() => {});
      if (quiet(refused)) return;
      await session.loginWithBackend().catch(() => {});
      return;
    }
    if (!backendUser) return;
    await new Promise((resolve) => setTimeout(resolve, confirmMs));
    if (quiet(refused)) return;
    signedIn = await ask();
    if (signedIn !== null || quiet(refused)) return;
    await session.logout().catch(() => {});
  }
  function check(refused = false) {
    if (stopped) return;
    if (checking) {
      again = { refused: refused || !!again?.refused };
      return;
    }
    checking = once(refused).finally(() => {
      checking = undefined;
      const next = again;
      again = undefined;
      if (next) check(next.refused);
    });
  }
  const unsubscribe = session.subscribe(() => {
    const snapshot = session.getSnapshot();
    if (snapshot.status !== "ready" && snapshot.status !== "signed-out") return;
    if (snapshot.account === announced) return;
    announced = snapshot.account;
    events.crossTab?.post();
  });
  const stopFocus = events.onFocus(() => check());
  const stopCrossTab = events.crossTab?.listen(() => check());
  // Let a provider that drives the session attach first.
  const start = setTimeout(() => check(), 0);
  const watch: BackendWatch = {
    authRefused: () => check(true),
    stop() {
      if (stopped) return;
      stopped = true;
      clearTimeout(start);
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
  };
}
