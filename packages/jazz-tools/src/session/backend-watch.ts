import type { AccountIdentity } from "../accounts/state.js";
import { AccountAuthError } from "../accounts/registry-client.js";
import { hasAuthProviderConnection } from "./auth-provider.js";
import type { JazzSession } from "./state.js";

const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";
const SYSTEM_ISSUER = "urn:jazz:system";

export interface BackendWatchEvents {
  /** Call `check` when the page regains focus or becomes visible. */
  onFocus(check: () => void): () => void;
  /** Tell other tabs this one changed account; call `check` when another tab says so. */
  crossTab?: { post(): void; listen(check: () => void): () => void };
}

/**
 * Keep a session without a `user` prop in step with the app's backend: ask it
 * who is signed in on start, when the page regains focus and when another tab
 * signs in or out. A different user signs in; a signed-out answer signs out a
 * backend user. A local-first guest stays a guest until someone signs in. A
 * `user` prop or `setUser` takes over and the watch stays quiet.
 *
 * `probe` resolves to the signed-in identity, or null when signed out. A probe
 * that reports there is no backend (404) stops the watch.
 */
export function watchBackendUser<Client>(
  session: JazzSession<Client>,
  probe: () => Promise<AccountIdentity | null>,
  events: BackendWatchEvents,
): () => void {
  let stopped = false;
  let checking: Promise<void> | undefined;
  let again = false;
  let announced = session.getSnapshot().account;

  const settled = () => {
    const snapshot = session.getSnapshot();
    return snapshot.status === "ready" || snapshot.status === "signed-out";
  };
  async function once() {
    if (stopped || hasAuthProviderConnection(session) || !settled()) return;
    let signedIn: AccountIdentity | null;
    try {
      signedIn = await probe();
    } catch (cause) {
      if (cause instanceof AccountAuthError && cause.status === 404) stop();
      return;
    }
    if (stopped || hasAuthProviderConnection(session) || !settled()) return;
    const current = session.getSnapshot().account?.identity;
    if (current?.issuer === SYSTEM_ISSUER) return;
    if (signedIn) {
      if (current?.issuer === signedIn.issuer && current.subject === signedIn.subject) return;
      await session.loginWithBackend().catch(() => {});
    } else if (current && current.issuer !== LOCAL_FIRST_ISSUER) {
      await session.logout().catch(() => {});
    }
  }
  function check() {
    if (stopped) return;
    if (checking) {
      again = true;
      return;
    }
    checking = once().finally(() => {
      checking = undefined;
      if (again) {
        again = false;
        check();
      }
    });
  }
  const unsubscribe = session.subscribe(() => {
    const snapshot = session.getSnapshot();
    if (snapshot.status !== "ready" && snapshot.status !== "signed-out") return;
    if (snapshot.account === announced) return;
    announced = snapshot.account;
    events.crossTab?.post();
  });
  const stopFocus = events.onFocus(check);
  const stopCrossTab = events.crossTab?.listen(check);
  function stop() {
    if (stopped) return;
    stopped = true;
    unsubscribe();
    stopFocus();
    stopCrossTab?.();
  }
  check();
  return stop;
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
