import { AccountAuthError, requestBackendSignIn } from "../accounts/registry-client.js";
import type { AccountIdentity } from "../accounts/state.js";
import type { SyncStatus } from "../runtime/sync-status.js";
import { watchBackendUser, type BackendWatchEvents } from "./backend-watch.js";
import type { JazzSession } from "./state.js";

/**
 * A Jazz session that follows the user your backend names. Jazz asks your
 * backend (its `authenticate`) who is signed in on start, on focus and when
 * another tab signs in or out.
 */
export interface UserFollowingSession<Client> extends JazzSession<Client> {
  /**
   * Your auth changed: call this after your own sign-in, sign-out or account
   * switch, once your session cookie (or bearer) is fresh. Jazz asks your
   * backend who is signed in now and follows its answer: another user signs
   * in, and no one signs the current user out. Resolves once the session
   * follows that answer, and rejects when your backend can't be asked (a
   * network error or a 5xx) or following it fails. A session left in error
   * for the same user (its client failed to open) retries. The user's
   * claims are as of their sign-in; reauthenticating as the same user
   * doesn't update them.
   */
  reauthenticate(): Promise<void>;
}

type ObservedClient = {
  db?: {
    onSyncStatusChange?(listener: (status: SyncStatus) => void): () => void;
    reconnect?(): Promise<void>;
  };
};

/**
 * A `decorate` for `createJazzSessionOwner`: keep the session in step with
 * the backend at `registry` (see `watchBackendUser`), and give it
 * `reauthenticate`.
 */
export function followUser(options: {
  registry: string;
  events: BackendWatchEvents;
  fetch?: typeof fetch;
}) {
  return <Client>(session: JazzSession<Client>) => decorate(session, options);
}

function decorate<Client>(
  session: JazzSession<Client>,
  options: {
    registry: string;
    events: BackendWatchEvents;
    fetch?: typeof fetch;
  },
): Pick<UserFollowingSession<Client>, "reauthenticate" | "close"> {
  const watch = watchBackendUser(
    session,
    () => signedInIdentity(options.registry, options.fetch),
    options.events,
    {
      reconnect: () =>
        void (session.getSnapshot().client as ObservedClient | undefined)?.db
          ?.reconnect?.()
          .catch(() => {}),
    },
  );
  // A sync server that refuses the sign-in (the backend's re-check ended it,
  // and reconnecting with fresh cookies failed) asks the backend again.
  let observed: unknown;
  let stopStatus: (() => void) | undefined;
  const followClient = () => {
    const client = session.getSnapshot().client;
    if (client === observed) return;
    observed = client;
    stopStatus?.();
    stopStatus = (client as ObservedClient | undefined)?.db?.onSyncStatusChange?.((status) => {
      if (status.link.state === "offline" && status.link.reason === "auth") watch.authRefused();
      else if (status.link.state === "connected") watch.connected();
    });
  };
  const unsubscribe = session.subscribe(followClient);
  followClient();
  const close = session.close;
  return {
    reauthenticate: () => watch.reauthenticate(),
    close() {
      watch.stop();
      unsubscribe();
      stopStatus?.();
      return close();
    },
  };
}

async function signedInIdentity(
  registry: string,
  fetcher?: typeof fetch,
): Promise<AccountIdentity | null> {
  try {
    const answer = (await requestBackendSignIn(registry, fetcher)) as {
      identity?: AccountIdentity;
    };
    return answer.identity ?? null;
  } catch (cause) {
    if (cause instanceof AccountAuthError && cause.status === 401) return null;
    throw cause;
  }
}
