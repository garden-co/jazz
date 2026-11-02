import { AccountAuthError, requestBackendSignIn } from "../accounts/registry-client.js";
import type { AccountIdentity } from "../accounts/state.js";
import type { SyncStatus } from "../runtime/sync-status.js";
import { connectAuthProvider, type AuthProviderConnection } from "./auth-provider.js";
import { watchBackendUser, type BackendWatchEvents } from "./backend-watch.js";
import type { JazzSession } from "./state.js";

/** A Jazz session that follows your auth's signed-in user. */
export interface UserFollowingSession<Client> extends JazzSession<Client> {
  /**
   * Tell Jazz who your auth says is signed in: `undefined` while it loads,
   * `null` when signed out, and the user's stable id when signed in. Each
   * change signs in again through your backend, whose `authenticate` decides
   * who the user is; `null` closes the account's client. Without it, Jazz
   * asks your backend who is signed in on start, on focus and when another
   * tab signs in or out.
   */
  setUser(user: string | null | undefined): void;
}

type ObservedClient = {
  db?: {
    onSyncStatusChange?(listener: (status: SyncStatus) => void): () => void;
    reconnect?(): Promise<void>;
  };
};

/**
 * A `decorate` for `createJazzSessionOwner`: give the session `setUser`, and
 * without it keep it in step with the backend at `registry` (see
 * `watchBackendUser`).
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
  options: { registry: string; events: BackendWatchEvents; fetch?: typeof fetch },
): Pick<UserFollowingSession<Client>, "setUser" | "close"> {
  let connection: AuthProviderConnection | undefined;
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
    });
  };
  const unsubscribe = session.subscribe(followClient);
  followClient();
  const close = session.close;
  return {
    setUser(user: string | null | undefined) {
      connection ??= connectAuthProvider(session);
      connection.update({ key: user ?? null, isPending: user === undefined });
    },
    close() {
      watch.stop();
      unsubscribe();
      stopStatus?.();
      connection?.dispose();
      connection = undefined;
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
