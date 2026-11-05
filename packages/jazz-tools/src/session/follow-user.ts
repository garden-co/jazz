import { AccountAuthError, requestBackendUser } from "../accounts/registry-client.js";
import type { AccountIdentity } from "../accounts/state.js";
import type { SyncStatus } from "../runtime/sync-status.js";
import { watchBackendUser, type BackendUser, type BackendWatchEvents } from "./backend-watch.js";
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

/**
 * How long opening a session waits for the backend's first answer before it
 * opens signed out anyway. Shorter than the request deadline, so a slow
 * backend costs an app at most this long on start; the check keeps running
 * and the session follows its answer when it comes.
 */
export const FIRST_ANSWER_DEADLINE_MS = 5_000;

/**
 * Opening a session with no account on this device (a returning backend user
 * keeps none): wait for the backend's first answer (`started`, from
 * `followUser`'s `onStart`), so apps don't show their sign-in screen to a
 * user who is signed in. An account already on this device opens at once
 * (offline too), and a failed check, or one slower than `deadlineMs`, opens
 * signed out as before. Never rejects.
 */
export async function untilFirstAnswer<Client>(
  session: JazzSession<Client>,
  started: Promise<void> | undefined,
  deadlineMs = FIRST_ANSWER_DEADLINE_MS,
): Promise<void> {
  if (!started || session.getSnapshot().status !== "signed-out") return;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  await Promise.race([
    started,
    new Promise<void>((resolve) => (deadline = setTimeout(resolve, deadlineMs))),
  ]);
  clearTimeout(deadline);
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
  /** See `linkGuestOnSignUp` on `createJazzClient`. Default true. */
  linkGuest?: boolean;
  /** Receives a promise that settles once the backend's first answer is followed. */
  onStart?(started: Promise<void>): void;
}) {
  return <Client>(session: JazzSession<Client>) => decorate(session, options);
}

function decorate<Client>(
  session: JazzSession<Client>,
  options: {
    registry: string;
    events: BackendWatchEvents;
    fetch?: typeof fetch;
    linkGuest?: boolean;
    onStart?(started: Promise<void>): void;
  },
): Pick<UserFollowingSession<Client>, "reauthenticate" | "linkWithBackend" | "close"> {
  const watch = watchBackendUser(
    session,
    () => signedInIdentity(options.registry, options.fetch),
    options.events,
    {
      linkGuest: options.linkGuest,
      reconnect: () =>
        void (session.getSnapshot().client as ObservedClient | undefined)?.db
          ?.reconnect?.()
          .catch(() => {}),
    },
  );
  options.onStart?.(watch.started);
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
  const link = session.linkWithBackend;
  return {
    reauthenticate: () => watch.reauthenticate(),
    // The watch links a guest to a user who just signed up as soon as it
    // sees them (see `watchBackendUser`). An app that also calls this after
    // its sign-up joins that link instead of failing on it.
    async linkWithBackend() {
      const joined = watch.linking;
      if (joined && joined.result === undefined) return joined.done;
      if (joined && session.getSnapshot().account === joined.result) return;
      return link();
    },
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
): Promise<BackendUser | null> {
  try {
    const answer = (await requestBackendUser(registry, fetcher)) as {
      identity?: AccountIdentity;
      account?: string | null;
    };
    if (!answer.identity) return null;
    return { ...answer.identity, hasAccount: answer.account != null };
  } catch (cause) {
    if (cause instanceof AccountAuthError && cause.status === 401) return null;
    throw cause;
  }
}
