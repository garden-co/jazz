import { AccountAuthError, requestBackendUser } from "../accounts/registry-client.js";
import type { AccountIdentity } from "../accounts/state.js";
import type { SyncStatus } from "../runtime/sync-status.js";
import {
  watchBackendUser,
  type BackendUser,
  type BackendWatchEvents,
  type SignedOutDecision,
} from "./backend-watch.js";
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
  /** See `linkGuestOnSignUp` on `createJazzClient`. Default true. */
  linkGuest?: boolean;
  /** See `onSignedOut` on `createJazzClient`. */
  onSignedOut?: () => SignedOutDecision | void | Promise<SignedOutDecision | void>;
}) {
  return <Client>(session: JazzSession<Client>) => decorate(session, options);
}

// Each following session's first backend answer (see `firstBackendAnswer`).
const firstAnswers = new WeakMap<object, Promise<void>>();

/**
 * @internal For hosts, before they hand out a session `followUser`
 * decorated. A session with no account on this device (no user remembered
 * here yet: the backend names them) opens signed out until the backend has
 * answered who is signed in, and an app would show its sign-in screen to a
 * user who is signed in. So when it opened signed out, wait for that first
 * answer. An account already on this device (a guest, or a backend user it
 * remembers) opens at once (offline too), and
 * a failed or timed-out check (the auth request deadline) opens signed out as
 * before. Never rejects.
 */
export async function firstBackendAnswer(session: JazzSession<unknown>): Promise<void> {
  if (session.getSnapshot().status !== "signed-out") return;
  await firstAnswers.get(session);
}

/**
 * @internal Settles once a `followUser` session's watch has followed the
 * backend's first answer (or failed to ask), whatever the session opened as.
 */
export function backendAnswered(session: JazzSession<unknown>): Promise<void> {
  return firstAnswers.get(session) ?? Promise.resolve();
}

function decorate<Client>(
  session: JazzSession<Client>,
  options: {
    registry: string;
    events: BackendWatchEvents;
    fetch?: typeof fetch;
    linkGuest?: boolean;
    onSignedOut?: () => SignedOutDecision | void | Promise<SignedOutDecision | void>;
  },
): Pick<UserFollowingSession<Client>, "reauthenticate" | "linkWithBackend" | "close"> {
  const probe = () => signedInIdentity(options.registry, options.fetch);
  const singleFlight = options.events.singleFlight;
  const watch = watchBackendUser(
    session,
    singleFlight ? () => singleFlight(probe) : probe,
    options.events,
    {
      linkGuest: options.linkGuest,
      onSignedOut: options.onSignedOut,
      reconnect: () =>
        void (session.getSnapshot().client as ObservedClient | undefined)?.db
          ?.reconnect?.()
          .catch(() => {}),
    },
  );
  firstAnswers.set(session, watch.started);
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

/** @internal Who the backend at `registry` says is signed in (null: no one). */
export async function signedInIdentity(
  registry: string,
  fetcher?: typeof fetch,
): Promise<BackendUser | null> {
  try {
    const answer = (await requestBackendUser(registry, fetcher)) as {
      identity?: AccountIdentity;
      account?: string | null;
      claims?: unknown;
    };
    if (!answer.identity) return null;
    const { claims } = answer;
    return {
      ...answer.identity,
      hasAccount: answer.account != null,
      ...(claims && typeof claims === "object" && !Array.isArray(claims)
        ? { claims: claims as Record<string, unknown> }
        : {}),
    };
  } catch (cause) {
    if (cause instanceof AccountAuthError && cause.status === 401) return null;
    throw cause;
  }
}
