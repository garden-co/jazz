import { AccountAuthError, requestBackendUser } from "../accounts/registry-client.js";
import type { AccountHandle, AccountIdentity } from "../accounts/state.js";
import type { SyncStatus } from "../runtime/sync-status.js";
import {
  watchBackendUser,
  type BackendUser,
  type BackendWatchEvents,
  type SignedOutDecision,
} from "./backend-watch.js";
import type { SessionCore } from "./state.js";

/**
 * A Jazz session that follows the user your backend names. Jazz asks your
 * backend (its `authenticate`) who is signed in on start, on focus and when
 * another tab signs in or out.
 */
export interface UserFollowingSession<Client> extends SessionCore<Client> {
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
  /** See `linkDeviceAccountOnSignUp` on `createJazzClient`. Default true. */
  linkGuest?: boolean;
  /** See `forgotten` on `watchBackendUser`. */
  forgotten?: (account: AccountHandle) => Promise<boolean>;
  /**
   * Call `changed` when another tab changes this device's accounts (a
   * `storage` event), so a guest it forgot asks the backend at once.
   */
  watchAccounts?: (changed: () => void) => () => void;
  /** See `onSignedOut` on `createJazzClient`. */
  onSignedOut?: () => SignedOutDecision | void | Promise<SignedOutDecision | void>;
}) {
  return <Client>(session: SessionCore<Client>) => decorate(session, options);
}

// Each following session's first backend answer (see `firstBackendAnswer`).
const firstAnswers = new WeakMap<object, Promise<void>>();

/**
 * How long opening a session waits for the backend's first answer (see
 * `firstBackendAnswer`) before it opens signed out anyway. Much shorter than
 * the auth request deadline (30 s), so a cold-starting or hung backend costs
 * an app at most this long on start. The check keeps running, and the
 * session signs the user in when the answer arrives.
 */
export const FIRST_ANSWER_DEADLINE_MS = 5_000;

/**
 * @internal For hosts, before they hand out a session `followUser`
 * decorated. A session with no account on this device (no user remembered
 * here yet: the backend names them) opens signed out until the backend has
 * answered who is signed in, and an app would show its sign-in screen to a
 * user who is signed in. So when it opened signed out, wait for that first
 * answer, for at most `deadlineMs` (`FIRST_ANSWER_DEADLINE_MS`, 5 s). An
 * account already on this device (a guest, or a backend user it remembers)
 * opens at once (offline too). A failed check opens signed out as before, and
 * so does one slower than the deadline: the request keeps running, and the
 * session follows its answer (signs the user in) when it comes. Never rejects.
 */
export async function firstBackendAnswer(
  session: SessionCore<unknown>,
  deadlineMs = FIRST_ANSWER_DEADLINE_MS,
): Promise<void> {
  if (session.getSnapshot().status !== "signed-out") return;
  const answered = firstAnswers.get(session);
  if (!answered) return;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  await Promise.race([
    answered,
    new Promise<void>((resolve) => (deadline = setTimeout(resolve, deadlineMs))),
  ]);
  clearTimeout(deadline);
}

/**
 * @internal Settles once a `followUser` session's watch has followed the
 * backend's first answer (or failed to ask), whatever the session opened as.
 */
export function backendAnswered(session: SessionCore<unknown>): Promise<void> {
  return firstAnswers.get(session) ?? Promise.resolve();
}

function decorate<Client>(
  session: SessionCore<Client>,
  options: {
    registry: string;
    events: BackendWatchEvents;
    fetch?: typeof fetch;
    linkGuest?: boolean;
    forgotten?: (account: AccountHandle) => Promise<boolean>;
    watchAccounts?: (changed: () => void) => () => void;
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
      forgotten: options.forgotten,
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
  // Another tab forgot this guest (it linked it, and may have signed that
  // user out since): ask the backend now rather than on the next focus.
  const { forgotten } = options;
  const stopAccounts = forgotten
    ? options.watchAccounts?.(() => {
        const account = session.getSnapshot().account;
        if (account?.identity.issuer !== "urn:jazz:local-first") return;
        void forgotten(account)
          .then((gone) => {
            if (gone && session.getSnapshot().account === account) return watch.reauthenticate();
          })
          .catch(() => {});
      })
    : undefined;
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
      stopAccounts?.();
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
    // No one signed in: a backend answers 200 with no `identity` (alpha.62
    // on: `{ user: null }`), or 401 (alpha.61 and earlier, caught below).
    const answer = (await requestBackendUser(registry, fetcher)) as {
      identity?: AccountIdentity;
      account?: string | null;
      claims?: unknown;
    } | null;
    if (!answer?.identity) return null;
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
