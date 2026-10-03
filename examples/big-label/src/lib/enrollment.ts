import type { JazzLifecycle } from "./jazz-lifecycle";

/** The part of `sessionStorage` the sign-up intent needs. */
export type IntentStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">;

const signupMarker = "big-label-register-external-account";
type SignupIntent = { email: string; identityId?: string };

/** Marks the next Better Auth session for this email as a new Jazz account. */
export function beginSignupIntent(storage: IntentStorage, email: string) {
  storage.setItem(signupMarker, JSON.stringify({ email } satisfies SignupIntent));
}

export function clearSignupIntent(storage: IntentStorage) {
  storage.removeItem(signupMarker);
}

/** Whether this signed-in identity should register rather than log in. */
export function claimsSignupIntent(storage: IntentStorage, email: string, identityId: string) {
  const encoded = storage.getItem(signupMarker);
  if (!encoded) return false;
  try {
    const intent = JSON.parse(encoded) as SignupIntent;
    if (intent.email !== email) return false;
    if (intent.identityId && intent.identityId !== identityId) return false;
    if (!intent.identityId)
      storage.setItem(signupMarker, JSON.stringify({ ...intent, identityId }));
    return true;
  } catch {
    clearSignupIntent(storage);
    return false;
  }
}

/**
 * One attempt to enroll the signed-in identity with Jazz. Resolves true when
 * the attempt finished and is still current, as soon as the account's client
 * is open; the personal label bootstrap then runs in the background.
 *
 * A Jazz account this browser kept from an earlier sign-in of the same
 * identity opens its local data at once. It has no credential until it logs
 * in again, so it cannot sync before that login; logging in as the same
 * identity hands the credential to the client already open instead of
 * closing and reopening it.
 *
 * `isCurrent` must belong to this attempt alone. React strict mode (and Retry)
 * abandon an attempt and start the next one straight away; a flag shared
 * between attempts reads "current" again for the abandoned one, which would
 * then register the identity as well and fail the live attempt with
 * `identity_already_assigned`.
 */
export async function enrollAndBootstrap(options: {
  lifecycle: JazzLifecycle;
  storage: IntentStorage;
  email: string;
  identityId: string;
  getToken: () => Promise<string>;
  /** Whether the server still has to bootstrap this account (default: yes). */
  needsBootstrap?: (accountId: string) => boolean;
  bootstrap: (token: string, accountId: string) => Promise<void>;
  /**
   * Called before this attempt resolves when a background bootstrap starts,
   * with the account it is for, so a failed attempt (token or bootstrap) can
   * be retried.
   */
  onBootstrapStart?: (accountId: string) => void;
  onBootstrapError?: (error: unknown) => void;
  isCurrent: () => boolean;
}): Promise<boolean> {
  const { lifecycle, storage, email, identityId, getToken, isCurrent } = options;
  const registering = claimsSignupIntent(storage, email, identityId);
  let registered = false;
  const retained = lifecycle.selectedAccount();
  if (!registering && retained?.identity.subject === identityId) {
    await lifecycle.attach(isCurrent);
    await lifecycle.revalidate((manager) => manager.loginJWT({ getToken }), isCurrent);
  } else {
    await lifecycle.transition(async (manager) => {
      if (!registering) return manager.loginJWT({ getToken });
      await manager.registerJWT({ getToken });
      registered = true;
    }, isCurrent);
  }
  // A registration consumes the intent even when its attempt was abandoned
  // meanwhile: the identity now has an account, so every later attempt logs in.
  if (registered) clearSignupIntent(storage);
  if (!isCurrent()) return false;
  const account = lifecycle.selectedAccount();
  if (account && (options.needsBootstrap?.(account.id) ?? true)) {
    options.onBootstrapStart?.(account.id);
    void getToken()
      .then((token) => options.bootstrap(token, account.id))
      .catch((error: unknown) => options.onBootstrapError?.(error));
  }
  return isCurrent();
}
