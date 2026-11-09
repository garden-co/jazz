import type { AuthMode, PublicSession, Session } from "./context.js";
import { resolveClientSessionStateSync, type ClientSessionInput } from "./client-session.js";

/**
 * Why the server refused a session's credential.
 *
 * - `expired`, `missing`, `invalid`: a new credential can fix it. An account
 *   context renews its own token on these.
 * - `disabled`: the app has that auth mode turned off.
 * - `revoked`: the account identity was revoked or reassigned.
 *
 * A new token cannot fix `disabled` or `revoked`.
 */
export type AuthFailureReason = "expired" | "missing" | "invalid" | "disabled" | "revoked";

/** Whether a new token can fix the refusal, so an app renews it. */
export function isRenewableAuthFailure(reason: AuthFailureReason): boolean {
  return reason !== "disabled" && reason !== "revoked";
}

export function mapAuthReason(reason: string): AuthFailureReason {
  const lower = reason.toLowerCase();
  if (lower.includes("expired")) return "expired";
  if (lower.includes("missing")) return "missing";
  if (lower.includes("disabled")) return "disabled";
  if (lower.includes("revoked")) return "revoked";
  return "invalid";
}

export interface AuthState {
  authMode: AuthMode;
  session: PublicSession | null;
  error?: AuthFailureReason;
}

type AuthStateListener = (state: AuthState) => void;

export interface AuthStateStoreOptions {
  initialState?: AuthState;
  lockAuthenticatedState?: boolean;
}

function authStateEquals(a: AuthState, b: AuthState): boolean {
  if (a.authMode !== b.authMode || a.error !== b.error) return false;
  const as = a.session;
  const bs = b.session;
  if (as === bs) return true;
  if (!as || !bs) return false;
  if (JSON.stringify(as.user) !== JSON.stringify(bs.user) || as.authMode !== bs.authMode) {
    return false;
  }
  return JSON.stringify(as.claims) === JSON.stringify(bs.claims);
}

function deriveAuthMode(input: ClientSessionInput): AuthMode {
  const resolved = resolveClientSessionStateSync(input);
  return resolved.session?.authMode ?? "external";
}

function deriveInitialState(input: ClientSessionInput): AuthState {
  const resolved = resolveClientSessionStateSync(input);
  return {
    authMode: resolved.session?.authMode ?? "external",
    session: resolved.session,
  };
}

export function createAuthStateStore(input: ClientSessionInput, options?: AuthStateStoreOptions) {
  const initialAuthMode = deriveAuthMode(input);
  let currentInput: ClientSessionInput = { ...input };
  let state = options?.initialState ?? deriveInitialState(input);
  const listeners = new Set<AuthStateListener>();

  const emit = () => {
    let failed = false;
    let firstError: unknown;
    for (const listener of listeners) {
      try {
        listener(state);
      } catch (error) {
        if (!failed) {
          failed = true;
          firstError = error;
        }
      }
    }
    // The state is already committed: finish publication before reporting an
    // observer failure, since an identical update will not notify again.
    if (failed) throw firstError;
  };

  const assertSamePrincipal = (nextSession: PublicSession | null): void => {
    if (JSON.stringify(state.session?.user ?? null) !== JSON.stringify(nextSession?.user ?? null)) {
      throw new Error(
        "Changing auth principal on a live client is not supported. Recreate the Db.",
      );
    }
  };

  const setState = (nextState: AuthState): AuthState => {
    if (authStateEquals(state, nextState)) return state;
    state = nextState;
    emit();
    return state;
  };

  const jwtInput = (jwtToken?: string, trustedReservedSession?: Session): ClientSessionInput => ({
    ...currentInput,
    jwtToken,
    cookieSession: undefined,
    trustedReservedSession,
  });

  const cookieInput = (cookieSession?: Session): ClientSessionInput => ({
    ...currentInput,
    jwtToken: undefined,
    cookieSession,
    trustedReservedSession: undefined,
  });

  /** Resolve `nextInput` and check it keeps the principal, without changing state. */
  const validateInput = (nextInput: ClientSessionInput): boolean => {
    if (options?.lockAuthenticatedState) return false;
    assertSamePrincipal(resolveClientSessionStateSync(nextInput).session);
    return true;
  };

  const applyInput = (nextInput: ClientSessionInput): AuthState => {
    if (options?.lockAuthenticatedState) {
      return state;
    }

    const resolved = resolveClientSessionStateSync(nextInput);
    assertSamePrincipal(resolved.session);
    currentInput = nextInput;
    return setState({ authMode: initialAuthMode, session: resolved.session });
  };

  return {
    getState(): AuthState {
      return state;
    },

    onChange(listener: AuthStateListener): () => void {
      listeners.add(listener);
      listener(state);
      return () => {
        listeners.delete(listener);
      };
    },

    markUnauthenticated(reason: AuthFailureReason): AuthState {
      return setState({ authMode: initialAuthMode, session: state.session, error: reason });
    },

    clearError(): AuthState {
      if (state.error === undefined) return state;
      state = { authMode: state.authMode, session: state.session };
      emit();
      return state;
    },

    /** Validate a prospective token update without changing state or notifying listeners. */
    validateJwtToken(jwtToken?: string, trustedReservedSession?: Session): boolean {
      return validateInput(jwtInput(jwtToken, trustedReservedSession));
    },

    /** Validate a prospective cookie update without changing state or notifying listeners. */
    validateCookieSession(cookieSession?: Session): boolean {
      return validateInput(cookieInput(cookieSession));
    },

    applyJwtToken(jwtToken?: string, trustedReservedSession?: Session): AuthState {
      return applyInput(jwtInput(jwtToken, trustedReservedSession));
    },

    applyCookieSession(cookieSession?: Session): AuthState {
      return applyInput(cookieInput(cookieSession));
    },
  };
}
