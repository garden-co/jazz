import type { AuthMode, Session } from "./context.js";
import { resolveClientSessionStateSync, type ClientSessionInput } from "./client-session.js";
import type { AuthFailureReason } from "./sync-transport.js";

export type { AuthFailureReason } from "./sync-transport.js";

export function mapAuthReason(reason: string): AuthFailureReason {
  const lower = reason.toLowerCase();
  if (lower.includes("expired")) return "expired";
  if (lower.includes("missing")) return "missing";
  if (lower.includes("disabled")) return "disabled";
  return "invalid";
}

export interface AuthState {
  authMode: AuthMode;
  session: Session | null;
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
  if (as.user_id !== bs.user_id || as.authMode !== bs.authMode) return false;
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
  let state = options?.initialState ?? deriveInitialState(input);
  const listeners = new Set<AuthStateListener>();

  const emit = () => {
    for (const listener of listeners) {
      listener(state);
    }
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
      const nextState: AuthState = {
        authMode: initialAuthMode,
        session: state.session,
        error: reason,
      };
      if (authStateEquals(state, nextState)) return state;
      state = nextState;
      emit();
      return state;
    },

    applyJwtToken(jwtToken?: string): AuthState {
      if (options?.lockAuthenticatedState) {
        return state;
      }

      const resolved = resolveClientSessionStateSync({
        appId: input.appId,
        jwtToken,
        cookieSession: input.cookieSession,
      });

      const currentUserId = state.session?.user_id ?? null;
      const nextUserId = resolved.session?.user_id ?? null;
      if (currentUserId !== nextUserId) {
        throw new Error(
          "Changing auth principal on a live client is not supported. Recreate the Db.",
        );
      }

      const nextState: AuthState = {
        authMode: initialAuthMode,
        session: resolved.session,
      };
      if (authStateEquals(state, nextState)) return state;
      state = nextState;
      emit();
      return state;
    },

    applyCookieSession(cookieSession?: Session): AuthState {
      if (options?.lockAuthenticatedState) {
        return state;
      }

      const resolved = resolveClientSessionStateSync({
        appId: input.appId,
        jwtToken: input.jwtToken,
        cookieSession,
      });

      const currentUserId = state.session?.user_id ?? null;
      const nextUserId = resolved.session?.user_id ?? null;
      if (currentUserId !== nextUserId) {
        throw new Error(
          "Changing auth principal on a live client is not supported. Recreate the Db.",
        );
      }

      const nextState: AuthState = {
        authMode: initialAuthMode,
        session: resolved.session,
      };
      if (authStateEquals(state, nextState)) return state;
      state = nextState;
      emit();
      return state;
    },
  };
}
