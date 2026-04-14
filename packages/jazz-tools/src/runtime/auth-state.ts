import type { Session } from "./context.js";
import { resolveClientSessionStateSync, type ClientSessionInput } from "./client-session.js";
import type { AuthFailureReason } from "./sync-transport.js";

export type { AuthFailureReason } from "./sync-transport.js";

export type AuthState =
  | {
      status: "authenticated";
      transport: "bearer" | "backend";
      session: Session | null;
    }
  | {
      status: "unauthenticated";
      reason: AuthFailureReason;
      session: Session | null;
    };

type AuthStateListener = (state: AuthState) => void;

function authStateEquals(a: AuthState, b: AuthState): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function deriveAuthenticatedState(input: ClientSessionInput): AuthState {
  const resolved = resolveClientSessionStateSync(input);
  if (resolved.transport) {
    return {
      status: "authenticated",
      transport: resolved.transport,
      session: resolved.session,
    };
  }

  return {
    status: "unauthenticated",
    reason: "missing",
    session: null,
  };
}

function authUserId(state: AuthState): string | null {
  return state.session?.user_id ?? null;
}

export function createAuthStateStore(input: ClientSessionInput) {
  let state = deriveAuthenticatedState(input);
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
        status: "unauthenticated",
        reason,
        session: state.session,
      };

      if (authStateEquals(state, nextState)) {
        return state;
      }

      state = nextState;
      emit();
      return state;
    },

    applyJwtToken(jwtToken?: string): AuthState {
      const nextState = deriveAuthenticatedState({
        appId: input.appId,
        jwtToken,
      });

      const currentUserId = authUserId(state);
      const nextUserId = authUserId(nextState);

      if (currentUserId !== nextUserId) {
        throw new Error(
          "Changing auth principal on a live client is not supported. Recreate the Db.",
        );
      }

      if (authStateEquals(state, nextState)) {
        return state;
      }

      state = nextState;
      emit();
      return state;
    },
  };
}
