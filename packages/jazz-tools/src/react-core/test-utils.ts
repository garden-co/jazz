import type { AuthState } from "../runtime/auth-state.js";
import type { AuthMode, Session } from "../runtime/context.js";

export function makeFakeClient(params: {
  authMode: AuthMode;
  userId: string;
  claims: Record<string, unknown>;
}) {
  const session: Session = {
    user_id: params.userId,
    claims: params.claims,
    authMode: params.authMode,
  };
  let state: AuthState = { authMode: params.authMode, session };
  const listeners = new Set<(s: AuthState) => void>();
  const updateAuthTokenSpy = { lastToken: null as string | null };
  return {
    db: {
      getAuthState: () => state,
      onAuthChanged: (cb: (s: AuthState) => void) => {
        listeners.add(cb);
        return () => listeners.delete(cb);
      },
      updateAuthToken: (token: string) => {
        updateAuthTokenSpy.lastToken = token;
        state = { authMode: state.authMode, session: state.session };
        for (const l of listeners) l(state);
      },
    },
    manager: {} as any,
    shutdown: async () => {},
    __updateAuthTokenSpy: updateAuthTokenSpy,
    __markUnauthenticated(reason: "expired" | "invalid" | "missing" | "disabled") {
      state = { ...state, error: reason };
      for (const l of listeners) l(state);
    },
  };
}
