import { accountRegistryUrl } from "../accounts/context.js";
import { createAccountManagerWithRuntime } from "../accounts/enrollment.js";
import type { AuthState } from "../runtime/auth-state.js";
import type { AuthMode, PublicSession } from "../runtime/context.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";

export function makeFakeClient(params: {
  authMode: AuthMode;
  userId: string;
  claims: Record<string, unknown>;
}) {
  const session: PublicSession = {
    user: {
      account: "00000000-0000-4000-8000-000000000002",
      identity: {
        issuer:
          params.authMode === "external" ? "https://issuer.example" : `urn:jazz:${params.authMode}`,
        subject: params.userId,
      },
    },
    claims: params.claims,
    authMode: params.authMode,
  };
  let state: AuthState = { authMode: params.authMode, session };
  const listeners = new Set<(s: AuthState) => void>();
  const updateAuthTokenSpy = { lastToken: null as string | null };
  return attachSubscriptionStore(
    {
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
      shutdown: async () => {},
      __updateAuthTokenSpy: updateAuthTokenSpy,
      __markUnauthenticated(reason: "expired" | "invalid" | "missing" | "disabled") {
        state = { ...state, error: reason };
        for (const l of listeners) l(state);
      },
    },
    {} as any,
  );
}

/** Mock only native crypto for provider lifecycle tests; use a real opaque handle. */
export function makeFakeAccount() {
  const identity = {
    issuer: "urn:jazz:local-first",
    subject: "00000000-0000-4000-8000-000000000001",
  };
  return createAccountManagerWithRuntime({
    registry: accountRegistryUrl("https://jazz.example.com", "app-1"),
    localFirst: {
      create: () => ({
        accountId: "00000000-0000-4000-8000-000000000002",
        identity,
        auth: `e30.${btoa(JSON.stringify({ iss: identity.issuer, sub: identity.subject }))}.sig`,
      }),
    },
  }).createLocalFirst();
}
