import { useCallback, useSyncExternalStore } from "react";
import { useJazzClient } from "./provider.js";
import type { AuthFailureReason } from "../runtime/auth-state.js";
import type { AuthMode } from "../runtime/context.js";

export interface AuthStateInfo {
  authMode: AuthMode;
  userId: string | null;
  claims: Record<string, unknown>;
  error?: AuthFailureReason;
}

const EMPTY_CLAIMS: Record<string, unknown> = Object.freeze({});

export function useAuthState(): AuthStateInfo {
  const client = useJazzClient();
  const state = useSyncExternalStore(
    useCallback((cb) => client.db.onAuthChanged(cb), [client.db]),
    () => client.db.getAuthState(),
    () => client.db.getAuthState(),
  );

  return {
    authMode: state.authMode,
    userId: state.session?.user_id ?? null,
    claims: state.session?.claims ?? EMPTY_CLAIMS,
    error: state.error,
  };
}
