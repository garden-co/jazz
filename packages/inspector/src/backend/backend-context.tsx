import type { InspectWhoami } from "jazz-tools/inspect";
import { createContext, useContext, type PropsWithChildren } from "react";
import type { InspectorCall } from "./api.js";
import type { ApiDataSource } from "./api-data.js";
import type { InspectAs, InspectorLinkTarget } from "jazz-tools/inspect";

export interface BackendContextValue {
  call: InspectorCall;
  whoami: InspectWhoami;
  data: ApiDataSource;
  /**
   * Whose permissions reads and writes run under, with the session claims
   * given for them; null for the backend's own.
   */
  viewAs: ViewAs | null;
  setViewAs(next: ViewAs | null): void;
  /** The app's permissions read session claims, so viewing as a user needs theirs. */
  permissionsReadClaims: boolean;
  /** What the link this page was opened with pointed at, if anything. */
  link: InspectorLinkTarget | null;
}

export interface ViewAs {
  userId: string;
  claims?: Record<string, unknown>;
}

/** `viewAs` as the API takes it: the bare user id when no claims are given. */
export function asForApi(viewAs: ViewAs | null): InspectAs | undefined {
  if (!viewAs) return undefined;
  return viewAs.claims ? viewAs : viewAs.userId;
}

const BackendContext = createContext<BackendContextValue | null>(null);

export function BackendProvider({
  value,
  children,
}: PropsWithChildren<{ value: BackendContextValue }>) {
  return <BackendContext.Provider value={value}>{children}</BackendContext.Provider>;
}

/** The backend this Inspector was served by; null in the overlay and the standalone Inspector. */
export function useBackendContext(): BackendContextValue | null {
  return useContext(BackendContext);
}
