import type { InspectWhoami } from "jazz-tools/inspect";
import { createContext, useContext, type PropsWithChildren } from "react";
import type { InspectorCall } from "./api.js";
import type { ApiDataSource } from "./api-data.js";
import type { InspectorLinkTarget } from "jazz-tools/inspect";

export interface BackendContextValue {
  call: InspectorCall;
  whoami: InspectWhoami;
  data: ApiDataSource;
  /** Whose permissions reads and writes run under; null for the backend's own. */
  viewAs: string | null;
  setViewAs(userId: string | null): void;
  /** What the link this page was opened with pointed at, if anything. */
  link: InspectorLinkTarget | null;
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
