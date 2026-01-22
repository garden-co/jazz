import type { Account, CoValue, Group, Resolved } from "../internal.js";
import type { JazzError } from "./JazzError.js";

export const CoValueLoadingState = {
  /**
   * The coValue is loaded.
   */
  LOADED: "loaded",
  /**
   * The coValue is being loaded.
   */
  LOADING: "loading",
  /**
   * The coValue existed but has been deleted (tombstoned).
   */
  DELETED: "deleted",
  /**
   * The coValue was loaded but the account is not authorized to access it.
   */
  UNAUTHORIZED: "unauthorized",
  /**
   * Tried to load the coValue but failed.
   */
  UNAVAILABLE: "unavailable",
} as const;

export type CoValueLoadingState =
  (typeof CoValueLoadingState)[keyof typeof CoValueLoadingState];

export type CoValueErrorState =
  | typeof CoValueLoadingState.UNAVAILABLE
  | typeof CoValueLoadingState.DELETED
  | typeof CoValueLoadingState.UNAUTHORIZED;

export type NotLoadedCoValueState =
  | typeof CoValueLoadingState.LOADING
  | CoValueErrorState;

export type SubscriptionValue<D> =
  | {
      type: typeof CoValueLoadingState.LOADED;
      value: D;
      id: string;
    }
  | JazzError;
export type SubscriptionValueLoading = {
  type: typeof CoValueLoadingState.LOADING;
  id: string;
};

export type BranchDefinition = { name: string; owner?: Group | Account };

/**
 * Detail structure for subscription performance marks and measures.
 * Used by SubscriptionScope.trackLoadingPerformance() to emit performance data.
 */
export interface SubscriptionPerformanceDetail {
  /** Type of performance entry */
  type: "jazz-subscription";
  /** Unique identifier for this subscription instance */
  uuid: string;
  /** CoValue ID (e.g., "co_z1234...") */
  id: string;
  /** Source identifier (hook name or API) */
  source: string;
  /** The resolve query object */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  resolve: any;
  /** Current status of the subscription */
  status: "pending" | "loaded" | "error";
  /** When the subscription started loading (DOMHighResTimeStamp) */
  startTime: number;
  /** When loading completed (if completed) */
  endTime?: number;
  /** Total load time in ms (if completed) */
  duration?: number;
  /** Error type if status is "error" */
  errorType?: "unavailable" | "unauthorized" | "deleted";
  /** Stack trace captured at subscription creation time */
  callerStack?: string;
  devtools?: ExtensionTrackEntryPayload | ExtensionMarkerPayload;
}

type DevToolsColor =
  | "primary"
  | "primary-light"
  | "primary-dark"
  | "secondary"
  | "secondary-light"
  | "secondary-dark"
  | "tertiary"
  | "tertiary-light"
  | "tertiary-dark"
  | "error";

interface ExtensionTrackEntryPayload {
  dataType?: "track-entry"; // Defaults to "track-entry"
  color?: DevToolsColor; // Defaults to "primary"
  track: string; // Required: Name of the custom track
  trackGroup?: string; // Optional: Group for organizing tracks
  properties?: [string, string][]; // Key-value pairs for detailed view
  tooltipText?: string; // Short description for tooltip
}

interface ExtensionMarkerPayload {
  dataType: "marker"; // Required: Identifies as a marker
  color?: DevToolsColor; // Defaults to "primary"
  properties?: [string, string][]; // Key-value pairs for detailed view
  tooltipText?: string; // Short description for tooltip
}
