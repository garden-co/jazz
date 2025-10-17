import type {
  Account,
  CoValue,
  Group,
  RefsToResolve,
  Resolved,
} from "../internal.js";
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

export type NotLoadedCoValueState =
  | typeof CoValueLoadingState.LOADING
  | typeof CoValueLoadingState.UNAUTHORIZED
  | typeof CoValueLoadingState.UNAVAILABLE;

export type SubscriptionValue<D extends CoValue, R extends RefsToResolve<D>> =
  | {
      type: typeof CoValueLoadingState.LOADED;
      value: Resolved<D, R>;
      id: string;
    }
  | JazzError;
export type SubscriptionValueLoading = {
  type: typeof CoValueLoadingState.LOADING;
  id: string;
};

export type BranchDefinition = { name: string; owner?: Group | Account | null };
