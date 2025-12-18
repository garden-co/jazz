import type {
  Account,
  CoValue,
  Group,
  Loaded,
  CoreCoValueSchema,
  ResolveQuery,
  CoreAccountSchema,
  CoreGroupSchema,
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

export type CoValueErrorState =
  | typeof CoValueLoadingState.UNAVAILABLE
  | typeof CoValueLoadingState.UNAUTHORIZED;

export type NotLoadedCoValueState =
  | typeof CoValueLoadingState.LOADING
  | CoValueErrorState;

export type SubscriptionValue<
  S extends CoreCoValueSchema,
  R extends ResolveQuery<S>,
> =
  | {
      type: typeof CoValueLoadingState.LOADED;
      value: Loaded<S, R>;
      id: string;
    }
  | JazzError;
export type SubscriptionValueLoading = {
  type: typeof CoValueLoadingState.LOADING;
  id: string;
};

export type BranchDefinition = {
  name: string;
  owner?:
    | Loaded<CoreAccountSchema, true>
    | Loaded<CoreGroupSchema, true>
    | null;
};
