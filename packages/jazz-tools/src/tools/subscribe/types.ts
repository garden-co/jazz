import type {
  Account,
  CoValue,
  Group,
  RefsToResolve,
  Resolved,
} from "../internal.js";
import type { JazzError } from "./JazzError.js";

export const CoValueLoadingState = {
  LOADED: "loaded",
  UNLOADED: "unloaded",
  UNAUTHORIZED: "unauthorized",
  UNAVAILABLE: "unavailable",
} as const;

export type CoValueLoadingState =
  (typeof CoValueLoadingState)[keyof typeof CoValueLoadingState];

export type SubscriptionValue<D extends CoValue, R extends RefsToResolve<D>> =
  | {
      type: typeof CoValueLoadingState.LOADED;
      value: Resolved<D, R>;
      id: string;
    }
  | JazzError;
export type Unloaded = {
  type: typeof CoValueLoadingState.UNLOADED;
  id: string;
};

export type BranchDefinition = { name: string; owner?: Group | Account | null };
