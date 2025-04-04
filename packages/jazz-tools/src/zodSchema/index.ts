import { getUnloadedState } from "./coValue/unloaded.js";
import { co, z } from "./schema.js";
import {
  ensureCoValueLoaded,
  loadCoValue,
  subscribeToCoValue,
} from "./subscribe.js";

export const SchemaV2 = {
  co,
  z,
  subscribeToCoValue: subscribeToCoValue,
  ensureCoValueLoaded: ensureCoValueLoaded,
  loadCoValue: loadCoValue,
  getUnloadedState: getUnloadedState,
} as const;

export type SchemaV2 = typeof SchemaV2;
