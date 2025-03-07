import { co } from "./schema.js";
import {
  ensureCoValueLoaded,
  loadCoValue,
  subscribeToCoValue,
} from "./subscribe.js";

export const SchemaV2 = {
  co,
  subscribeToCoValue: subscribeToCoValue,
  ensureCoValueLoaded: ensureCoValueLoaded,
  loadCoValue: loadCoValue,
} as const;

export type SchemaV2 = typeof SchemaV2;
