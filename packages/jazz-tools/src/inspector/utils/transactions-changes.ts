import type {
  AccountRole,
  BinaryStreamStart,
  CoID,
  RawCoValue,
  Role,
} from "cojson";
import type { ListOpPayload } from "cojson/dist/coValues/coList.js";
import type { MapOpPayload } from "cojson/dist/coValues/coMap.js";
import type {
  BinaryStreamChunk,
  BinaryStreamEnd,
} from "cojson/dist/coValues/coStream.js";
import { isCoId } from "../viewer/types";

export const isGroupExtension = (
  change: any,
): change is Extract<
  MapOpPayload<`child_${string}`, "extend">,
  { op: "set" }
> => {
  return change?.op === "set" && change?.value === "extend";
};

export const isGroupExtendRevocation = (
  change: any,
): change is Extract<
  MapOpPayload<`child_${string}`, "revoked">,
  { op: "set" }
> => {
  return change?.op === "set" && change?.value === "revoked";
};

export const isGroupPromotion = (
  change: any,
): change is Extract<
  MapOpPayload<`parent_co_${string}`, AccountRole>,
  { op: "set" }
> => {
  return change?.op === "set" && change?.key.startsWith("parent_co_");
};

export const isUserPromotion = (
  change: any,
): change is Extract<MapOpPayload<CoID<RawCoValue>, Role>, { op: "set" }> => {
  return (
    change?.op === "set" && (isCoId(change?.key) || change?.key === "everyone")
  );
};

export const isKeyRevelation = (
  change: any,
): change is Extract<
  MapOpPayload<`${string}_for_${string}`, string>,
  { op: "set" }
> => {
  return change?.op === "set" && change?.key.includes("_for_");
};

export const isPropertySet = (
  change: any,
): change is Extract<MapOpPayload<string, any>, { op: "set" }> => {
  return change?.op === "set" && "key" in change && "value" in change;
};
export const isPropertyDeletion = (
  change: any,
): change is Extract<MapOpPayload<string, any>, { op: "del" }> => {
  return change?.op === "del" && "key" in change;
};

export const isItemAppend = (
  change: any,
): change is Extract<ListOpPayload<any>, { op: "app" }> => {
  return change?.op === "app" && "after" in change && "value" in change;
};
export const isItemPrepend = (
  change: any,
): change is Extract<ListOpPayload<any>, { op: "pre" }> => {
  return change?.op === "pre" && "before" in change && "value" in change;
};

export const isItemDeletion = (
  change: any,
): change is Extract<ListOpPayload<any>, { op: "del" }> => {
  return change?.op === "del" && "insertion" in change;
};

export const isStreamStart = (change: any): change is BinaryStreamStart => {
  return change?.type === "start" && "mimeType" in change;
};

export const isStreamChunk = (change: any): change is BinaryStreamChunk => {
  return change?.type === "chunk" && "chunk" in change;
};

export const isStreamEnd = (change: any): change is BinaryStreamEnd => {
  return change?.type === "end";
};
