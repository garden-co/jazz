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
} from "cojson/dist/coValues/binaryCoStream.js";
import { isCoId } from "../viewer/types";

export const isGroupExtension = (
  coValue: RawCoValue,
  change: any,
): change is Extract<
  MapOpPayload<`child_${string}`, "extend">,
  { op: "set" }
> => {
  if (coValue.core.isGroup() === false) return false;
  return change?.op === "set" && change?.value === "extend";
};

export const isGroupExtendRevocation = (
  coValue: RawCoValue,
  change: any,
): change is Extract<
  MapOpPayload<`child_${string}`, "revoked">,
  { op: "set" }
> => {
  if (coValue.core.isGroup() === false) return false;
  return change?.op === "set" && change?.value === "revoked";
};

export const isGroupPromotion = (
  coValue: RawCoValue,
  change: any,
): change is Extract<
  MapOpPayload<`parent_co_${string}`, AccountRole>,
  { op: "set" }
> => {
  if (coValue.core.isGroup() === false) return false;
  return change?.op === "set" && change?.key.startsWith("parent_co_");
};

export const isUserPromotion = (
  coValue: RawCoValue,
  change: any,
): change is Extract<MapOpPayload<CoID<RawCoValue>, Role>, { op: "set" }> => {
  if (coValue.core.isGroup() === false) return false;
  return (
    change?.op === "set" && (isCoId(change?.key) || change?.key === "everyone")
  );
};

export const isKeyRevelation = (
  coValue: RawCoValue,
  change: any,
): change is Extract<
  MapOpPayload<`${string}_for_${string}`, string>,
  { op: "set" }
> => {
  if (
    coValue.core.isGroup() === false &&
    coValue.headerMeta?.type !== "account"
  )
    return false;
  return change?.op === "set" && change?.key.includes("_for_");
};

export const isPropertySet = (
  coValue: RawCoValue,
  change: any,
): change is Extract<MapOpPayload<string, any>, { op: "set" }> => {
  return change?.op === "set" && "key" in change && "value" in change;
};
export const isPropertyDeletion = (
  coValue: RawCoValue,
  change: any,
): change is Extract<MapOpPayload<string, any>, { op: "del" }> => {
  return change?.op === "del" && "key" in change;
};

export const isItemAppend = (
  coValue: RawCoValue,
  change: any,
): change is Extract<ListOpPayload<any>, { op: "app" }> => {
  if (coValue.type !== "colist" && coValue.type !== "coplaintext") return false;
  return change?.op === "app" && "after" in change && "value" in change;
};
export const isItemPrepend = (
  coValue: RawCoValue,
  change: any,
): change is Extract<ListOpPayload<any>, { op: "pre" }> => {
  if (coValue.type !== "colist" && coValue.type !== "coplaintext") return false;
  return change?.op === "pre" && "before" in change && "value" in change;
};

export const isItemDeletion = (
  coValue: RawCoValue,
  change: any,
): change is Extract<ListOpPayload<any>, { op: "del" }> => {
  if (coValue.type !== "colist" && coValue.type !== "coplaintext") return false;
  return change?.op === "del" && "insertion" in change;
};

export const isStreamStart = (
  coValue: RawCoValue,
  change: any,
): change is BinaryStreamStart => {
  if (coValue.type !== "coStream") return false;
  return change?.type === "start" && "mimeType" in change;
};

export const isStreamChunk = (
  coValue: RawCoValue,
  change: any,
): change is BinaryStreamChunk => {
  if (coValue.type !== "coStream") return false;
  return change?.type === "chunk" && "chunk" in change;
};

export const isStreamEnd = (
  coValue: RawCoValue,
  change: any,
): change is BinaryStreamEnd => {
  if (coValue.type !== "coStream") return false;
  return change?.type === "end";
};
