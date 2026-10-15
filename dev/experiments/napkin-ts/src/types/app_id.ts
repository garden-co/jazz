import { uuidV5, uuidFromString } from "../support/hash.js";
import { utf8 } from "../support/bytes.js";
import { newObjectId, type ObjectId } from "./object.js";
import { parseUuid, uuidFromBytes } from "./ids.js";

/** Identifier for an application's catalogue and sync namespace (a UUID string). */
export type AppId = ObjectId;

export const NAMESPACE_DNS = uuidFromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

export const appIdRandom = (): AppId => newObjectId();
/** UUIDv5 in the DNS namespace: same name, same AppId. */
export const appIdFromName = (name: string): AppId =>
  uuidFromBytes(uuidV5(NAMESPACE_DNS, utf8(name)));
export const appIdFromString = (s: string): AppId => parseUuid(s);
