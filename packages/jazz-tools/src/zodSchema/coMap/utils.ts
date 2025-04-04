import { RawAccount, RawCoMap } from "cojson";
import { RawCoValue } from "cojson/src/exports.js";
import { RegisteredSchemas } from "../../coValues/registeredSchemas.js";
import { coValuesCache } from "../../lib/cache.js";

export function getOwnerFromRawValue(raw: RawCoValue | RawCoMap) {
  return coValuesCache.get(raw.group as any, () =>
    raw.group instanceof RawAccount
      ? RegisteredSchemas["Account"].fromRaw(raw.group)
      : RegisteredSchemas["Group"].fromRaw(raw.group as any),
  );
}
