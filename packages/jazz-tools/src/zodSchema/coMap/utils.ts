import { RawAccount, RawCoValue } from "cojson";
import { RegisteredSchemas } from "../../coValues/registeredSchemas.js";
import { coValuesCache } from "../../lib/cache.js";

export function getOwnerFromRawValue(raw: RawCoValue) {
  return coValuesCache.get(raw.group as any, () =>
    raw.group instanceof RawAccount
      ? RegisteredSchemas["Account"].fromRaw(raw.group)
      : RegisteredSchemas["Group"].fromRaw(raw.group as any),
  );
}
