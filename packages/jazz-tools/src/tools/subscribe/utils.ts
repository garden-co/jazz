import { RawAccount, RawCoValue } from "cojson";
import { RegisteredSchemas } from "../coValues/registeredSchemas.js";
import {
  CoValue,
  RefEncoded,
  coValueClassFromCoValueClassOrSchema,
  instantiateRefEncodedFromRaw,
} from "../internal.js";
import { coValuesCache } from "../lib/cache.js";
import { SubscriptionScope } from "./SubscriptionScope.js";

export function getOwnerFromRawValue(raw: RawCoValue) {
  const owner = raw.group;

  return coValuesCache.get(owner, () =>
    owner instanceof RawAccount
      ? coValueClassFromCoValueClassOrSchema(
          RegisteredSchemas["Account"],
        ).fromRaw(owner)
      : RegisteredSchemas["Group"].fromRaw(owner as any),
  );
}

export function createCoValue<D extends CoValue>(
  ref: RefEncoded<D>,
  raw: RawCoValue,
  subscriptionScope: SubscriptionScope<D>,
) {
  const freshValueInstance = instantiateRefEncodedFromRaw(ref, raw);

  Object.defineProperty(freshValueInstance.$jazz, "_subscriptionScope", {
    value: subscriptionScope,
    writable: false,
    enumerable: false,
    configurable: false,
  });

  return {
    type: "loaded" as const,
    value: freshValueInstance,
    id: subscriptionScope.id,
  };
}
