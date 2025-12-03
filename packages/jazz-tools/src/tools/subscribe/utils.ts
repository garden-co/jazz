import { RawAccount, RawCoValue, Role } from "cojson";
import { RegisteredSchemas } from "../coValues/registeredSchemas.js";
import {
  CoValue,
  RefEncoded,
  accountOrGroupToGroup,
  instantiateRefEncodedFromRaw,
} from "../internal.js";
import { coValuesCache } from "../lib/cache.js";
import { SubscriptionScope } from "./SubscriptionScope.js";
import { CoValueLoadingState } from "./types.js";

export function myRoleForRawValue(raw: RawCoValue): Role | undefined {
  const rawOwner = raw.group;

  const owner = coValuesCache.get(rawOwner, () =>
    rawOwner instanceof RawAccount
      ? RegisteredSchemas["Account"].fromRaw(rawOwner)
      : RegisteredSchemas["Group"].fromRaw(rawOwner),
  );

  return accountOrGroupToGroup(owner).myRole();
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
    type: CoValueLoadingState.LOADED,
    value: freshValueInstance,
    id: subscriptionScope.id,
  };
}

export type PromiseWithStatus<T> = PromiseLike<T> & {
  status?: "pending" | "fulfilled" | "rejected";
  value?: T;
  reason?: unknown;
};

export function resolvedPromise<T>(value: T): PromiseWithStatus<T> {
  const promise = Promise.resolve(value) as PromiseWithStatus<T>;
  promise.status = "fulfilled";
  promise.value = value;
  return promise;
}

export function rejectedPromise<T>(reason: unknown): PromiseWithStatus<T> {
  const promise = Promise.reject(reason) as PromiseWithStatus<T>;
  promise.status = "rejected";
  promise.reason = reason;
  return promise;
}
