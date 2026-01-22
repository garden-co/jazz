import { RawAccount, RawCoValue, Role } from "cojson";
import { RegisteredSchemas } from "../coValues/registeredSchemas.js";
import {
  CoValue,
  RefEncoded,
  RefsToResolve,
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
  ref: RefEncoded<CoValue>,
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
    value: freshValueInstance as unknown as D,
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

export function isEqualRefsToResolve(
  a: RefsToResolve<any>,
  b: RefsToResolve<any>,
) {
  // Fast path: same reference
  if (a === b) {
    return true;
  }

  // Fast path: both are boolean
  if (typeof a === "boolean" && typeof b === "boolean") {
    return a === b;
  }

  // One is boolean, the other is not
  if (typeof a === "boolean" || typeof b === "boolean") {
    return false;
  }

  // Both must be objects at this point
  if (
    typeof a !== "object" ||
    typeof b !== "object" ||
    a === null ||
    b === null
  ) {
    return false;
  }

  // Get all keys from both objects
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);

  // Different number of keys means not equal
  if (keysA.length !== keysB.length) {
    return false;
  }

  // Check each key
  for (const key of keysA) {
    if (!(key in b)) {
      return false;
    }

    const valueA = (a as any)[key];
    const valueB = (b as any)[key];

    // Recursively compare nested RefsToResolve values
    if (!isEqualRefsToResolve(valueA, valueB)) {
      return false;
    }
  }

  return true;
}
