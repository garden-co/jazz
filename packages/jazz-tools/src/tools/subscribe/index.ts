import type { CoValue, CoValueClass, RefEncoded } from "../internal.js";
import { SubscriptionScope } from "./SubscriptionScope.js";

export function getSubscriptionScope<D extends CoValue>(value: D) {
  const subscriptionScope = value._subscriptionScope;

  if (subscriptionScope) {
    return subscriptionScope;
  }

  const node = value.$jazz.raw.core.node;
  const resolve = true;
  const id = value.id;

  const newSubscriptionScope = new SubscriptionScope(node, resolve, id, {
    ref: value.constructor as CoValueClass<D>,
    optional: false,
  });

  Object.defineProperty(value, "_subscriptionScope", {
    value: subscriptionScope,
    writable: false,
    enumerable: false,
    configurable: false,
  });

  return newSubscriptionScope;
}

/** Autoload internals */

/**
 * Given a coValue, access a child coValue by key
 *
 * By subscribing to a given key, the subscription will automatically react to the id changes
 * on that key (e.g. deleting the key value will result on unsubscribing from the id)
 */
export function accessChildByKey<D extends CoValue>(
  parent: D,
  childId: string,
  key: string,
) {
  const subscriptionScope = getSubscriptionScope(parent);

  if (!subscriptionScope.childValues.has(childId)) {
    subscriptionScope.subscribeToKey(key);
  }

  const value = subscriptionScope.childValues.get(childId);

  if (value?.type === "loaded") {
    return value.value;
  } else {
    return null;
  }
}

/**
 * Given a coValue, access a child coValue by id
 *
 * By subscribing to a given id, the subscription becomes permanent and will unsubscribe
 * only when the root subscription scope is destroyed.
 *
 * Used for refs that never change (e.g. CoFeed entries, CoMap edits)
 */
export function accessChildById<D extends CoValue>(
  parent: D,
  childId: string,
  schema: RefEncoded<CoValue>,
) {
  const subscriptionScope = getSubscriptionScope(parent);

  subscriptionScope.subscribeToId(childId, schema);

  const value = subscriptionScope.childValues.get(childId);

  if (value?.type === "loaded") {
    return value.value;
  } else {
    return null;
  }
}
