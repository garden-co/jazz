import type { CoValue, CoValueClass, RefEncoded } from "../internal.js";
import { createUnloadedCoValue } from "../internal.js";
import { SubscriptionScope } from "./SubscriptionScope.js";
import { CoValueLoadingState } from "./types.js";

export function getSubscriptionScope<D extends CoValue>(value: D) {
  const subscriptionScope = value.$jazz._subscriptionScope;

  if (subscriptionScope) {
    return subscriptionScope;
  }

  const node = value.$jazz.raw.core.node;
  const resolve = true;
  const id = value.$jazz.id;

  const newSubscriptionScope = new SubscriptionScope(node, resolve, id, {
    ref: value.constructor as CoValueClass<D>,
    optional: false,
  });

  Object.defineProperty(value.$jazz, "_subscriptionScope", {
    value: newSubscriptionScope,
    writable: false,
    enumerable: false,
    configurable: false,
  });

  newSubscriptionScope.destroy();

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

  const node = subscriptionScope.childNodes.get(childId);

  if (!subscriptionScope.isSubscribedToId(childId)) {
    subscriptionScope.subscribeToKey(key);
  } else if (node && node.closed) {
    node.pullValue((value) =>
      subscriptionScope.handleChildUpdate(childId, value),
    );
  }

  const value = subscriptionScope.childValues.get(childId);

  if (value?.type === CoValueLoadingState.LOADED) {
    return value.value;
  } else {
    return createUnloadedCoValue(childId, CoValueLoadingState.UNLOADED);
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

  if (value?.type === CoValueLoadingState.LOADED) {
    return value.value;
  } else {
    return createUnloadedCoValue(childId, CoValueLoadingState.UNLOADED);
  }
}
