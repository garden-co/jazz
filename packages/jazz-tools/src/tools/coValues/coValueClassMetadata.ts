import { type CoValueClass, type Group } from "../internal.js";

interface CoValueClassMetadata {
  configureImplicitGroupOwner?: (newGroup: Group) => void;
}

/**
 * Stores metadata for CoValue classes.
 *
 * Used to avoid circular dependencies between CoValue schemas and CoValue classes.
 *
 * @internal
 */
export const coValueClassMetadata = new WeakMap<
  // Class or constructor function
  CoValueClass | Function,
  CoValueClassMetadata
>();
