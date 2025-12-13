import { beforeEach, describe, expect, it } from "vitest";
import { Account, Group, co, z } from "../exports.js";
import {
  CoValueLoadingState,
  InstanceOfSchema,
  RefEncoded,
  coValueClassFromCoValueClassOrSchema,
} from "../internal.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { JazzError } from "../subscribe/JazzError.js";
import { SubscriptionScope } from "../subscribe/SubscriptionScope.js";

describe("SubscriptionScope", () => {
  const Person = co.map({
    name: z.string(),
  });

  const personField = {
    type: "ref",
    ref: coValueClassFromCoValueClassOrSchema(Person),
    optional: false,
    field: Person,
  } satisfies RefEncoded<InstanceOfSchema<typeof Person>>;

  beforeEach(async () => {
    await setupJazzTestSync();

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  describe("getCurrentValue reference stability", () => {
    it("returns the same reference when called multiple times with the same loading state", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;

      const scope = new SubscriptionScope(node, true, id, personField);

      // Simulate LOADING state
      scope.value = { type: CoValueLoadingState.LOADING, id };

      const firstCall = scope.getCurrentValue();
      const secondCall = scope.getCurrentValue();
      const thirdCall = scope.getCurrentValue();

      // All calls should return the same reference
      expect(firstCall).toBe(secondCall);
      expect(secondCall).toBe(thirdCall);
      expect(firstCall).toBe(thirdCall);

      // Verify it's a NotLoaded value with LOADING state
      expect(firstCall.$jazz.loadingState).toBe(CoValueLoadingState.LOADING);
      expect(firstCall.$isLoaded).toBe(false);
      expect(firstCall.$jazz.id).toBe(id);

      // Verify the cached value matches
      expect(scope.unloadedValue).toBe(firstCall);

      scope.destroy();
    });

    it("returns different references when the loading state changes", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;

      const scope = new SubscriptionScope(node, true, id, personField);

      // Start with LOADING state
      scope.value = { type: CoValueLoadingState.LOADING, id };
      const loadingValue = scope.getCurrentValue();

      // Switch to UNAVAILABLE state
      scope.updateValue(
        new JazzError(id, CoValueLoadingState.UNAVAILABLE, [
          {
            code: CoValueLoadingState.UNAVAILABLE,
            message: "The value is unavailable",
            params: { id },
            path: [],
          },
        ]),
      );
      const unavailableValue = scope.getCurrentValue();

      // Switch to UNAUTHORIZED state
      scope.updateValue(
        new JazzError(id, CoValueLoadingState.UNAUTHORIZED, [
          {
            code: CoValueLoadingState.UNAUTHORIZED,
            message: "The current user is not authorized to access this value",
            params: { id },
            path: [],
          },
        ]),
      );
      const unauthorizedValue = scope.getCurrentValue();

      // All should be different references
      expect(loadingValue).not.toBe(unavailableValue);
      expect(loadingValue).not.toBe(unauthorizedValue);
      expect(unavailableValue).not.toBe(unauthorizedValue);

      // Verify each has the correct loading state
      expect(loadingValue.$jazz.loadingState).toBe(CoValueLoadingState.LOADING);
      expect(unavailableValue.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
      expect(unauthorizedValue.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );

      // Verify the cached value is the last one
      expect(scope.unloadedValue).toBe(unauthorizedValue);

      scope.destroy();
    });

    it("maintains reference stability across multiple state transitions", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;

      const scope = new SubscriptionScope(node, true, id, personField);

      // Get LOADING value multiple times
      scope.value = { type: CoValueLoadingState.LOADING, id };
      const loading1 = scope.getCurrentValue();
      const loading2 = scope.getCurrentValue();
      expect(loading1).toBe(loading2);

      // Switch to UNAVAILABLE
      scope.updateValue(
        new JazzError(id, CoValueLoadingState.UNAVAILABLE, [
          {
            code: CoValueLoadingState.UNAVAILABLE,
            message: "The value is unavailable",
            params: { id },
            path: [],
          },
        ]),
      );
      const unavailable1 = scope.getCurrentValue();
      expect(unavailable1).not.toBe(loading1);

      // Get UNAVAILABLE again - should return same reference
      const unavailable2 = scope.getCurrentValue();
      expect(unavailable1).toBe(unavailable2);

      // Switch to UNAUTHORIZED
      scope.updateValue(
        new JazzError(id, CoValueLoadingState.UNAUTHORIZED, [
          {
            code: CoValueLoadingState.UNAUTHORIZED,
            message: "The current user is not authorized to access this value",
            params: { id },
            path: [],
          },
        ]),
      );
      const unauthorized1 = scope.getCurrentValue();
      expect(unauthorized1).not.toBe(unavailable1);

      // Get UNAUTHORIZED again - should return same reference
      const unauthorized2 = scope.getCurrentValue();
      expect(unauthorized1).toBe(unauthorized2);

      // Switch back to UNAVAILABLE - should create new reference
      scope.updateValue(
        new JazzError(id, CoValueLoadingState.UNAVAILABLE, [
          {
            code: CoValueLoadingState.UNAVAILABLE,
            message: "The value is unavailable",
            params: { id },
            path: [],
          },
        ]),
      );
      const unavailable3 = scope.getCurrentValue();
      expect(unavailable3).not.toBe(unavailable1);
      expect(unavailable3).not.toBe(unavailable2);

      // Get UNAVAILABLE again - should return same reference as unavailable3
      const unavailable4 = scope.getCurrentValue();
      expect(unavailable3).toBe(unavailable4);

      scope.destroy();
    });

    it("returns stable reference when switching back to a previously used state after cache was overwritten", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;

      const scope = new SubscriptionScope(node, true, id, personField);

      // First, get a LOADING value
      scope.value = { type: CoValueLoadingState.LOADING, id };
      const firstLoadingValue = scope.getCurrentValue();

      // Switch to UNAVAILABLE (this overwrites the cache)
      scope.updateValue(
        new JazzError(id, CoValueLoadingState.UNAVAILABLE, [
          {
            code: CoValueLoadingState.UNAVAILABLE,
            message: "The value is unavailable",
            params: { id },
            path: [],
          },
        ]),
      );
      const unavailableValue = scope.getCurrentValue();

      // Switch back to LOADING (should create a new reference since cache was overwritten)
      scope.value = { type: CoValueLoadingState.LOADING, id };
      const secondLoadingValue = scope.getCurrentValue();

      // The second LOADING value should be different from the first
      // because the cache was overwritten with UNAVAILABLE
      expect(firstLoadingValue).not.toBe(secondLoadingValue);

      // But both should have the same loading state
      expect(firstLoadingValue.$jazz.loadingState).toBe(
        CoValueLoadingState.LOADING,
      );
      expect(secondLoadingValue.$jazz.loadingState).toBe(
        CoValueLoadingState.LOADING,
      );

      // The cache should now point to the second LOADING value
      expect(scope.unloadedValue).toBe(secondLoadingValue);

      scope.destroy();
    });

    it("preserves correct loading state in returned value", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;

      const scope = new SubscriptionScope(node, true, id, personField);

      // Test LOADING state
      scope.value = { type: CoValueLoadingState.LOADING, id };
      const loadingValue = scope.getCurrentValue();
      expect(loadingValue.$jazz.loadingState).toBe(CoValueLoadingState.LOADING);
      expect(loadingValue.$isLoaded).toBe(false);
      expect(loadingValue.$jazz.id).toBe(id);

      // Test UNAVAILABLE state
      scope.updateValue(
        new JazzError(id, CoValueLoadingState.UNAVAILABLE, [
          {
            code: CoValueLoadingState.UNAVAILABLE,
            message: "The value is unavailable",
            params: { id },
            path: [],
          },
        ]),
      );
      const unavailableValue = scope.getCurrentValue();
      expect(unavailableValue.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
      expect(unavailableValue.$isLoaded).toBe(false);
      expect(unavailableValue.$jazz.id).toBe(id);

      // Test UNAUTHORIZED state
      scope.updateValue(
        new JazzError(id, CoValueLoadingState.UNAUTHORIZED, [
          {
            code: CoValueLoadingState.UNAUTHORIZED,
            message: "The current user is not authorized to access this value",
            params: { id },
            path: [],
          },
        ]),
      );
      const unauthorizedValue = scope.getCurrentValue();
      expect(unauthorizedValue.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
      expect(unauthorizedValue.$isLoaded).toBe(false);
      expect(unauthorizedValue.$jazz.id).toBe(id);

      scope.destroy();
    });

    it("returns LOADING state when shouldSendUpdates returns false", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;

      const scope = new SubscriptionScope(node, true, id, personField);

      // Set up a loaded value with pending children
      const loadedPerson = Person.create({ name: "Jane" });
      scope.updateValue({
        type: CoValueLoadingState.LOADED,
        value: loadedPerson,
        id: loadedPerson.$jazz.id,
      });

      // Add a pending child to make shouldSendUpdates return false
      scope.pendingLoadedChildren.add("some-child-id");

      const value1 = scope.getCurrentValue();
      const value2 = scope.getCurrentValue();

      // Should return the same LOADING reference
      expect(value1).toBe(value2);
      expect(value1.$jazz.loadingState).toBe(CoValueLoadingState.LOADING);
      expect(value1.$isLoaded).toBe(false);

      // Clear pending children
      scope.pendingLoadedChildren.clear();

      // Now should return the loaded value
      const loadedValue = scope.getCurrentValue();
      expect(loadedValue).toBe(loadedPerson);
      expect(loadedValue.$isLoaded).toBe(true);

      scope.destroy();
    });

    it("returns error state from errorFromChildren when present", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;

      const scope = new SubscriptionScope(node, true, id, personField);

      // Set up a loaded value
      const loadedPerson = Person.create({ name: "Jane" });
      scope.updateValue({
        type: CoValueLoadingState.LOADED,
        value: loadedPerson,
        id: loadedPerson.$jazz.id,
      });

      // Set up an error from children
      const childError = new JazzError(
        "child-id" as any,
        CoValueLoadingState.UNAVAILABLE,
        [
          {
            code: CoValueLoadingState.UNAVAILABLE,
            message: "Child value is unavailable",
            params: { id: "child-id" },
            path: [],
          },
        ],
      );
      scope.errorFromChildren = childError;

      const value1 = scope.getCurrentValue();
      const value2 = scope.getCurrentValue();

      // Should return the same UNAVAILABLE reference
      expect(value1).toBe(value2);
      expect(value1.$jazz.loadingState).toBe(CoValueLoadingState.UNAVAILABLE);
      expect(value1.$isLoaded).toBe(false);

      // Clear the error
      scope.errorFromChildren = undefined;

      // Now should return the loaded value
      const loadedValue = scope.getCurrentValue();
      expect(loadedValue).toBe(loadedPerson);
      expect(loadedValue.$isLoaded).toBe(true);

      scope.destroy();
    });
  });
});
