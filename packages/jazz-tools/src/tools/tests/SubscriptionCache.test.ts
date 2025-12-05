import { beforeEach, describe, expect, it, vi } from "vitest";
import { Account, Group, co, z } from "../exports.js";
import { SubscriptionCache } from "../subscribe/SubscriptionCache.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";

describe("SubscriptionCache", () => {
  const Person = co.map({
    name: co.plainText(),
  });

  beforeEach(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  describe("cache key comparison logic", () => {
    it("matches entries with identical schema, id, resolve, and branch", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;
      const cache = new SubscriptionCache();

      const scope1 = cache.getOrCreate(node, Person, id, true, false, false);
      const scope2 = cache.getOrCreate(node, Person, id, true, false, false);

      expect(scope1).toBe(scope2);
      cache.clear();
    });

    it("creates different entries for different resolve queries", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;
      const cache = new SubscriptionCache();

      const scope1 = cache.getOrCreate(node, Person, id, true, false, false);
      const scope2 = cache.getOrCreate(
        node,
        Person,
        id,
        { name: true },
        false,
        false,
      );

      expect(scope1).not.toBe(scope2);
      cache.clear();
    });

    it("creates different entries for different branch definitions", () => {
      const person = Person.create({ name: "John" });
      const group = Group.create();
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;
      const cache = new SubscriptionCache();

      const scope1 = cache.getOrCreate(node, Person, id, true, false, false, {
        name: "branch1",
        owner: group,
      });
      const scope2 = cache.getOrCreate(node, Person, id, true, false, false, {
        name: "branch2",
        owner: group,
      });

      expect(scope1).not.toBe(scope2);
      cache.clear();
    });

    it("matches entries with same branch name but different owner references", () => {
      const person = Person.create({ name: "John" });
      const group1 = Group.create();
      const group2 = Group.create();
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;
      const cache = new SubscriptionCache();

      const scope1 = cache.getOrCreate(node, Person, id, true, false, false, {
        name: "branch",
        owner: group1,
      });
      const scope2 = cache.getOrCreate(node, Person, id, true, false, false, {
        name: "branch",
        owner: group2,
      });

      expect(scope1).not.toBe(scope2);
      cache.clear();
    });
  });

  describe("subscriber count tracking", () => {
    it("tracks subscriber count changes via onSubscriberChange", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;
      const cache = new SubscriptionCache();

      const scope = cache.getOrCreate(node, Person, id, true, false, false);

      expect(scope.subscribers.size).toBe(0);

      const unsubscribe1 = scope.subscribe(() => {});
      expect(scope.subscribers.size).toBe(1);

      const unsubscribe2 = scope.subscribe(() => {});
      expect(scope.subscribers.size).toBe(2);

      unsubscribe1();
      expect(scope.subscribers.size).toBe(1);

      unsubscribe2();
      expect(scope.subscribers.size).toBe(0);

      cache.clear();
    });
  });

  describe("cleanup lifecycle", () => {
    it("schedules cleanup when subscriber count reaches zero", async () => {
      vi.useFakeTimers();
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;
      const cache = new SubscriptionCache(100); // 100ms timeout

      const scope = cache.getOrCreate(node, Person, id, true, false, false);

      const unsubscribe = scope.subscribe(() => {});
      unsubscribe();

      await vi.advanceTimersByTimeAsync(110);

      expect(scope.closed).toBe(true);

      cache.clear();
      vi.useRealTimers();
    });

    it("cancels cleanup when new subscription arrives during pending cleanup", async () => {
      vi.useFakeTimers();
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;
      const cache = new SubscriptionCache(100); // 100ms timeout

      const scope1 = cache.getOrCreate(node, Person, id, true, false, false);

      const unsubscribe = scope1.subscribe(() => {});
      unsubscribe();

      await vi.advanceTimersByTimeAsync(50);

      // Request again before cleanup
      const scope2 = cache.getOrCreate(node, Person, id, true, false, false);

      expect(scope2).toBe(scope1);
      await vi.advanceTimersByTimeAsync(60);

      cache.clear();
      vi.useRealTimers();
    });

    it("executes cleanup after timeout", async () => {
      vi.useFakeTimers();
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const id = person.$jazz.id;
      const cache = new SubscriptionCache(100); // 100ms timeout

      const scope = cache.getOrCreate(node, Person, id, true, false, false);

      const unsubscribe = scope.subscribe(() => {});
      unsubscribe();

      await vi.advanceTimersByTimeAsync(150);

      const idSet = (cache as any).cache.get(id);
      expect(idSet).toBeUndefined();

      cache.clear();
      vi.useRealTimers();
    });
  });

  describe("clear method", () => {
    it("destroys all entries", () => {
      const person1 = Person.create({ name: "John" });
      const person2 = Person.create({ name: "Jane" });
      const node = person1.$jazz.raw.core.node;
      const cache = new SubscriptionCache();

      const scope1 = cache.getOrCreate(
        node,
        Person,
        person1.$jazz.id,
        true,
        false,
        false,
      );
      const scope2 = cache.getOrCreate(
        node,
        Person,
        person2.$jazz.id,
        true,
        false,
        false,
      );

      cache.clear();

      expect(scope1.closed).toBe(true);
      expect(scope2.closed).toBe(true);
    });
  });

  describe("edge cases", () => {
    it("throws error for null/undefined id", () => {
      const person = Person.create({ name: "John" });
      const node = person.$jazz.raw.core.node;
      const cache = new SubscriptionCache();

      expect(() => {
        cache.getOrCreate(node, Person, null as any, true, false, false);
      }).toThrow("Cannot create subscription with undefined or null id");

      expect(() => {
        cache.getOrCreate(node, Person, undefined as any, true, false, false);
      }).toThrow("Cannot create subscription with undefined or null id");

      cache.clear();
    });
  });
});
