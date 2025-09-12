import { beforeEach, describe, expect, test, vi } from "vitest";
import { Group, co, z } from "../exports.js";
import { CoValueCoreSubscription } from "./CoValueCoreSubscription.js";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing.js";
import { waitFor } from "../tests/utils.js";

beforeEach(async () => {
  await setupJazzTestSync();

  // Create a test account for each test
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
});

describe("CoValueCoreSubscription", async () => {
  /**
   * Tests scenarios where the CoValue is immediately available
   * (already loaded in memory, no async loading required)
   */
  describe("immediate availability scenarios", () => {
    test("should emit immediately when CoValue is available and no branch requested", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      type Person = co.loaded<typeof Person>;

      // Create a person that's immediately available
      const person = Person.create({ name: "John", age: 30 });
      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to the person without requesting a specific branch
      const subscription = new CoValueCoreSubscription(
        person.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
      );

      // Should immediately call the listener since CoValue is available
      expect(listener).toHaveBeenCalledTimes(1);
      expect(lastResult.get("name")).toEqual("John");

      subscription.unsubscribe();
    });

    test("should subscribe to branch when CoValue is available and branch is requested and available", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // Create a person that's immediately available
      const person = Person.create({ name: "John", age: 30 });
      let lastResult: any = null;
      const listener = vi.fn();

      // Create a branch on the person with modified data
      const branch = person.$jazz.raw.core.createBranch(
        "main",
        person.$jazz.owner.$jazz.raw.id,
      );

      // @ts-ignore Update the person name in the branch
      branch.getCurrentContent().set("name", "Jane");

      // Subscribe to the specific branch
      const subscription = new CoValueCoreSubscription(
        person.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main", owner: person.$jazz.owner },
      );

      // Should immediately call the listener with branch data
      expect(listener).toHaveBeenCalledTimes(1);
      expect(lastResult.get("name")).toEqual("Jane");

      subscription.unsubscribe();
    });

    test("should immediately load when a new branch is requested", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // Create a person that's immediately available
      const person = Person.create({ name: "John", age: 30 });
      let lastResult: any = null;
      const listener = vi.fn();

      // Request a branch that doesn't exist yet
      const subscription = new CoValueCoreSubscription(
        person.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main" },
      );

      // Should immediately load the branch
      expect(listener).toHaveBeenCalled();
      expect(listener).toHaveBeenCalledTimes(1);

      // Should return the branch, that contains the source data
      expect(lastResult.get("name")).toEqual("John");
      expect(lastResult.id).not.toBe(person.$jazz.id); // Should be a different instance

      subscription.unsubscribe();
    });

    test("should fall through to loading when CoValue is not available and branch is requested", async () => {
      const bob = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // Create a person that's immediately available
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      person.$jazz.raw.core.createBranch("main");
      person.$jazz.set("name", "Jane");

      let lastResult: any = null;
      const listener = vi.fn();

      // Request a branch that doesn't exist yet
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main" },
      );

      // // Should not call listener immediately since branch isn't available
      expect(listener).not.toHaveBeenCalled();

      // Wait for the branch to be created and loaded
      await waitFor(() => expect(listener).toHaveBeenCalled());

      expect(listener).toHaveBeenCalledTimes(1);

      // Should return the branch, that contains the source data
      expect(lastResult.core.isBranched()).toEqual(true);

      await waitFor(() => {
        expect(lastResult.get("name")).toEqual("John");
      });

      expect(lastResult.id).not.toBe(person.$jazz.id); // Should be a different instance

      subscription.unsubscribe();
    });

    test("should fall through to loading when CoValue is available and branch is not available", async () => {
      const bob = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // Create a person that's immediately available
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      const branch = person.$jazz.raw.core.createBranch("main");
      person.$jazz.set("name", "Jane");

      let lastResult: any = null;
      const listener = vi.fn();

      await Person.load(person.$jazz.id, {
        loadAs: bob,
      });

      // Since the peer is server, we automatically get the branch
      // so we delete it to simulate a situation where the branch is created but not available
      bob.$jazz.localNode.internalDeleteCoValue(branch.id);

      // Request a branch that doesn't exist yet
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main" },
      );

      // // Should not call listener immediately since branch isn't available
      expect(listener).not.toHaveBeenCalled();

      // Wait for the branch to be created and loaded
      await waitFor(() => expect(listener).toHaveBeenCalled());

      expect(listener).toHaveBeenCalledTimes(1);

      // Should return the branch, that contains the source data
      expect(lastResult.get("name")).toEqual("John");
      expect(lastResult.id).not.toBe(person.$jazz.id); // Should be a different instance

      subscription.unsubscribe();
    });
  });

  /**
   * Tests scenarios where the CoValue needs to be loaded asynchronously
   * (not currently in memory, requires network/sync operations)
   */
  describe("loading scenarios", () => {
    test("should emit in async when CoValue is available and no branch requested", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person on a different account that bob doesn't have access to yet
      // The sync is delayed by a queueMicrotask, making the load async
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to a CoValue that needs to be loaded
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
      );

      // Should not call listener immediately since CoValue needs to be loaded
      expect(listener).not.toHaveBeenCalled();

      // Wait for the async loading to complete
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Should call listener with the loaded value
      expect(lastResult.get("name")).toEqual("John");
      expect(lastResult.id).toBe(person.$jazz.id);

      subscription.unsubscribe();
    });

    test("should handle loading when CoValue is not available and branch is requested", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person on a different account that bob doesn't have access to yet
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      let lastResult: any = null;
      const listener = vi.fn();

      // Request both the CoValue and a specific branch
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main" },
      );

      // Should not call listener immediately since both CoValue and branch need to be loaded
      expect(listener).not.toHaveBeenCalled();

      // Wait for the async loading to complete
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Should return the branch, that contains the source data
      expect(lastResult.get("name")).toEqual("John");
      expect(lastResult.id).not.toBe(person.$jazz.id); // Should be a different instance

      subscription.unsubscribe();
    });
  });

  /**
   * Tests scenarios involving branch checkout operations
   * (creating, accessing, and working with different branches of CoValues)
   */
  describe("branch checkout scenarios", () => {
    test("should handle successful branch checkout when source is not available", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person on a different account that bob doesn't have access to yet
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      // Create a branch on the person with modified data
      const branch = person.$jazz.raw.core.createBranch(
        "main",
        person.$jazz.owner.$jazz.raw.id,
      );

      // @ts-ignore Update the person name in the branch
      branch.getCurrentContent().set("name", "Jane");

      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to the specific branch
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main", owner: person.$jazz.owner },
      );

      // Should not call listener immediately since source isn't available
      expect(listener).not.toHaveBeenCalled();

      // Wait for the branch checkout to complete
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Should return the branch data
      await waitFor(() => expect(lastResult.get("name")).toEqual("Jane"));
      expect(lastResult.id).not.toBe(person.$jazz.id); // Should be a different instance

      subscription.unsubscribe();
    });

    test("should create a private branch when a custom owner id is provided", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person on a different account that bob doesn't have access to yet
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic(), // Only read access
      );

      // Create a branch on the person using the current owner id
      const branch = person.$jazz.raw.core.createBranch(
        "main",
        person.$jazz.owner.$jazz.raw.id,
      );

      // @ts-ignore Update the person name in the branch
      branch.getCurrentContent().set("name", "Jane");

      let lastResult: any = null;
      const listener = vi.fn();

      // Wait for the branch to sync before subscribing
      await branch.waitForSync();

      // Prefetch the person, so the branch can be created synchronously
      await Person.load(person.$jazz.id, {
        loadAs: bob,
      });

      // Subscribe with bob's ID as the owner, creating a private branch
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main", owner: bob },
      );

      // Loads immediately, new branch is created
      expect(listener).toHaveBeenCalled();

      // Should return the source data (not branch data) since it's a private branch
      expect(lastResult.get("name")).toEqual("John");
      expect(lastResult.core.getGroup().id).toBe(bob.$jazz.id); // Should be owned by bob
      expect(lastResult.id).not.toBe(person.$jazz.id); // Should be a different instance
      expect(lastResult.id).not.toBe(branch.id); // Should not be the original branch

      // Should have write access to the private branch, even though source gives only read access
      lastResult.set("name", "Guido");
      expect(lastResult.get("name")).toEqual("Guido");

      subscription.unsubscribe();
    });
  });

  describe("error handling scenarios", () => {
    test("should handle return unavailable when the id is invalid", async () => {
      const bob = await createJazzTestAccount();
      const invalidId = "invalid-co-value-id";

      let lastResult: any = null;
      const listener = vi.fn();

      // Try to subscribe to an invalid CoValue ID
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        invalidId,
        (result) => {
          lastResult = result;
          listener(result);
        },
      );

      // Should not call listener immediately since ID is invalid
      expect(listener).not.toHaveBeenCalled();

      // Wait for the error handling to complete
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Should report unavailable when loading fails
      expect(lastResult).toBe("unavailable");

      subscription.unsubscribe();
    });

    test("should handle return unavailable when the id is invalid and a branch is requested", async () => {
      const bob = await createJazzTestAccount();
      const invalidId = "invalid-co-value-id";

      let lastResult: any = null;
      const listener = vi.fn();

      // Try to subscribe to an invalid CoValue ID with branch request
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        invalidId,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main", owner: bob },
      );

      // Should not call listener immediately since ID is invalid
      expect(listener).not.toHaveBeenCalled();

      // Wait for the error handling to complete
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Should report unavailable when loading fails
      expect(lastResult).toBe("unavailable");

      subscription.unsubscribe();
    });

    test("should handle return unavailable when the owner is unavailable", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const alice = await createJazzTestAccount();

      // Disconnect all peers to not sync the unavailable group
      alice.$jazz.localNode.syncManager
        .getServerPeers(alice.$jazz.raw.id)
        .forEach((peer) => peer.gracefulShutdown());

      const unavailableGroup = Group.create(alice).makePublic("writer");

      const bob = await createJazzTestAccount();

      // Create a person that bob can access
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );
      let lastResult: any = null;
      const listener = vi.fn();

      // Try to subscribe with an invalid owner ID
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        true,
        { name: "main", owner: unavailableGroup },
      );

      // Should not call listener immediately since owner is unavailable
      expect(listener).not.toHaveBeenCalled();

      // Wait for the error handling to complete
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Should report unavailable when loading fails
      expect(lastResult).toBe("unavailable");

      subscription.unsubscribe();
    });
  });

  /**
   * Tests scenarios where CoValues transition from unavailable to available
   */
  describe("resolving an unavailable covalue", () => {
    test("should handle state changes when source becomes available", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person that bob can access
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      // Disconnect all peers to make the CoValue unavailable
      bob.$jazz.localNode.syncManager
        .getServerPeers(person.$jazz.raw.id)
        .forEach((peer) => peer.gracefulShutdown());

      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to the now-unavailable CoValue with branch request
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main" },
      );

      // Wait for the initial unavailable state
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Clear the listener to track new calls
      listener.mockClear();

      // Reconnect to make the CoValue available again
      bob.$jazz.localNode.syncManager.addPeer(
        getPeerConnectedToTestSyncServer(),
      );

      // Wait for the CoValue to become available
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Should return the source data when branch isn't available
      expect(lastResult.get("name")).toEqual("John");
      expect(lastResult.id).not.toBe(person.$jazz.id); // Should be a different instance

      subscription.unsubscribe();
    });

    test("should handle state changes when source becomes available and no branch requested", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person that bob can access
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      // Disconnect all peers to make the CoValue unavailable
      bob.$jazz.localNode.syncManager
        .getServerPeers(person.$jazz.raw.id)
        .forEach((peer) => peer.gracefulShutdown());

      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to the now-unavailable CoValue without branch request
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
      );

      // Wait for the initial unavailable state
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Clear the listener to track new calls
      listener.mockClear();

      // Reconnect to make the CoValue available again
      bob.$jazz.localNode.syncManager.addPeer(
        getPeerConnectedToTestSyncServer(),
      );

      // Wait for the CoValue to become available
      await waitFor(() => expect(listener).toHaveBeenCalled());

      // Should return the original CoValue when no branch is requested
      expect(lastResult.get("name")).toEqual("John");
      expect(lastResult.id).toBe(person.$jazz.id);

      subscription.unsubscribe();
    });
  });

  /**
   * Tests unsubscribe behavior in various scenarios
   * (immediate unsubscribe, multiple calls, during async operations)
   */
  describe("unsubscribe scenarios", () => {
    test("should properly unsubscribe when called", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // Create a person that's immediately available
      const person = Person.create({ name: "John", age: 30 });
      const listener = vi.fn();

      // Subscribe to the person
      const subscription = new CoValueCoreSubscription(
        person.$jazz.localNode,
        person.$jazz.id,
        (value) => {
          listener(value);
        },
      );

      // Should call listener once for initial value
      expect(listener).toHaveBeenCalledTimes(1);

      // Unsubscribe from updates
      subscription.unsubscribe();

      // Update the person to trigger subscription callback
      person.$jazz.set("name", "Jane");

      // Listener should not be called after unsubscribe
      expect(listener).toHaveBeenCalledTimes(1);
    });

    test("should handle multiple unsubscribe calls gracefully", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // Create a person that's immediately available
      const person = Person.create({ name: "John", age: 30 });
      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to the person
      const subscription = new CoValueCoreSubscription(
        person.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
      );

      // Should call listener once for initial value
      expect(listener).toHaveBeenCalledTimes(1);

      // Call unsubscribe multiple times
      subscription.unsubscribe();
      subscription.unsubscribe(); // Second call should not cause issues

      // Update the person to trigger subscription callback
      person.$jazz.set("name", "Jane");

      // Listener should not be called after unsubscribe
      expect(listener).toHaveBeenCalledTimes(1);
    });

    test("should unsubscribe during async operations", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person on a different account that bob doesn't have access to yet
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to a CoValue that needs to be loaded
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
      );

      // Unsubscribe immediately before the async operation completes
      subscription.unsubscribe();

      // Wait a bit to ensure async operations would have completed
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Listener should not be called after unsubscribe
      expect(listener).toHaveBeenCalledTimes(0);
    });

    test("should unsubscribe during checkout operations", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person on a different account that bob doesn't have access to yet
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to a CoValue with branch request that needs to be loaded
      const subscription = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main" },
      );

      // Unsubscribe immediately before the async operation completes
      subscription.unsubscribe();

      // Wait a bit to ensure async operations would have completed
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Listener should not be called after unsubscribe
      expect(listener).toHaveBeenCalledTimes(0);
    });
  });

  /**
   * Tests concurrent operations and multiple subscriptions
   * (multiple subscribers to same CoValue, same branch, etc.)
   */
  describe("concurrent operations", () => {
    test("should handle multiple subscriptions to the same CoValue", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person on a different account that bob doesn't have access to yet
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      let lastResultSubscription1: any = null;
      let lastResultSubscription2: any = null;
      const listener = vi.fn();

      // Subscribe to the CoValue with branch request
      const subscription1 = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResultSubscription1 = result;
          listener(result);
        },
      );
      const subscription2 = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResultSubscription2 = result;
          listener(result);
        },
      );

      await waitFor(() => expect(listener).toHaveBeenCalledTimes(2));

      // Should return the same instance of the RawCoValue
      expect(lastResultSubscription1).toBe(lastResultSubscription2);
      expect(lastResultSubscription1.get("name")).toEqual("John");
      expect(lastResultSubscription1.id).toBe(person.$jazz.id);

      subscription1.unsubscribe();
      subscription2.unsubscribe();
    });

    test("should handle multiple subscriptions to the same branch", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      const bob = await createJazzTestAccount();

      // Create a person on a different account that bob doesn't have access to yet
      const person = Person.create(
        { name: "John", age: 30 },
        Group.create().makePublic("writer"),
      );

      let lastResultSubscription1: any = null;
      let lastResultSubscription2: any = null;
      const listener = vi.fn();

      // Subscribe to the CoValue with branch request
      const subscription1 = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResultSubscription1 = result;
          listener(result);
        },
        false,
        { name: "main" },
      );
      const subscription2 = new CoValueCoreSubscription(
        bob.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResultSubscription2 = result;
          listener(result);
        },
        false,
        { name: "main" },
      );

      // Wait for the async loading to complete
      await waitFor(() => expect(listener).toHaveBeenCalledTimes(2));

      // Should return the same instance of the RawCoValue
      expect(lastResultSubscription1).toBe(lastResultSubscription2);
      expect(lastResultSubscription1.get("name")).toEqual("John");
      expect(lastResultSubscription1.id).not.toBe(person.$jazz.id);

      subscription1.unsubscribe();
      subscription2.unsubscribe();
    });
  });

  /**
   * Tests real-time update scenarios
   * (property changes, branch updates, rapid successive changes)
   */
  describe("updates", () => {
    test("should receive updates when CoValue properties change", async () => {
      // Define a Person schema with optional email field
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string().optional(),
      });

      // Create a person that's immediately available
      const person = Person.create({ name: "John", age: 30 });
      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to the person
      const subscription = new CoValueCoreSubscription(
        person.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
      );

      // Initial call with default values
      expect(listener).toHaveBeenCalledTimes(1);
      expect(lastResult.get("name")).toEqual("John");

      // Update properties to trigger subscription callbacks
      person.$jazz.set("age", 31);
      person.$jazz.set("email", "john@example.com");

      // Wait for all updates to be processed
      await waitFor(() => expect(listener).toHaveBeenCalledTimes(3));

      // Check that we received updates for each change
      expect(lastResult.get("age")).toEqual(31);
      expect(lastResult.get("email")).toEqual("john@example.com");
      expect(lastResult.get("name")).toEqual("John"); // Other properties should remain
      expect(lastResult.get("age")).toEqual(31);

      subscription.unsubscribe();
    });

    test("should receive updates when CoValue properties change in branch", async () => {
      // Define a Person schema with optional email field
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string().optional(),
      });

      // Create a person that's immediately available
      const person = Person.create({ name: "John", age: 30 });

      // Create a branch on the person
      const branch = person.$jazz.raw.core.createBranch(
        "main",
        person.$jazz.owner.$jazz.raw.id,
      );

      // @ts-ignore Update the person age in the branch
      branch.getCurrentContent().set("age", 25);

      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to the specific branch
      const subscription = new CoValueCoreSubscription(
        person.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main", owner: person.$jazz.owner },
      );

      // Initial call with branch value
      expect(listener).toHaveBeenCalledTimes(1);
      expect(lastResult.get("age")).toEqual(25);

      // @ts-ignore Update the person name in the branch
      branch.getCurrentContent().set("name", "Jane");

      // Wait for the update to be processed
      await waitFor(() => expect(listener).toHaveBeenCalledTimes(2));

      // Check that we received updates for each change
      expect(lastResult.get("name")).toEqual("Jane");
      expect(lastResult.get("age")).toEqual(25); // Should remain from branch

      subscription.unsubscribe();
    });

    test("should not receive updates when CoValue properties change in source", async () => {
      // Define a Person schema with optional email field
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string().optional(),
      });

      // Create a person that's immediately available
      const person = Person.create({ name: "John", age: 30 });

      // Create a branch on the person
      const branch = person.$jazz.raw.core.createBranch(
        "main",
        person.$jazz.owner.$jazz.raw.id,
      );

      // @ts-ignore Update the person age in the branch
      branch.getCurrentContent().set("age", 25);

      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to the specific branch
      const subscription = new CoValueCoreSubscription(
        person.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
        false,
        { name: "main", owner: person.$jazz.owner },
      );

      // Initial call with branch value
      expect(listener).toHaveBeenCalledTimes(1);
      expect(lastResult.get("age")).toEqual(25);

      // Update properties in the source (not the branch)
      person.$jazz.set("age", 31);
      person.$jazz.set("email", "john@example.com");

      // Listener should not be called since we're subscribed to the branch, not the source
      expect(listener).toHaveBeenCalledTimes(1);

      subscription.unsubscribe();
    });

    test("should handle rapid successive updates correctly", async () => {
      // Define a Person schema with score field
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        score: z.number(),
      });

      // Create a person that's immediately available
      const person = Person.create({ name: "John", age: 30, score: 100 });
      let lastResult: any = null;
      const listener = vi.fn();

      // Subscribe to the person
      const subscription = new CoValueCoreSubscription(
        person.$jazz.localNode,
        person.$jazz.id,
        (result) => {
          lastResult = result;
          listener(result);
        },
      );

      // Initial call with default values
      expect(listener).toHaveBeenCalledTimes(1);

      // Make rapid successive updates to test update handling
      person.$jazz.set("age", 31);
      person.$jazz.set("score", 150);
      person.$jazz.set("name", "Jane");
      person.$jazz.set("age", 32);

      expect(listener).toHaveBeenCalledTimes(5);

      // Check final state after all updates
      expect(lastResult.get("name")).toEqual("Jane");
      expect(lastResult.get("age")).toEqual(32);
      expect(lastResult.get("score")).toEqual(150);

      subscription.unsubscribe();
    });
  });
});
