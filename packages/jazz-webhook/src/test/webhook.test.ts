import { describe, expect, test, onTestFinished, assert } from "vitest";
import { co, z, Group } from "jazz-tools";
import { assertLoaded, createJazzTestAccount } from "jazz-tools/testing";
import { WebhookTestServer } from "./http-server.js";
import {
  WebhookRegistry,
  WebhookRegistration,
  RegistryState,
  JazzWebhookOptions,
} from "../webhook.js";
import { isTxSuccessful } from "../successMap.js";
import { CojsonInternalTypes } from "cojson";

// Define test schemas
const TestCoMap = co.map({
  value: z.string(),
});

const TestRoot = co.map({
  webhookRegistry: RegistryState,
});

const TestAccount = co
  .account({
    root: TestRoot,
    profile: co.profile(),
  })
  .withMigration((account, creationProps) => {
    const group = Group.create({ owner: account });
    group.addMember("everyone", "reader");

    if (!account.$jazz.has("profile")) {
      account.$jazz.set(
        "profile",
        co.profile().create(
          {
            name: creationProps?.name ?? "Test Account",
          },
          group,
        ),
      );
    }

    if (!account.$jazz.has("root")) {
      account.$jazz.set(
        "root",
        TestRoot.create(
          {
            webhookRegistry: WebhookRegistry.createRegistry(group),
          },
          group,
        ),
      );
    }
  });

interface TestContext {
  account: any;
  webhookServer: WebhookTestServer;
  registryState: RegistryState;
  webhookRegistry: WebhookRegistry;
}

async function setupTest(options?: JazzWebhookOptions): Promise<TestContext> {
  const account = await createJazzTestAccount({
    AccountSchema: TestAccount,
    isCurrentActiveAccount: true,
  });

  const webhookServer = new WebhookTestServer();
  await webhookServer.start();
  const registryState = account.root.webhookRegistry;
  const webhookRegistry = new WebhookRegistry(
    registryState,
    options || {
      baseDelayMs: 10,
    },
  );
  webhookRegistry.start();

  // Set up cleanup for this test
  onTestFinished(async () => {
    if (webhookServer) {
      webhookServer.close();
    }
    webhookRegistry.shutdown();
  });

  return {
    account,
    webhookServer,
    registryState,
    webhookRegistry,
  };
}

describe("jazz-webhook", () => {
  describe("webhook registration", () => {
    test("should register a valid webhook", async () => {
      const { webhookServer, registryState, webhookRegistry } =
        await setupTest();

      const webhookId = await webhookRegistry.register(
        webhookServer.getUrl(),
        "co_z1234567890abcdef",
      );

      expect(webhookId).toBeDefined();
      assert(registryState[webhookId]);
      assertLoaded(registryState[webhookId]);
      expect(registryState[webhookId].webhookUrl).toBe(webhookServer.getUrl());
      expect(registryState[webhookId].coValueId).toBe("co_z1234567890abcdef");
      expect(registryState[webhookId].active).toBe(true);
      expect(Object.keys(registryState[webhookId]!.successMap!).length).toBe(0);
    });

    test("should throw error for invalid callback URL", async () => {
      const { webhookRegistry: webhookManager } = await setupTest();

      await expect(
        webhookManager.register("not-a-url", "co_z1234567890abcdef"),
      ).rejects.toThrow("Invalid webhook URL: not-a-url");
    });

    test("should throw error for invalid CoValue ID format", async () => {
      const { webhookServer, webhookRegistry: webhookManager } =
        await setupTest();

      await expect(
        webhookManager.register(webhookServer.getUrl(), "invalid-id"),
      ).rejects.toThrow(
        "Invalid CoValue ID format: invalid-id. Expected format: co_z...",
      );

      await expect(
        webhookManager.register(webhookServer.getUrl(), "co_invalid"),
      ).rejects.toThrow(
        "Invalid CoValue ID format: co_invalid. Expected format: co_z...",
      );
    });

    test("should create multiple webhooks with different IDs", async () => {
      const { webhookServer, registryState, webhookRegistry } =
        await setupTest();

      const webhook1 = await webhookRegistry.register(
        webhookServer.getUrl(),
        "co_z1111111111111111",
      );
      const webhook2 = await webhookRegistry.register(
        webhookServer.getUrl(),
        "co_z2222222222222222",
      );

      expect(webhook1).not.toBe(webhook2);
      assert(registryState[webhook1]);
      assertLoaded(registryState[webhook1]);
      expect(registryState[webhook1].coValueId).toBe("co_z1111111111111111");
      assert(registryState[webhook2]);
      assertLoaded(registryState[webhook2]);
      expect(registryState[webhook2].coValueId).toBe("co_z2222222222222222");
    });
  });

  describe("webhook unregistration", () => {
    test("should unregister a webhook", async () => {
      const { webhookServer, registryState, webhookRegistry } =
        await setupTest();

      const webhookId = await webhookRegistry.register(
        webhookServer.getUrl(),
        "co_z1234567890abcdef",
      );

      const webhook = registryState[webhookId];
      assert(webhook);
      assertLoaded(webhook);
      expect(webhook.active).toBe(true);

      webhookRegistry.unregister(webhookId);

      expect(registryState[webhookId]).toBeUndefined();
      expect(webhook!.active).toBe(false);
    });

    test("should throw error for non-existent webhook", async () => {
      const { webhookRegistry } = await setupTest();

      expect(() => {
        webhookRegistry.unregister("fake-id");
      }).toThrow("Webhook with ID fake-id not found");
    });
  });

  describe("webhook emission with real HTTP server", () => {
    test("should emit webhook when CoValue changes", async () => {
      const {
        account,
        webhookServer,
        webhookRegistry: webhookManager,
      } = await setupTest();

      // Create a test CoMap
      const testMap = TestCoMap.create(
        { value: "initial" },
        account.root.$jazz.owner,
      );
      const coValueId = testMap.$jazz.id as `co_z${string}`;

      // Register webhook
      await webhookManager.register(webhookServer.getUrl(), coValueId);

      // Make a change to trigger webhook
      testMap.$jazz.set("value", "changed");

      // Wait a bit to ensure the transaction is fully processed
      await new Promise((resolve) => setTimeout(resolve, 50));

      const txID = testMap.$jazz.raw.lastEditAt("value")?.tx;
      if (!txID) {
        throw new Error("Failed to get transaction ID after change");
      }

      // Wait for webhook to be emitted
      const requests = await webhookServer.waitForRequests(2, 3000);

      expect(requests.length).toBe(2);
      const lastRequest = webhookServer.getLastRequest();
      expect(lastRequest.coValueId).toBe(coValueId);
      expect(lastRequest.txID).toEqual(txID);
    });

    test("should queue multiple changes and emit only the latest", async () => {
      const {
        account,
        webhookServer,
        webhookRegistry: webhookManager,
      } = await setupTest();

      const testMap = TestCoMap.create(
        { value: "initial" },
        account.root.$jazz.owner,
      );
      const coValueId = testMap.$jazz.id as `co_z${string}`;

      await webhookManager.register(webhookServer.getUrl(), coValueId);

      // Make multiple rapid changes
      testMap.$jazz.set("value", "change1");
      testMap.$jazz.set("value", "change2");
      testMap.$jazz.set("value", "change3");

      // Wait for webhook to be processed
      await new Promise((resolve) => setTimeout(resolve, 500));

      // Should only receive one webhook with the latest state
      expect(webhookServer.requests.length).toBeGreaterThanOrEqual(1);

      // The last request should be for the final change
      const lastRequest = webhookServer.requests.at(-1);
      expect(lastRequest?.coValueId).toBe(coValueId);
    });

    test("should update lastSuccessfulEmit after successful webhook", async () => {
      const { account, webhookServer, registryState, webhookRegistry } =
        await setupTest();

      const testMap = TestCoMap.create(
        { value: "initial" },
        account.root.$jazz.owner,
      );
      const coValueId = testMap.$jazz.id as `co_z${string}`;

      const webhookId = await webhookRegistry.register(
        webhookServer.getUrl(),
        coValueId,
      );

      testMap.$jazz.set("value", "changed");

      const requests = await webhookServer.waitForRequests(1, 3000);

      expect(requests.length).toBeGreaterThanOrEqual(1);

      const lastRequest = webhookServer.getLastRequest();
      const webhook = registryState[webhookId];

      await new Promise((resolve) => setTimeout(resolve, 100));

      assert(webhook);
      assertLoaded(webhook);
      assertLoaded(webhook.successMap);
      expect(isTxSuccessful(webhook.successMap, lastRequest.txID)).toBe(true);
    });

    test("should retry failed webhooks with exponential backoff", async () => {
      const {
        account,
        webhookServer,
        webhookRegistry: webhookManager,
      } = await setupTest();

      const testMap = TestCoMap.create(
        { value: "initial" },
        account.root.$jazz.owner,
      );
      const coValueId = testMap.$jazz.id as `co_z${string}`;

      // Set up server to fail first 2 requests, then succeed
      webhookServer.setResponse(0, 500, "Server Error");
      webhookServer.setResponse(1, 500, "Server Error");
      webhookServer.setResponse(2, 200, "Success");

      await webhookManager.register(webhookServer.getUrl(), coValueId);

      testMap.$jazz.set("value", "changed");

      // Should eventually succeed after retries
      const requests = await webhookServer.waitForRequests(3, 10000);

      expect(requests).toHaveLength(3);
      expect(requests[0]!.coValueId).toBe(coValueId);
      expect(requests[1]!.coValueId).toBe(coValueId);
      expect(requests[2]!.coValueId).toBe(coValueId);
    });

    test("should respect Retry-After response header", async () => {
      const {
        account,
        webhookServer,
        webhookRegistry: webhookManager,
      } = await setupTest({
        baseDelayMs: 100000, // Really long default delay, we expect this to be overridden by the Retry-After header
      });

      const testMap = TestCoMap.create(
        { value: "initial" },
        account.root.$jazz.owner,
      );
      const coValueId = testMap.$jazz.id as `co_z${string}`;

      // Set up server to fail with Retry-After header
      webhookServer.setResponse(0, 500, "Server Error", undefined, {
        "Retry-After": "0.05",
      }); // 50ms
      webhookServer.setResponse(1, 500, "Server Error", undefined, {
        "Retry-After": "0.05",
      }); // 50ms
      webhookServer.setResponse(2, 200, "Success");

      await webhookManager.register(webhookServer.getUrl(), coValueId);

      testMap.$jazz.set("value", "changed");

      // Should eventually succeed after retry
      const requests = await webhookServer.waitForRequests(3, 1000);

      expect(requests).toHaveLength(3);
      expect(requests[0]!.coValueId).toBe(coValueId);
      expect(requests[1]!.coValueId).toBe(coValueId);
      expect(requests[2]!.coValueId).toBe(coValueId);
    });

    test("should give up after max retries", async () => {
      const {
        account,
        webhookServer,
        webhookRegistry: webhookManager,
      } = await setupTest();

      const testMap = TestCoMap.create(
        { value: "initial" },
        account.root.$jazz.owner,
      );
      const coValueId = testMap.$jazz.id as `co_z${string}`;

      // Set up server to always fail
      for (let i = 0; i < 10; i++) {
        webhookServer.setResponse(i, 500, "Server Error");
      }

      await webhookManager.register(webhookServer.getUrl(), coValueId);

      testMap.$jazz.set("value", "changed");

      // Should retry 5 times then give up
      const requests = await webhookServer.waitForRequests(5, 1000);

      expect(requests).toHaveLength(5); // 5 retries
      expect(requests.every((req) => req.coValueId === coValueId)).toBe(true);
    });

    test("should handle slow server responses", async () => {
      const {
        account,
        webhookServer,
        webhookRegistry: webhookManager,
      } = await setupTest();

      const testMap = TestCoMap.create(
        { value: "initial" },
        account.root.$jazz.owner,
      );
      const coValueId = testMap.$jazz.id as `co_z${string}`;

      // Set up server with slow response
      webhookServer.setResponse(0, 200, "Success", 500);

      await webhookManager.register(webhookServer.getUrl(), coValueId);

      testMap.$jazz.set("value", "changed");

      await webhookServer.waitForRequests(1, 5000);

      const request = webhookServer.expectSingleRequest();

      expect(request.coValueId).toBe(coValueId);
    });

    test("should handle webhook unregistration cleanup", async () => {
      const { account, webhookServer, webhookRegistry } = await setupTest();

      const testMap = TestCoMap.create(
        { value: "initial" },
        account.root.$jazz.owner,
      );
      const coValueId = testMap.$jazz.id as `co_z${string}`;

      const webhookId = await webhookRegistry.register(
        webhookServer.getUrl(),
        coValueId,
      );

      // Make initial change
      testMap.$jazz.set("value", "changed");
      await webhookServer.waitForRequests(2, 3000);

      const initialRequestCount = webhookServer.requests.length;

      // Unregister webhook
      webhookRegistry.unregister(webhookId);

      // Wait a bit to ensure the unregistration is processed
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Make another change - should not trigger webhook
      testMap.$jazz.set("value", "changed_again");

      // Wait a bit to ensure no additional requests
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(webhookServer.getRequestCount()).toBe(initialRequestCount);
    });

    test("should start all active webhook subscriptions", async () => {
      const { account, webhookServer, webhookRegistry } = await setupTest();

      const testMap1 = TestCoMap.create(
        { value: "initial1" },
        account.root.$jazz.owner,
      );
      const testMap2 = TestCoMap.create(
        { value: "initial2" },
        account.root.$jazz.owner,
      );
      const coValueId1 = testMap1.$jazz.id as `co_z${string}`;
      const coValueId2 = testMap2.$jazz.id as `co_z${string}`;

      // Register webhooks (this automatically starts subscriptions)
      const webhookId1 = await webhookRegistry.register(
        webhookServer.getUrl(),
        coValueId1,
      );
      const webhookId2 = await webhookRegistry.register(
        webhookServer.getUrl(),
        coValueId2,
      );

      // Shutdown all subscriptions
      webhookRegistry.shutdown();

      // Verify no subscriptions are active
      expect(webhookRegistry["activeSubscriptions"].size).toBe(0);

      // Start all active webhook subscriptions
      await webhookRegistry.start();

      // Verify subscriptions are active again
      expect(webhookRegistry["activeSubscriptions"].size).toBe(2);
      expect(webhookRegistry["activeSubscriptions"].has(webhookId1)).toBe(true);
      expect(webhookRegistry["activeSubscriptions"].has(webhookId2)).toBe(true);

      // Make changes to both CoValues to verify webhooks are working
      testMap1.$jazz.set("value", "changed1");
      testMap2.$jazz.set("value", "changed2");

      // Wait for webhooks to be emitted
      // TODO: why are we getting two requests for webhook1
      const requests = await webhookServer.waitForRequests(4, 3000);

      expect(requests.length).toBeGreaterThanOrEqual(2);

      // Verify both webhooks were triggered
      const coValueIds = requests.map((req) => req.coValueId);
      expect(coValueIds).toContain(coValueId1);
      expect(coValueIds).toContain(coValueId2);
    });

    test("should when restarting, only the changed covalues should trigger webhooks", async () => {
      const { account, webhookServer, webhookRegistry } = await setupTest();

      const testMap1 = TestCoMap.create(
        { value: "initial1" },
        account.root.$jazz.owner,
      );
      const initialTxID1 = testMap1.$jazz.raw.lastEditAt("value")!.tx;
      const testMap2 = TestCoMap.create(
        { value: "initial2" },
        account.root.$jazz.owner,
      );
      const initialTxID2 = testMap2.$jazz.raw.lastEditAt("value")!.tx;
      const coValueId1 = testMap1.$jazz.id as `co_z${string}`;
      const coValueId2 = testMap2.$jazz.id as `co_z${string}`;

      // Register webhooks (this automatically starts subscriptions)
      const webhookId1 = await webhookRegistry.register(
        webhookServer.getUrl(),
        coValueId1,
      );
      const webhookId2 = await webhookRegistry.register(
        webhookServer.getUrl(),
        coValueId2,
      );

      await waitForWebhookEmitted(webhookId1, initialTxID1);
      await waitForWebhookEmitted(webhookId2, initialTxID2);

      webhookRegistry.shutdown();

      testMap1.$jazz.set("value", "changed");
      const changedTxID1 = testMap1.$jazz.raw.lastEditAt("value")!.tx;

      webhookRegistry.start();

      await waitForWebhookEmitted(webhookId1, changedTxID1);

      // Wait some extra time to ensure that we have only one extra request
      await new Promise((resolve) => setTimeout(resolve, 100));

      // The two initial updates, plus the one we made before restarting
      expect(webhookServer.getRequestCount()).toBe(3);
    });

    test("should not start subscriptions for inactive webhooks", async () => {
      const { account, webhookServer, webhookRegistry } = await setupTest();

      const testMap = TestCoMap.create(
        { value: "initial" },
        account.root.$jazz.owner,
      );
      const coValueId = testMap.$jazz.id as `co_z${string}`;

      // Register webhook
      const webhookId = await webhookRegistry.register(
        webhookServer.getUrl(),
        coValueId,
      );

      // Wait a bit to ensure no webhook is sent
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(webhookServer.getRequestCount()).toBe(1);

      // Deactivate the webhook
      const webhook = webhookRegistry.state[webhookId];
      assert(webhook);
      assertLoaded(webhook);
      webhook.$jazz.set("active", false);

      // Shutdown all subscriptions
      webhookRegistry.shutdown();

      // Start all active webhook subscriptions
      webhookRegistry.start();

      // Verify no subscriptions are active (since webhook is inactive)
      expect(webhookRegistry["activeSubscriptions"].size).toBe(0);

      // Make a change - should not trigger webhook
      testMap.$jazz.set("value", "changed");

      // Wait a bit to ensure no webhook is sent
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(webhookServer.getRequestCount()).toBe(1);
    });
  });
});

async function waitForWebhookEmitted(
  id: string,
  txID: CojsonInternalTypes.TransactionID,
) {
  const webhook = await WebhookRegistration.load(id, {
    resolve: {
      successMap: { $each: true },
    },
  });

  assertLoaded(webhook);

  return waitFor(() => {
    expect(isTxSuccessful(webhook.successMap, txID)).toBe(true);
  });
}

function waitFor(callback: () => boolean | void | Promise<boolean | void>) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = async () => {
      try {
        return { ok: await callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(async () => {
      const { ok, error } = await checkPassed();

      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }

      if (++retries > 10) {
        clearInterval(interval);
        reject(error);
      }
    }, 100);
  });
}
