import { afterEach, describe, expect, test } from "vitest";
import {
  PriorityBasedMessageQueue,
  meteredQueue,
} from "../PriorityBasedMessageQueue.js";
import { CO_VALUE_PRIORITY } from "../priority.js";
import type { SyncMessage } from "../sync.js";
import {
  createTestMetricReader,
  tearDownTestMetricReader,
} from "./testUtils.js";

function setup() {
  const queue = new PriorityBasedMessageQueue(CO_VALUE_PRIORITY.MEDIUM);
  return { queue };
}

function setupMeteredQueue(attrs?: Record<string, string>) {
  const metricReader = createTestMetricReader();

  const { queue: unmeteredQueue } = setup();
  const queue = meteredQueue(unmeteredQueue, attrs);
  return { queue, metricReader };
}

describe("PriorityBasedMessageQueue", () => {
  afterEach(() => {
    tearDownTestMetricReader();
  });

  describe("meteredQueue", () => {
    test("should corretly count pushes", async () => {
      const { queue, metricReader } = setupMeteredQueue();
      const message: SyncMessage = {
        action: "load",
        id: "co_ztest-id",
        header: false,
        sessions: {},
      };

      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pushed", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
        }),
      ).toBeUndefined();

      void queue.push(message);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pushed", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
        }),
      ).toBe(1);

      void queue.push(message);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pushed", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
        }),
      ).toBe(2);
    });

    test("should corretly count pulls", async () => {
      const { queue, metricReader } = setupMeteredQueue();
      const message: SyncMessage = {
        action: "load",
        id: "co_ztest-id",
        header: false,
        sessions: {},
      };

      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pulled", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
        }),
      ).toBeUndefined();

      void queue.push(message);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pulled", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
        }),
      ).toBeUndefined();

      void queue.pull();

      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pulled", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
        }),
      ).toBe(1);

      // We only have one item in the queue, so this should not change the metric value
      void queue.pull();
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pulled", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
        }),
      ).toBe(1);
    });

    test("should corretly set custom attributes to the metrics", async () => {
      const { queue, metricReader } = setupMeteredQueue({ role: "server" });
      const message: SyncMessage = {
        action: "load",
        id: "co_ztest-id",
        header: false,
        sessions: {},
      };

      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pushed", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          role: "server",
        }),
      ).toBeUndefined();
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pushed", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          role: "client",
        }),
      ).toBeUndefined();

      void queue.push(message);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pushed", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          role: "server",
        }),
      ).toBe(1);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pulled", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          role: "server",
        }),
      ).toBeUndefined();

      void queue.pull();

      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pushed", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          role: "server",
        }),
      ).toBe(1);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pulled", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          role: "server",
        }),
      ).toBe(1);
    });
  });

  test("should push message with default priority", async () => {
    const { queue } = setup();
    const message: SyncMessage = {
      action: "load",
      id: "co_ztest-id",
      header: false,
      sessions: {},
    };
    void queue.push(message);
    const pulledEntry = queue.pull();
    expect(pulledEntry?.entry.msg).toEqual(message);
  });

  test("should push message with specified priority", async () => {
    const { queue } = setup();
    const message: SyncMessage = {
      action: "content",
      id: "co_zhigh",
      new: {},
      priority: CO_VALUE_PRIORITY.HIGH,
    };
    void queue.push(message);
    const pulledEntry = queue.pull();
    expect(pulledEntry?.entry.msg).toEqual(message);
  });

  test("should pull messages in priority order", async () => {
    const { queue } = setup();
    const lowPriorityMsg: SyncMessage = {
      action: "content",
      id: "co_zlow",
      new: {},
      priority: CO_VALUE_PRIORITY.LOW,
    };
    const mediumPriorityMsg: SyncMessage = {
      action: "content",
      id: "co_zmedium",
      new: {},
      priority: CO_VALUE_PRIORITY.MEDIUM,
    };
    const highPriorityMsg: SyncMessage = {
      action: "content",
      id: "co_zhigh",
      new: {},
      priority: CO_VALUE_PRIORITY.HIGH,
    };

    void queue.push(lowPriorityMsg);
    void queue.push(mediumPriorityMsg);
    void queue.push(highPriorityMsg);

    expect(queue.pull()?.entry.msg).toEqual(highPriorityMsg);
    expect(queue.pull()?.entry.msg).toEqual(mediumPriorityMsg);
    expect(queue.pull()?.entry.msg).toEqual(lowPriorityMsg);
  });

  test("should return undefined when pulling from empty queue", () => {
    const { queue } = setup();
    expect(queue.pull()).toBeUndefined();
  });

  test("should resolve promise when message is pulled", async () => {
    const { queue } = setup();
    const message: SyncMessage = {
      action: "load",
      id: "co_ztest-id",
      header: false,
      sessions: {},
    };
    const pushPromise = queue.push(message);

    const pulledEntry = queue.pull();
    pulledEntry?.entry.resolve();

    await expect(pushPromise).resolves.toBeUndefined();
  });

  test("should reject promise when message is rejected", async () => {
    const { queue } = setup();
    const message: SyncMessage = {
      action: "load",
      id: "co_ztest-id",
      header: false,
      sessions: {},
    };
    const pushPromise = queue.push(message);

    const pulledEntry = queue.pull();
    pulledEntry?.entry.reject(new Error("Test error"));

    await expect(pushPromise).rejects.toThrow("Test error");
  });
});
