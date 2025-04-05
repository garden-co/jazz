import { afterEach, describe, expect, test } from "vitest";
import { PriorityBasedMessageQueue } from "../PriorityBasedMessageQueue.js";
import { CO_VALUE_PRIORITY } from "../priority.js";
import type { SyncMessage } from "../sync.js";
import {
  createTestMetricReader,
  tearDownTestMetricReader,
} from "./testUtils.js";

function setup(attrs?: Record<string, string | number>) {
  const metricReader = createTestMetricReader();
  const queue = new PriorityBasedMessageQueue(CO_VALUE_PRIORITY.MEDIUM, attrs);
  return { queue, metricReader };
}

describe("PriorityBasedMessageQueue", () => {
  afterEach(() => {
    tearDownTestMetricReader();
  });

  describe("meteredQueue", () => {
    test("should corretly count pushes", async () => {
      const { queue, metricReader } = setup();
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
      ).toBe(0);

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
      const { queue, metricReader } = setup();
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
      ).toBe(0);

      void queue.push(message);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.pulled", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
        }),
      ).toBe(0);

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
      const { queue, metricReader } = setup({ role: "server" });
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
      ).toBe(0);
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
      ).toBe(0);

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

  test("should initialize with correct properties", () => {
    const { queue } = setup();
    expect(queue["defaultPriority"]).toBe(CO_VALUE_PRIORITY.MEDIUM);
    expect(queue["queues"].length).toBe(3);
    expect(queue["queues"].every((q) => !q.length)).toBe(true);
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
    expect(pulledEntry?.msg).toEqual(message);
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
    expect(pulledEntry?.msg).toEqual(message);
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

    expect(queue.pull()?.msg).toEqual(highPriorityMsg);
    expect(queue.pull()?.msg).toEqual(mediumPriorityMsg);
    expect(queue.pull()?.msg).toEqual(lowPriorityMsg);
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
    pulledEntry?.resolve();

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
    pulledEntry?.reject(new Error("Test error"));

    await expect(pushPromise).rejects.toThrow("Test error");
  });
});
