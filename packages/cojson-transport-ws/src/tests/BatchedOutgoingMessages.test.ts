import { cojsonInternals, type SyncMessage } from "cojson";
import { type Mocked, afterEach, describe, expect, test, vi } from "vitest";
import { BatchedOutgoingMessages } from "../BatchedOutgoingMessages";
import type { AnyWebSocket } from "../types";
import { createTestMetricReader, tearDownTestMetricReader } from "./utils.js";

const { CO_VALUE_PRIORITY } = cojsonInternals;

function createMockWebSocket(
  overrides: Partial<AnyWebSocket> = {},
): Mocked<AnyWebSocket> {
  return {
    readyState: 1,
    bufferedAmount: 0,
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    close: vi.fn(),
    send: vi.fn(),
    ...overrides,
  } as unknown as Mocked<AnyWebSocket>;
}

function createTestMessage(
  priority: number = CO_VALUE_PRIORITY.HIGH,
): SyncMessage {
  return {
    action: "content",
    id: `co_ztest${priority}`,
    new: {},
    priority,
  } as SyncMessage;
}

describe("BatchedOutgoingMessages", () => {
  describe("telemetry", () => {
    afterEach(() => {
      tearDownTestMetricReader();
    });

    test("should correctly measure egress", async () => {
      const metricReader = createTestMetricReader();

      const mockWebSocket = {
        readyState: 1,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        close: vi.fn(),
        send: vi.fn(),
      } as unknown as Mocked<AnyWebSocket>;

      const outgoing = new BatchedOutgoingMessages(
        mockWebSocket,
        true,
        "server",
        { test: "test" },
      );

      const encryptedChanges = "Hello, world!";
      vi.useFakeTimers();
      outgoing.push({
        action: "content",
        new: {
          someSessionId: {
            newTransactions: [
              {
                privacy: "private",
                encryptedChanges,
              },
            ],
          },
        },
      } as unknown as SyncMessage);

      await vi.runAllTimersAsync();
      vi.useRealTimers();

      expect(
        await metricReader.getMetricValue("jazz.usage.egress", {
          test: "test",
        }),
      ).toBe(encryptedChanges.length);

      const trustingChanges = "Jazz is great!";
      vi.useFakeTimers();
      outgoing.push({
        action: "content",
        new: {
          someSessionId: {
            after: 0,
            newTransactions: [
              {
                privacy: "trusting",
                changes: trustingChanges,
              },
            ],
          },
        },
      } as unknown as SyncMessage);

      await vi.runAllTimersAsync();
      vi.useRealTimers();

      expect(
        await metricReader.getMetricValue("jazz.usage.egress", {
          test: "test",
        }),
      ).toBe(encryptedChanges.length + trustingChanges.length);
    });
  });

  describe("queue push/pull metrics", () => {
    afterEach(() => {
      tearDownTestMetricReader();
    });

    test("fast path: should send directly and not use queue when WebSocket is ready", async () => {
      const metricReader = createTestMetricReader();
      const mockWebSocket = createMockWebSocket({
        readyState: 1, // OPEN
        bufferedAmount: 0,
      });

      const outgoing = new BatchedOutgoingMessages(
        mockWebSocket,
        false, // disable batching for simpler testing
        "client",
      );

      // Push a message when WebSocket is ready
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.HIGH));

      // Message should be sent directly
      expect(mockWebSocket.send).toHaveBeenCalledTimes(1);

      // Fast path: pushed and pulled should be equal
      const pushed = await metricReader.getMetricValue(
        "jazz.messagequeue.outgoing.pushed",
        { priority: CO_VALUE_PRIORITY.HIGH, peerRole: "client" },
      );
      const pulled = await metricReader.getMetricValue(
        "jazz.messagequeue.outgoing.pulled",
        { priority: CO_VALUE_PRIORITY.HIGH, peerRole: "client" },
      );
      expect(pushed).toEqual(pulled);
      expect(pushed).toBe(1);
    });

    test("slow path: should push to queue when WebSocket is not ready", async () => {
      const metricReader = createTestMetricReader();
      const mockWebSocket = createMockWebSocket({
        readyState: 0, // CONNECTING
        bufferedAmount: 0,
      });

      const outgoing = new BatchedOutgoingMessages(
        mockWebSocket,
        false,
        "client",
      );

      // Push a message when WebSocket is not ready (will be queued)
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.HIGH));

      // Slow path should push to queue
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pushed", {
          priority: CO_VALUE_PRIORITY.HIGH,
          peerRole: "client",
        }),
      ).toBe(1);

      // Message should NOT be sent (WebSocket not open)
      expect(mockWebSocket.send).not.toHaveBeenCalled();
    });

    test("fast path: multiple sends should not accumulate queue backlog", async () => {
      const metricReader = createTestMetricReader();
      const mockWebSocket = createMockWebSocket({
        readyState: 1,
        bufferedAmount: 0,
      });

      const outgoing = new BatchedOutgoingMessages(
        mockWebSocket,
        false,
        "client",
      );

      // Send multiple messages via fast path
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.HIGH));
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.HIGH));
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.HIGH));

      // All messages should be sent directly
      expect(mockWebSocket.send).toHaveBeenCalledTimes(3);

      // Queue metrics should be balanced for fast-path sends
      const pushed = await metricReader.getMetricValue(
        "jazz.messagequeue.outgoing.pushed",
        { priority: CO_VALUE_PRIORITY.HIGH, peerRole: "client" },
      );
      const pulled = await metricReader.getMetricValue(
        "jazz.messagequeue.outgoing.pulled",
        { priority: CO_VALUE_PRIORITY.HIGH, peerRole: "client" },
      );

      expect(pushed).toBe(pulled);
      expect(pushed).toBe(3);
    });

    test("should queue second message when first is being processed", async () => {
      const metricReader = createTestMetricReader();
      const mockWebSocket = createMockWebSocket({
        readyState: 0, // CONNECTING - will trigger slow path
        bufferedAmount: 0,
      });

      const outgoing = new BatchedOutgoingMessages(
        mockWebSocket,
        false,
        "client",
      );

      // First message starts async processing (slow path)
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.HIGH));

      // Second message should be queued because processing is in progress
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.MEDIUM));

      // Both should be pushed to queue
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pushed", {
          priority: CO_VALUE_PRIORITY.HIGH,
          peerRole: "client",
        }),
      ).toBe(1);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pushed", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          peerRole: "client",
        }),
      ).toBe(1);
    });

    test("should discard messages pushed after close to prevent push/pull mismatch", async () => {
      const metricReader = createTestMetricReader();
      const mockWebSocket = createMockWebSocket({
        readyState: 1, // OPEN
        bufferedAmount: 0,
      });

      const outgoing = new BatchedOutgoingMessages(
        mockWebSocket,
        false,
        "client",
      );

      // Send a message normally
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.HIGH));
      expect(mockWebSocket.send).toHaveBeenCalledTimes(1);

      // Close the channel
      outgoing.close();

      // Try to send more messages after close
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.HIGH));
      outgoing.push(createTestMessage(CO_VALUE_PRIORITY.MEDIUM));

      // Messages after close should be discarded, not sent
      expect(mockWebSocket.send).toHaveBeenCalledTimes(1);

      // Queue metrics should be balanced (no messages queued after close)
      const pushedHigh = await metricReader.getMetricValue(
        "jazz.messagequeue.outgoing.pushed",
        { priority: CO_VALUE_PRIORITY.HIGH, peerRole: "client" },
      );
      const pulledHigh = await metricReader.getMetricValue(
        "jazz.messagequeue.outgoing.pulled",
        { priority: CO_VALUE_PRIORITY.HIGH, peerRole: "client" },
      );
      const pushedMedium = await metricReader.getMetricValue(
        "jazz.messagequeue.outgoing.pushed",
        { priority: CO_VALUE_PRIORITY.MEDIUM, peerRole: "client" },
      );
      const pulledMedium = await metricReader.getMetricValue(
        "jazz.messagequeue.outgoing.pulled",
        { priority: CO_VALUE_PRIORITY.MEDIUM, peerRole: "client" },
      );

      // All queues should be balanced
      expect(pushedHigh).toBe(pulledHigh);
      expect(pushedMedium).toBe(pulledMedium);
    });
  });
});
