import type { SyncMessage } from "cojson";
import { type Mocked, afterEach, describe, expect, test, vi } from "vitest";
import { BatchedOutgoingMessages } from "../BatchedOutgoingMessages";
import type { AnyWebSocket } from "../types";
import { createTestMetricReader, tearDownTestMetricReader } from "./utils.js";

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
});
