// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createJazzTestAccount,
  setupJazzTestSync,
  MockConnectionStatus,
} from "../testing";
import { render, screen, waitFor } from "./testUtils";
import TestConnectionStatus from "./TestConnectionStatus.svelte";

describe("SyncConnectionStatus", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("should return true by default in the test environment", async () => {
    const { container } = render(TestConnectionStatus, {}, {
      account: await createJazzTestAccount({
        isCurrentActiveAccount: true,
      }),
    });

    await waitFor(() => {
      expect(screen.getByTestId("connected").textContent).toBe("true");
    });
  });

  it("should handle updates", async () => {
    const { container } = render(TestConnectionStatus, {}, {
      account: await createJazzTestAccount({
        isCurrentActiveAccount: true,
      }),
    });

    // Initially should be connected
    await waitFor(() => {
      expect(screen.getByTestId("connected").textContent).toBe("true");
    });

    // Simulate disconnection
    MockConnectionStatus.setIsConnected(false);
    
    await waitFor(() => {
      expect(screen.getByTestId("connected").textContent).toBe("false");
    });

    // Simulate reconnection
    MockConnectionStatus.setIsConnected(true);
    
    await waitFor(() => {
      expect(screen.getByTestId("connected").textContent).toBe("true");
    });
  });
});
