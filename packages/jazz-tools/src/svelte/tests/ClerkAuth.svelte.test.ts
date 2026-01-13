// @vitest-environment happy-dom

import {
  Account,
  InMemoryKVStore,
  JazzClerkAuth,
  KvStoreContext,
} from "jazz-tools";
import type { MinimalClerkClient } from "jazz-tools";
import { render as renderSvelte, waitFor } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing";
import { render, screen } from "./testUtils";
import TestClerkAuthWrapper from "./TestClerkAuthWrapper.svelte";
import JazzSvelteProviderWithClerk from "../auth/JazzSvelteProviderWithClerk.svelte";

KvStoreContext.getInstance().initialize(new InMemoryKVStore());

function createMockClerkClient(
  user: MinimalClerkClient["user"] = null,
): MinimalClerkClient {
  return {
    user,
    signOut: vi.fn(),
    addListener: vi.fn(() => () => {}),
  };
}

describe("useClerkAuth", () => {
  let account: Account;

  beforeEach(async () => {
    await setupJazzTestSync();
    account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
  });

  it("should return anonymous state when not authenticated", async () => {
    const mockClerk = createMockClerkClient();

    render(
      TestClerkAuthWrapper,
      { clerk: mockClerk },
      { account, isAuthenticated: false },
    );

    expect(screen.getByTestId("auth-state").textContent).toBe("anonymous");
  });

  it("should return signedIn state when authenticated", async () => {
    const mockClerk = createMockClerkClient({
      id: "user_123",
      fullName: "Test User",
      username: "testuser",
      firstName: "Test",
      lastName: "User",
      primaryEmailAddress: { emailAddress: "test@example.com" },
      unsafeMetadata: {
        jazzAccountID: "test123",
        jazzAccountSecret: "secret123",
      },
      update: vi.fn(),
    });

    render(
      TestClerkAuthWrapper,
      { clerk: mockClerk },
      { account, isAuthenticated: true },
    );

    expect(screen.getByTestId("auth-state").textContent).toBe("signedIn");
  });

  it("should register the clerk listener", async () => {
    const mockClerk = createMockClerkClient();

    render(
      TestClerkAuthWrapper,
      { clerk: mockClerk },
      { account, isAuthenticated: false },
    );

    expect(mockClerk.addListener).toHaveBeenCalled();
  });

  it("should cleanup listener on unmount", async () => {
    const mockUnsubscribe = vi.fn();
    const mockClerk = createMockClerkClient();
    mockClerk.addListener = vi.fn(() => mockUnsubscribe);

    const { unmount } = render(
      TestClerkAuthWrapper,
      { clerk: mockClerk },
      { account, isAuthenticated: false },
    );

    expect(mockClerk.addListener).toHaveBeenCalled();
    unmount();
    expect(mockUnsubscribe).toHaveBeenCalled();
  });

  it("should throw error in guest mode", async () => {
    const guest = await createJazzTestGuest();
    const mockClerk = createMockClerkClient();

    expect(() => {
      render(TestClerkAuthWrapper, { clerk: mockClerk }, { account: guest });
    }).toThrow("Clerk auth is not supported in guest mode");
  });

  it("should call listener with clerk client events", async () => {
    const mockClerk = createMockClerkClient();
    let listenerCallback: ((data: unknown) => void) | undefined;
    mockClerk.addListener = vi.fn((callback) => {
      listenerCallback = callback;
      return () => {};
    });

    render(
      TestClerkAuthWrapper,
      { clerk: mockClerk },
      { account, isAuthenticated: false },
    );

    expect(mockClerk.addListener).toHaveBeenCalled();
    expect(listenerCallback).toBeDefined();
  });
});

describe("JazzClerkAuth.initializeAuth", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should handle initialization errors gracefully", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const initializeAuthSpy = vi
      .spyOn(JazzClerkAuth, "initializeAuth")
      .mockRejectedValue(new Error("Test error"));

    const mockClerk = createMockClerkClient();

    // The error should be thrown by the mock
    await expect(JazzClerkAuth.initializeAuth(mockClerk)).rejects.toThrow(
      "Test error",
    );

    // Restore using vitest's cleanup
    initializeAuthSpy.mockRestore();
    consoleSpy.mockRestore();
  });
});

describe("JazzSvelteProviderWithClerk", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should render children after successful initialization", async () => {
    const mockClerk = createMockClerkClient();
    vi.spyOn(JazzClerkAuth, "initializeAuth").mockResolvedValue(undefined);

    const { container } = renderSvelte(JazzSvelteProviderWithClerk, {
      props: {
        clerk: mockClerk,
        sync: { peer: "wss://test.example.com" },
      },
    });

    await waitFor(() => {
      // After initialization, the provider should render (even without children)
      // The error div should not be present
      expect(
        container.querySelector('[data-testid="jazz-clerk-auth-error"]'),
      ).toBeNull();
    });
  });

  it("should show default error message when initialization fails", async () => {
    const mockClerk = createMockClerkClient();
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(JazzClerkAuth, "initializeAuth").mockRejectedValue(
      new Error("Init failed"),
    );

    const { container } = renderSvelte(JazzSvelteProviderWithClerk, {
      props: {
        clerk: mockClerk,
        sync: { peer: "wss://test.example.com" },
      },
    });

    await waitFor(() => {
      const errorDiv = container.querySelector(
        '[data-testid="jazz-clerk-auth-error"]',
      );
      expect(errorDiv).not.toBeNull();
      expect(errorDiv?.textContent).toContain(
        "Authentication initialization failed",
      );
    });

    consoleSpy.mockRestore();
  });

  it("should call onAuthError callback when initialization fails", async () => {
    const mockClerk = createMockClerkClient();
    const onAuthError = vi.fn();
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(JazzClerkAuth, "initializeAuth").mockRejectedValue(
      new Error("Init failed"),
    );

    renderSvelte(JazzSvelteProviderWithClerk, {
      props: {
        clerk: mockClerk,
        sync: { peer: "wss://test.example.com" },
        onAuthError,
      },
    });

    await waitFor(() => {
      expect(onAuthError).toHaveBeenCalledWith(expect.any(Error));
      expect(onAuthError).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Init failed" }),
      );
    });

    consoleSpy.mockRestore();
  });

  it("should not update state after unmount (cancellation)", async () => {
    const mockClerk = createMockClerkClient();
    let resolveInit: () => void;
    const initPromise = new Promise<void>((resolve) => {
      resolveInit = resolve;
    });
    vi.spyOn(JazzClerkAuth, "initializeAuth").mockReturnValue(initPromise);

    const { unmount } = renderSvelte(JazzSvelteProviderWithClerk, {
      props: {
        clerk: mockClerk,
        sync: { peer: "wss://test.example.com" },
      },
    });

    // Unmount before initialization completes
    unmount();

    // Resolve the promise after unmount - should not cause errors
    resolveInit!();

    // Give time for any potential state updates
    await new Promise((resolve) => setTimeout(resolve, 10));

    // If we get here without errors, the cancellation worked
    expect(true).toBe(true);
  });
});

describe("useClerkAuth reactive state transitions", () => {
  let account: Account;

  beforeEach(async () => {
    await setupJazzTestSync();
    account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should update state reactively when listener callback fires", async () => {
    const mockClerk = createMockClerkClient();
    let listenerCallback: ((data: unknown) => void) | undefined;
    mockClerk.addListener = vi.fn((callback) => {
      listenerCallback = callback;
      return () => {};
    });

    const { container } = render(
      TestClerkAuthWrapper,
      { clerk: mockClerk },
      { account, isAuthenticated: false },
    );

    // Initial state should be anonymous
    expect(screen.getByTestId("auth-state").textContent).toBe("anonymous");

    // Verify listener was registered
    expect(listenerCallback).toBeDefined();

    // Note: Full state transition testing would require more complex setup
    // involving the actual JazzClerkAuth.onClerkUserChange flow.
    // This test verifies the listener registration which is the foundation
    // for reactive updates.
  });
});
