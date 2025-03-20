// @vitest-environment happy-dom

import { act, render } from "@testing-library/react";
import { JazzClerkAuth, type MinimalClerkClient } from "jazz-auth-clerk";
import { AuthSecretStorage, InMemoryKVStore, KvStoreContext } from "jazz-tools";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { JazzProviderWithClerk } from "../index";

vi.mock("jazz-react", async (importOriginal) => {
  const { JazzTestProvider, createJazzTestAccount } = await import(
    "jazz-react/testing"
  );

  const account = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  function JazzProvider(props: { children: React.ReactNode }) {
    return (
      <JazzTestProvider account={account}>{props.children}</JazzTestProvider>
    );
  }

  return {
    ...(await importOriginal<typeof import("jazz-react")>()),
    JazzProvider,
  };
});

vi.mock("jazz-auth-clerk", async (importOriginal) => {
  const { JazzClerkAuth } = await import("jazz-auth-clerk");

  JazzClerkAuth.loadClerkAuthData = vi.fn().mockResolvedValue(undefined);

  return {
    ...(await importOriginal<typeof import("jazz-auth-clerk")>()),
    JazzClerkAuth,
  };
});

const authSecretStorage = new AuthSecretStorage();
KvStoreContext.getInstance().initialize(new InMemoryKVStore());

describe("JazzProviderWithClerk", () => {
  beforeEach(async () => {
    await authSecretStorage.clear();
    vi.clearAllMocks();
  });

  const setup = (
    children = <div data-testid="test-child">Test Content</div>,
  ) => {
    let callbacks = new Set<(clerk: MinimalClerkClient) => void>();

    const mockClerk: MinimalClerkClient = {
      user: {
        fullName: "Test User",
        username: "test",
        firstName: "Test",
        lastName: "User",
        id: "test",
        primaryEmailAddress: {
          emailAddress: "test@test.com",
        },
        unsafeMetadata: {},
        update: vi.fn(),
      },
      signOut: vi.fn().mockImplementation(() => {
        mockClerk.user = null;
        Array.from(callbacks).map((callback) => callback(mockClerk));
      }),
      addListener: vi.fn((callback) => {
        callbacks.add(callback);

        return () => {
          callbacks.delete(callback);
        };
      }),
    };

    let utils: ReturnType<typeof render>;
    act(() => {
      utils = render(
        <JazzProviderWithClerk
          clerk={mockClerk}
          sync={{
            peer: "wss://test.jazz.tools?key=minimal-auth-clerk-example@garden.co",
          }}
        >
          {children}
        </JazzProviderWithClerk>,
      );
    });

    return {
      ...utils!,
      mockClerk,
      callbacks,
    };
  };

  it("should push the local credentials to clerk", async () => {
    const { mockClerk, callbacks } = setup();

    expect(mockClerk.user?.update).not.toHaveBeenCalled();

    await act(async () => {
      await Promise.all(
        Array.from(callbacks).map((callback) => callback(mockClerk)),
      );
    });

    expect(mockClerk.user?.update).toHaveBeenCalledWith({
      unsafeMetadata: {
        jazzAccountID: expect.any(String),
        jazzAccountSecret: expect.any(String),
        jazzAccountSeed: expect.any(Array),
      },
    });
  });

  it("should load the clerk credentials when the user is authenticated", async () => {
    await act(async () => {
      render(
        <JazzProviderWithClerk
          clerk={{
            addListener: vi.fn(),
            signOut: vi.fn(),
            user: {
              update: vi.fn(),
              unsafeMetadata: {
                jazzAccountID: "test",
                jazzAccountSecret: "test",
                jazzAccountSeed: "test",
              },
              firstName: "Test",
              lastName: "User",
              username: "test",
              fullName: "Test User",
              id: "test",
              primaryEmailAddress: {
                emailAddress: "test@test.com",
              },
            },
          }}
          sync={{ peer: "wss://test.jazz.tools" }}
        >
          <div data-testid="test-child">Test Content</div>
        </JazzProviderWithClerk>,
      );
    });

    expect(JazzClerkAuth.loadClerkAuthData).toHaveBeenCalledWith(
      {
        jazzAccountID: "test",
        jazzAccountSecret: "test",
        jazzAccountSeed: "test",
      },
      authSecretStorage,
    );
  });

  it("should not load the clerk credentials when the user is not authenticated", async () => {
    await act(async () => {
      render(
        <JazzProviderWithClerk
          clerk={{
            addListener: vi.fn(),
            signOut: vi.fn(),
            user: null,
          }}
          sync={{ peer: "wss://test.jazz.tools" }}
        >
          <div data-testid="test-child">Test Content</div>
        </JazzProviderWithClerk>,
      );
    });

    expect(JazzClerkAuth.loadClerkAuthData).not.toHaveBeenCalledWith();
  });
});
