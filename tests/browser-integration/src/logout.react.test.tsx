import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { commands } from "@vitest/browser/context";
import { AuthSecretStorage, co, z } from "jazz-tools";
import { JazzReactProvider, useAccount, useCoState } from "jazz-tools/react";
import { afterAll, afterEach, describe, expect, test } from "vitest";
import { createAccountContext, startSyncServer } from "./testUtils";

// Define a simple account schema for testing
const TestMap = co.map({ count: co.map({ value: z.number() }) });

const TestAccount = co
  .account({
    profile: co.map({ name: z.string() }),
    root: TestMap,
  })
  .withMigration((account) => {
    if (!account.root) {
      account.root = TestMap.create(
        { count: TestMap.shape.count.create({ value: 0 }) },
        { owner: account },
      );
    }
  });

// React component that uses Jazz hooks for testing logout behavior
function TestLogoutComponent({ onLogout }: { onLogout?: () => void }) {
  const { logOut, me } = useAccount(TestAccount, {
    resolve: {
      profile: true,
    },
  });

  const root = useCoState(TestAccount.shape.root, me?.root?.id, {
    resolve: {
      count: true,
    },
  });

  const handleLogout = () => {
    logOut();
    onLogout?.();
  };

  if (me && root) {
    return (
      <div>
        <p data-testid="user-name">Welcome, {me.profile.name}</p>
        <p data-testid="root-value">{root.count.value}</p>
        <button
          data-testid="increment-button"
          onClick={() => {
            root.count.value++;
          }}
        >
          Increment
        </button>
        <button data-testid="logout-button" onClick={handleLogout}>
          Logout
        </button>
      </div>
    );
  }

  return (
    <div>
      <p data-testid="not-authenticated">Not authenticated</p>
    </div>
  );
}

afterAll(async () => {
  await commands.cleanup();
});

describe("Jazz logout behavior in React apps", () => {
  afterEach(async () => {
    await new AuthSecretStorage().clear();
  });

  test("should update the profile state on logout", async () => {
    const syncServer = await startSyncServer();
    let logoutCallbackCalled = false;

    // Create an authenticated account context
    await createAccountContext({
      defaultProfileName: "John Doe",
      sync: {
        peer: syncServer.url,
      },
      storage: "indexedDB",
      AccountSchema: TestAccount,
    });

    // Render the React component with Jazz provider
    const { unmount } = render(
      <JazzReactProvider
        sync={{ peer: syncServer.url }}
        storage="indexedDB"
        AccountSchema={TestAccount}
        defaultProfileName="Anonymous user"
      >
        <TestLogoutComponent
          onLogout={() => {
            logoutCallbackCalled = true;
          }}
        />
      </JazzReactProvider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("user-name")).toBeInTheDocument();
    });

    expect(screen.getByTestId("user-name")).toHaveTextContent(
      "Welcome, John Doe",
    );

    const logoutButton = screen.getByTestId("logout-button");
    fireEvent.click(logoutButton);

    expect(logoutCallbackCalled).toBe(true);

    await waitFor(() => {
      expect(screen.getByTestId("user-name")).toHaveTextContent(
        "Welcome, Anonymous user",
      );
    });

    unmount();
  });

  test("should reset nested co-state on logout", async () => {
    const syncServer = await startSyncServer();
    let logoutCallbackCalled = false;

    render(
      <JazzReactProvider
        sync={{ peer: syncServer.url }}
        storage="indexedDB"
        AccountSchema={TestAccount}
        defaultProfileName="Anonymous user"
      >
        <TestLogoutComponent
          onLogout={() => {
            logoutCallbackCalled = true;
          }}
        />
      </JazzReactProvider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("root-value")).toBeInTheDocument();
    });

    expect(screen.getByTestId("root-value")).toHaveTextContent("0");

    const incrementButton = screen.getByTestId("increment-button");
    fireEvent.click(incrementButton);

    expect(screen.getByTestId("root-value")).toHaveTextContent("1");

    fireEvent.click(incrementButton);

    expect(screen.getByTestId("root-value")).toHaveTextContent("2");

    const logoutButton = screen.getByTestId("logout-button");
    fireEvent.click(logoutButton);

    expect(logoutCallbackCalled).toBe(true);

    await waitFor(() => {
      expect(screen.getByTestId("root-value")).toHaveTextContent("0");
    });
  });
});
