import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { JazzProvider } from "./provider.js";
import type { AuthState } from "../runtime/auth-state.js";
import type { Session } from "../runtime/context.js";

// A client whose db exposes how many onAuthChanged listeners are currently
// attached, so we can assert the orchestrator's session-sync follows the live db.
function makeAuthTrackingClient(session: Session) {
  const listeners = new Set<(s: AuthState) => void>();
  const state: AuthState = { authMode: "external", session };
  return {
    db: {
      subscribeAll: () => () => {},
      applyQueryBundle: () => {},
      getAuthState: () => state,
      onAuthChanged: (cb: (s: AuthState) => void) => {
        listeners.add(cb);
        return () => listeners.delete(cb);
      },
      updateAuthToken: () => {},
    },
    manager: {} as never,
    session,
    shutdown: async () => {},
    authListenerCount: () => listeners.size,
  };
}

const clients = new Map<string, ReturnType<typeof makeAuthTrackingClient>>();
function createTestClient(config: { appId: string }) {
  let client = clients.get(config.appId);
  if (!client) {
    client = makeAuthTrackingClient({ user_id: config.appId, claims: {}, authMode: "external" });
    clients.set(config.appId, client);
  }
  return Promise.resolve(client);
}

afterEach(() => {
  cleanup();
  clients.clear();
  vi.restoreAllMocks();
});

describe("SSR attach — session sync", () => {
  it("re-subscribes the orchestrator session-sync when the live client changes", async () => {
    const tree = (appId: string) => (
      <JazzProvider
        config={{ appId, serverUrl: "https://jazz.example.com" }}
        createJazzClient={createTestClient as never}
        ssr
      >
        <div data-testid="child" />
      </JazzProvider>
    );

    let rerender!: (ui: React.ReactElement) => void;
    await act(async () => {
      const result = render(tree("app-a"));
      rerender = result.rerender;
      await Promise.resolve();
    });

    // Swap the live client by changing the config (a fresh client per appId).
    await act(async () => {
      rerender(tree("app-b"));
      await Promise.resolve();
    });

    // The new live client must have both onAuthChanged subscribers re-attached:
    // useAuthSubscription and the orchestrator's session-sync. A one-shot attach
    // effect that carries the session-sync skips it on the swap, leaving one.
    expect(clients.get("app-b")!.authListenerCount()).toBe(2);
  });
});
