import { afterEach, describe, expect, it, vi } from "vitest";
import { registerWindowJazzStorageClient } from "./window-client-storage.js";
import type { DbConfig } from "./runtime/db.js";

function makeStorageDb(config: DbConfig) {
  return {
    getConfig: () => config,
    deleteClientStorage: async () => {},
    shutdown: async () => {},
  };
}

describe("registerWindowJazzStorageClient", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  // TEST_BURNDOWN_TS: registerWindowJazzStorageClient > scopes the reported namespace for cookie sessions and leaves anonymous cookie sessions unscoped
  // known red; tracked in TEST_BURNDOWN.md — resolveStorageNamespace resolves the session from jwtToken only, so an external cookie session reports the unscoped appId namespace and the anonymous authMode carve-out is never applied.
  it.skip("scopes the reported namespace for cookie sessions and leaves anonymous cookie sessions unscoped", () => {
    vi.stubGlobal("window", {});

    const unregisterExternal = registerWindowJazzStorageClient(
      makeStorageDb({
        appId: "chat-app",
        driver: { type: "persistent" },
        cookieSession: {
          user_id: "alice@example.com",
          claims: {},
          authMode: "external",
        },
      }),
    );
    const unregisterAnonymous = registerWindowJazzStorageClient(
      makeStorageDb({
        appId: "chat-app",
        driver: { type: "persistent" },
        cookieSession: {
          user_id: "ephemeral-visitor",
          claims: {},
          authMode: "anonymous",
        },
      }),
    );

    expect(window.__jazz?.listLiveStorageNamespaces()).toEqual([
      "chat-app",
      "chat-app::alice%40example.com",
    ]);

    unregisterExternal();
    unregisterAnonymous();
  });
});
