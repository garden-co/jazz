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

  it("scopes the reported namespace for cookie sessions and leaves anonymous cookie sessions unscoped", () => {
    vi.stubGlobal("window", {});

    const unregisterExternal = registerWindowJazzStorageClient(
      makeStorageDb({
        appId: "chat-app",
        driver: { type: "persistent" },
        cookieSession: {
          user_id: "alice@example.com",
          claims: {},
          issuer: "https://issuer.example",
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
          issuer: "urn:jazz:anonymous",
          authMode: "anonymous",
        },
      }),
    );
    const unregisterSameExternal = registerWindowJazzStorageClient(
      makeStorageDb({
        appId: "chat-app",
        driver: { type: "persistent" },
        cookieSession: {
          user_id: "alice@example.com",
          claims: {},
          issuer: "https://issuer.example",
          authMode: "external",
        },
      }),
    );
    const unregisterOtherExternal = registerWindowJazzStorageClient(
      makeStorageDb({
        appId: "chat-app",
        driver: { type: "persistent" },
        cookieSession: {
          user_id: "bob@example.com",
          claims: {},
          issuer: "https://issuer.example",
          authMode: "external",
        },
      }),
    );

    expect(window.__jazz?.listLiveStorageNamespaces()).toEqual([
      "chat-app",
      "chat-app::%5B%22https%3A%2F%2Fissuer.example%22%2C%22alice%40example.com%22%5D",
      "chat-app::%5B%22https%3A%2F%2Fissuer.example%22%2C%22bob%40example.com%22%5D",
    ]);

    unregisterExternal();
    unregisterAnonymous();
    unregisterSameExternal();
    unregisterOtherExternal();
  });
});
