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

  it("reports physical namespaces derived from the logical base and complete auth scope", () => {
    vi.stubGlobal("window", {});

    const appId = "chat-app";
    const env = "test";
    const logicalBase = "shared-device-cache";
    const physicalName = (auth: object) =>
      `${logicalBase}::jazz-browser-v1::${encodeURIComponent(
        JSON.stringify({ version: 1, appId, env, auth }),
      )}`;

    const unregisterExternal = registerWindowJazzStorageClient(
      makeStorageDb({
        appId,
        env,
        driver: { type: "persistent", dbName: logicalBase },
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
        appId,
        env,
        driver: { type: "persistent", dbName: logicalBase },
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
        appId,
        env,
        driver: { type: "persistent", dbName: logicalBase },
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
        appId,
        env,
        driver: { type: "persistent", dbName: logicalBase },
        cookieSession: {
          user_id: "bob@example.com",
          claims: {},
          issuer: "https://issuer.example",
          authMode: "external",
        },
      }),
    );

    expect(window.__jazz?.listLiveStorageNamespaces()).toEqual([
      physicalName({ kind: "anonymous" }),
      physicalName({
        kind: "principal",
        authMode: "external",
        user: '["https://issuer.example","alice@example.com"]',
      }),
      physicalName({
        kind: "principal",
        authMode: "external",
        user: '["https://issuer.example","bob@example.com"]',
      }),
    ]);
    // Planted positive: using only the caller's logical base, or omitting any
    // app/environment/auth field, would collapse distinct live stores here.

    unregisterExternal();
    unregisterAnonymous();
    unregisterSameExternal();
    unregisterOtherExternal();
  });
});
