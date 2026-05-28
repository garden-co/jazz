import { beforeEach, describe, expect, it, vi } from "vitest";

import { readFragmentConfig, type StoredConnections } from "#lib/config/connections.ts";
import {
  resolveActiveStoredInspectorNavigationTarget,
  resolveStoredInspectorNavigationTarget,
} from "#lib/navigation/inspectorNavigation.ts";

const fetchSchemaHashesMock = vi.fn();

vi.mock("jazz-tools", () => ({
  fetchSchemaHashes: (...args: unknown[]) => fetchSchemaHashesMock(...args),
}));

const storedConnections: StoredConnections = {
  version: 2,
  activeConnectionId: "local",
  connections: [
    {
      id: "local",
      name: "Local dev",
      serverUrl: "http://localhost:19879",
      appId: "local-app-id",
      adminSecret: "local-admin-secret",
      env: "dev",
      branch: "main",
      schemaHash: "hash-a",
    },
  ],
};

describe("inspector route navigation", () => {
  beforeEach(() => {
    window.location.hash = "";
    fetchSchemaHashesMock.mockReset();
    fetchSchemaHashesMock.mockResolvedValue({ hashes: ["hash-a", "hash-b"] });
  });

  it("resolves an active stored connection into a shareable inspector route target", async () => {
    await expect(
      resolveActiveStoredInspectorNavigationTarget({ store: storedConnections }),
    ).resolves.toEqual({
      connectionId: "local",
      branch: "main",
      schemaHash: "hash-a",
    });
  });

  it("preserves branch overrides when completing partial connection routes", async () => {
    await expect(
      resolveStoredInspectorNavigationTarget({
        connectionId: "local",
        branchOverride: "feature",
        store: storedConnections,
      }),
    ).resolves.toEqual({
      connectionId: "local",
      branch: "feature",
      schemaHash: "hash-a",
    });
  });

  it("uses an explicit schema hash from the URL without fetching a replacement", async () => {
    await expect(
      resolveStoredInspectorNavigationTarget({
        connectionId: "local",
        branchOverride: "feature",
        schemaHashOverride: "hash-from-url",
        store: storedConnections,
      }),
    ).resolves.toEqual({
      connectionId: "local",
      branch: "feature",
      schemaHash: "hash-from-url",
    });
  });

  it("falls back to the first available schema when the stored schema is no longer published", async () => {
    fetchSchemaHashesMock.mockResolvedValue({ hashes: ["hash-b"] });

    await expect(
      resolveStoredInspectorNavigationTarget({
        connectionId: "local",
        store: storedConnections,
      }),
    ).resolves.toEqual({
      connectionId: "local",
      branch: "main",
      schemaHash: "hash-b",
    });
  });

  it("returns null for unknown connection ids so route loaders can redirect to management", async () => {
    await expect(
      resolveStoredInspectorNavigationTarget({
        connectionId: "missing",
        store: storedConnections,
      }),
    ).resolves.toBeNull();
  });

  it("detects URL hash prefill links before the root route opens the active connection", () => {
    window.location.hash = "#serverUrl=https%3A%2F%2Fsync.example.test&appId=app-id";

    expect(readFragmentConfig()).toMatchObject({
      serverUrl: "https://sync.example.test",
      appId: "app-id",
      adminSecret: "",
      env: "dev",
      branch: "main",
    });
  });
});
