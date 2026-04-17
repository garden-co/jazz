import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";

const STORAGE_KEY = "jazz-inspector-standalone-config";

const createJazzClientMock = vi.fn();
const fetchSchemaHashesMock = vi.fn();
const fetchStoredPermissionsMock = vi.fn();
const fetchStoredWasmSchemaMock = vi.fn();
const devtoolsProviderMock = vi.fn();

vi.mock("jazz-tools/react", () => ({
  createJazzClient: (...args: unknown[]) => createJazzClientMock(...args),
  JazzClientProvider: ({ children }: { children: React.ReactNode }) => children,
}));

vi.mock("jazz-tools", () => ({
  fetchSchemaHashes: (...args: unknown[]) => fetchSchemaHashesMock(...args),
  fetchStoredPermissions: (...args: unknown[]) => fetchStoredPermissionsMock(...args),
  fetchStoredWasmSchema: (...args: unknown[]) => fetchStoredWasmSchemaMock(...args),
}));

vi.mock("./contexts/standalone-context.js", () => ({
  StandaloneProvider: ({ children }: { children: React.ReactNode }) => children,
}));

vi.mock("./contexts/devtools-context.js", () => ({
  DevtoolsProvider: ({
    children,
    ...props
  }: {
    children: React.ReactNode;
    storedPermissions?: unknown;
    runtime: string;
    wasmSchema: unknown;
  }) => {
    devtoolsProviderMock(props);
    return children;
  },
}));

vi.mock("./routes.js", () => ({
  InspectorRoutes: () => <div>Inspector ready</div>,
}));

describe("App", () => {
  beforeEach(() => {
    localStorage.clear();
    window.location.hash = "";
    createJazzClientMock.mockReset();
    fetchSchemaHashesMock.mockReset();
    fetchStoredPermissionsMock.mockReset();
    fetchStoredWasmSchemaMock.mockReset();
    devtoolsProviderMock.mockReset();
  });

  afterEach(() => {
    cleanup();
  });

  it("loads the standalone inspector when permissions fetch fails", async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        serverUrl: "http://localhost:1625",
        appId: "00000000-0000-0000-0000-000000000099",
        adminSecret: "admin-secret",
        env: "dev",
        branch: "main",
        schemaHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      }),
    );

    createJazzClientMock.mockResolvedValue({
      shutdown: vi.fn().mockResolvedValue(undefined),
    });
    fetchStoredWasmSchemaMock.mockResolvedValue({
      schema: {
        todos: {
          columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
        },
      },
      publishedAt: 123,
    });
    fetchSchemaHashesMock.mockResolvedValue({
      hashes: ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
    });
    fetchStoredPermissionsMock.mockRejectedValue(new Error("Permissions fetch failed: 404"));

    render(<App />);

    expect(await screen.findByText("Inspector ready")).not.toBeNull();
    expect(screen.queryByRole("heading", { name: "Connection error" })).toBeNull();
    await waitFor(() => {
      expect(devtoolsProviderMock).toHaveBeenCalledWith(
        expect.objectContaining({
          runtime: "standalone",
          storedPermissions: null,
        }),
      );
    });
  });
});
