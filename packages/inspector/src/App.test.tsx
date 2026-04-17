import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { useStandaloneContext } from "./contexts/standalone-context.js";

const STORAGE_KEY = "jazz-inspector-standalone-config";

const createJazzClientMock = vi.fn();
const fetchSchemaHashesMock = vi.fn();
const fetchStoredPermissionsMock = vi.fn();
const fetchStoredWasmSchemaMock = vi.fn();
const devtoolsProviderMock = vi.fn();

vi.mock("jazz-tools/react", () => ({
  createJazzClient: (...args: unknown[]) => createJazzClientMock(...args),
  JazzClientProvider: ({ children }: { children: ReactNode }) => children,
}));

vi.mock("jazz-tools", () => ({
  fetchSchemaHashes: (...args: unknown[]) => fetchSchemaHashesMock(...args),
  fetchStoredPermissions: (...args: unknown[]) => fetchStoredPermissionsMock(...args),
  fetchStoredWasmSchema: (...args: unknown[]) => fetchStoredWasmSchemaMock(...args),
}));

vi.mock("./contexts/devtools-context.js", () => ({
  DevtoolsProvider: ({
    children,
    ...props
  }: {
    children: ReactNode;
    storedPermissions?: unknown;
    runtime: string;
    wasmSchema: unknown;
  }) => {
    devtoolsProviderMock(props);
    return children;
  },
}));

vi.mock("./routes.js", () => ({
  InspectorRoutes: function MockInspectorRoutes() {
    const standaloneContext = useStandaloneContext();

    return (
      <>
        <div>Inspector ready</div>
        <button type="button" onClick={standaloneContext?.onEdit}>
          Open edit
        </button>
      </>
    );
  },
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

    createJazzClientMock.mockResolvedValue({
      shutdown: vi.fn(),
    });
    fetchStoredWasmSchemaMock.mockResolvedValue({
      schema: {},
    });
    fetchSchemaHashesMock.mockResolvedValue({
      hashes: ["hash-a", "hash-b"],
    });
    fetchStoredPermissionsMock.mockResolvedValue(null);
  });

  afterEach(() => {
    localStorage.clear();
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

  it("lets you edit a stored connection and reset from the edit page", async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        serverUrl: "http://localhost:19879",
        appId: "test-app-id",
        adminSecret: "admin-secret",
        env: "dev",
        branch: "main",
        schemaHash: "hash-b",
        serverPathPrefix: "/apps/test-app-id",
      }),
    );

    render(<App />);

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Open edit" })).not.toBeNull();
    });

    fireEvent.click(screen.getByRole("button", { name: "Open edit" }));

    expect(await screen.findByRole("heading", { name: "Edit connection" })).not.toBeNull();
    expect(screen.getByLabelText("Server URL")).toHaveProperty("value", "http://localhost:19879");
    expect(screen.getByLabelText("App ID")).toHaveProperty("value", "test-app-id");
    expect(screen.getByLabelText("Admin secret")).toHaveProperty("value", "admin-secret");
    expect(screen.getByLabelText(/Path prefix/i)).toHaveProperty("value", "/apps/test-app-id");

    fireEvent.click(screen.getByRole("button", { name: "Reset connection" }));

    expect(await screen.findByRole("heading", { name: "Connect to Jazz server" })).not.toBeNull();
    expect(screen.getByLabelText("Server URL")).toHaveProperty("value", "");
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  });
});
