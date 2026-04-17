import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { useStandaloneContext } from "./contexts/standalone-context.js";

const mockCreateJazzClient = vi.fn();
const mockFetchSchemaHashes = vi.fn();
const mockFetchStoredWasmSchema = vi.fn();

const STORAGE_KEY = "jazz-inspector-standalone-config";

vi.mock("jazz-tools/react", () => ({
  createJazzClient: (...args: unknown[]) => mockCreateJazzClient(...args),
  JazzClientProvider: ({ children }: { children: ReactNode }) => children,
}));

vi.mock("jazz-tools", () => ({
  fetchSchemaHashes: (...args: unknown[]) => mockFetchSchemaHashes(...args),
  fetchStoredWasmSchema: (...args: unknown[]) => mockFetchStoredWasmSchema(...args),
}));

vi.mock("./routes.js", () => ({
  InspectorRoutes: function MockInspectorRoutes() {
    const standaloneContext = useStandaloneContext();

    return (
      <button type="button" onClick={standaloneContext?.onEdit}>
        Open edit
      </button>
    );
  },
}));

describe("App", () => {
  beforeEach(() => {
    localStorage.clear();
    mockCreateJazzClient.mockReset();
    mockFetchSchemaHashes.mockReset();
    mockFetchStoredWasmSchema.mockReset();

    mockCreateJazzClient.mockResolvedValue({
      shutdown: vi.fn(),
    });
    mockFetchStoredWasmSchema.mockResolvedValue({
      schema: {},
    });
    mockFetchSchemaHashes.mockResolvedValue({
      hashes: ["hash-a", "hash-b"],
    });
  });

  afterEach(() => {
    localStorage.clear();
    cleanup();
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
