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
        <button type="button" onClick={standaloneContext?.onManageConnections}>
          Open connections
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

  it("lets you manage and switch between named stored connections", async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
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
          {
            id: "staging",
            name: "Staging",
            serverUrl: "https://staging.example.com",
            appId: "staging-app-id",
            adminSecret: "staging-admin-secret",
            env: "dev",
            branch: "main",
            schemaHash: "hash-b",
          },
        ],
      }),
    );

    render(<App />);

    await waitFor(() => {
      expect(createJazzClientMock).toHaveBeenLastCalledWith(
        expect.objectContaining({
          appId: "local-app-id",
          serverUrl: "http://localhost:19879",
          adminSecret: "local-admin-secret",
        }),
      );
      expect(screen.getByRole("button", { name: "Open connections" })).not.toBeNull();
    });

    fireEvent.click(screen.getByRole("button", { name: "Open connections" }));

    expect(await screen.findByRole("heading", { name: "Connections" })).not.toBeNull();
    expect(screen.getByText("Local dev")).not.toBeNull();
    expect(screen.getByText("Staging")).not.toBeNull();
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.queryByRole("button", { name: "Back to inspector" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Using Local dev" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Open Staging" }));

    await waitFor(() => {
      expect(createJazzClientMock).toHaveBeenLastCalledWith(
        expect.objectContaining({
          appId: "staging-app-id",
          serverUrl: "https://staging.example.com",
          adminSecret: "staging-admin-secret",
        }),
      );
    });

    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}") as {
      activeConnectionId?: string;
    };
    expect(stored.activeConnectionId).toBe("staging");
    expect(await screen.findByText("Inspector ready")).not.toBeNull();
  });

  it("adds a named connection from the connection manager", async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
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
      }),
    );

    render(<App />);

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Open connections" })).not.toBeNull();
    });

    fireEvent.click(screen.getByRole("button", { name: "Open connections" }));
    fireEvent.click(await screen.findByRole("button", { name: "Add connection" }));

    expect(await screen.findByRole("heading", { name: "Add connection" })).not.toBeNull();
    expect(screen.getByLabelText("Server URL")).toHaveProperty(
      "value",
      "https://v2.sync.jazz.tools/",
    );
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "Preview" } });
    fireEvent.change(screen.getByLabelText("Server URL"), {
      target: { value: "https://preview.example.com" },
    });
    fireEvent.change(screen.getByLabelText("App ID"), { target: { value: "preview-app-id" } });
    fireEvent.change(screen.getByLabelText("Admin secret"), {
      target: { value: "preview-admin-secret" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Connect" }));

    expect(await screen.findByRole("heading", { name: "Select schema" })).not.toBeNull();
    fireEvent.change(screen.getByLabelText("Schema hash"), { target: { value: "hash-b" } });
    fireEvent.click(screen.getByRole("button", { name: "Use schema" }));

    await waitFor(() => {
      expect(createJazzClientMock).toHaveBeenLastCalledWith(
        expect.objectContaining({
          appId: "preview-app-id",
          serverUrl: "https://preview.example.com",
          adminSecret: "preview-admin-secret",
        }),
      );
    });

    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}") as {
      activeConnectionId?: string;
      connections?: Array<{ id: string; name: string }>;
    };
    expect(stored.connections).toHaveLength(2);
    const preview = stored.connections?.find((connection) => connection.name === "Preview");
    expect(preview).toBeDefined();
    expect(stored.activeConnectionId).toBe(preview?.id);
  });

  it("prefills the connection form from partial hash params", async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        serverUrl: "http://localhost:19879",
        appId: "stored-app-id",
        adminSecret: "stored-admin-secret",
        env: "dev",
        branch: "main",
        schemaHash: "hash-b",
      }),
    );
    window.location.hash =
      "#serverUrl=https%3A%2F%2Fstaging.v2.aws.cloud.jazz.tools&appId=019d9bc9-646b-7560-b26d-b775a7d061d3";

    render(<App />);

    expect(await screen.findByRole("heading", { name: "Add connection" })).not.toBeNull();
    expect(screen.getByLabelText("Server URL")).toHaveProperty(
      "value",
      "https://staging.v2.aws.cloud.jazz.tools",
    );
    expect(screen.getByLabelText("App ID")).toHaveProperty(
      "value",
      "019d9bc9-646b-7560-b26d-b775a7d061d3",
    );
    expect(screen.getByLabelText("Admin secret")).toHaveProperty("value", "");
  });
});
