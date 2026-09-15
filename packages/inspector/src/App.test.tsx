import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
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

vi.mock("jazz-tools/_dev/inspector-client", () => ({
  createInspectorAdminClient: (...args: unknown[]) => createJazzClientMock(...args),
}));

vi.mock("jazz-tools/react", () => ({
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
    vi.restoreAllMocks();
    localStorage.clear();
    cleanup();
  });

  it("migrates a v2 connection without retaining its secret and connects manually when permissions fetch fails", async () => {
    const adminSecret = "admin-secret";
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        version: 2,
        activeConnectionId: "local",
        connections: [
          {
            id: "local",
            name: "Local dev",
            serverUrl: "http://localhost:1625",
            appId: "00000000-0000-0000-0000-000000000099",
            adminSecret,
            env: "dev",
            schemaHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          },
        ],
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

    expect(screen.getByLabelText("Server URL")).toHaveProperty("value", "http://localhost:1625");
    expect(screen.getByLabelText("App ID")).toHaveProperty(
      "value",
      "00000000-0000-0000-0000-000000000099",
    );
    expect(screen.getByLabelText("Admin secret")).toHaveProperty("value", "");
    expect(createJazzClientMock).not.toHaveBeenCalled();
    expect(fetchSchemaHashesMock).not.toHaveBeenCalled();
    expect(fetchStoredWasmSchemaMock).not.toHaveBeenCalled();
    expect(fetchStoredPermissionsMock).not.toHaveBeenCalled();

    const migrated = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}") as {
      version?: number;
      connections?: Array<Record<string, unknown>>;
    };
    expect(migrated.version).toBe(2);
    expect(migrated.connections?.[0]).toEqual(
      expect.objectContaining({
        id: "local",
        name: "Local dev",
        serverUrl: "http://localhost:1625",
        appId: "00000000-0000-0000-0000-000000000099",
        env: "dev",
        schemaHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      }),
    );
    expect(migrated.connections?.[0]).not.toHaveProperty("adminSecret");
    expect(localStorage.getItem(STORAGE_KEY)).not.toContain(adminSecret);

    fireEvent.change(screen.getByLabelText("Admin secret"), {
      target: { value: adminSecret },
    });
    fireEvent.click(screen.getByRole("button", { name: "Connect" }));
    expect(await screen.findByRole("heading", { name: "Select schema" })).not.toBeNull();
    fireEvent.change(screen.getByLabelText("Schema hash"), {
      target: { value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Use schema" }));

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
            env: "dev",
            schemaHash: "hash-a",
          },
          {
            id: "staging",
            name: "Staging",
            serverUrl: "https://staging.example.com",
            appId: "staging-app-id",
            env: "dev",
            schemaHash: "hash-b",
          },
        ],
      }),
    );

    render(<App />);

    expect(screen.getByLabelText("Server URL")).toHaveProperty("value", "http://localhost:19879");
    expect(screen.getByLabelText("App ID")).toHaveProperty("value", "local-app-id");
    expect(screen.getByLabelText("Admin secret")).toHaveProperty("value", "");
    expect(createJazzClientMock).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    expect(await screen.findByRole("heading", { name: "Connections" })).not.toBeNull();
    expect(screen.getByText("Local dev")).not.toBeNull();
    expect(screen.getByText("Staging")).not.toBeNull();
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.queryByRole("button", { name: "Back to inspector" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Using Local dev" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Open Staging" }));

    expect(screen.getByLabelText("Server URL")).toHaveProperty(
      "value",
      "https://staging.example.com",
    );
    expect(screen.getByLabelText("App ID")).toHaveProperty("value", "staging-app-id");
    expect(screen.getByLabelText("Admin secret")).toHaveProperty("value", "");
    expect(createJazzClientMock).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText("Admin secret"), {
      target: { value: "staging-admin-secret" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Connect" }));
    expect(await screen.findByRole("heading", { name: "Select schema" })).not.toBeNull();
    fireEvent.change(screen.getByLabelText("Schema hash"), { target: { value: "hash-b" } });
    fireEvent.click(screen.getByRole("button", { name: "Use schema" }));

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
      connections?: Array<Record<string, unknown>>;
    };
    expect(stored.activeConnectionId).toBe("staging");
    const staging = stored.connections?.find((connection) => connection["name"] === "Staging");
    expect(staging).toEqual(
      expect.objectContaining({
        name: "Staging",
        serverUrl: "https://staging.example.com",
        appId: "staging-app-id",
        env: "dev",
        schemaHash: "hash-b",
      }),
    );
    expect(staging).not.toHaveProperty("adminSecret");
    expect(await screen.findByText("Inspector ready")).not.toBeNull();
  });

  it("keeps the selected connection in memory when storage cannot persist a switch", async () => {
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
            env: "dev",
            schemaHash: "hash-a",
          },
          {
            id: "staging",
            name: "Staging",
            serverUrl: "https://staging.example.com",
            appId: "staging-app-id",
            env: "dev",
            schemaHash: "hash-b",
          },
        ],
      }),
    );

    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(await screen.findByRole("heading", { name: "Connections" })).not.toBeNull();

    vi.spyOn(localStorage, "setItem").mockImplementation(() => {
      throw new Error("storage unavailable");
    });

    fireEvent.click(screen.getByRole("button", { name: "Open Staging" }));

    expect(await screen.findByRole("heading", { name: "Add connection" })).not.toBeNull();
    expect(screen.getByLabelText("Server URL")).toHaveProperty(
      "value",
      "https://staging.example.com",
    );
    expect(screen.getByLabelText("App ID")).toHaveProperty("value", "staging-app-id");
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
            env: "dev",
            schemaHash: "hash-a",
          },
        ],
      }),
    );

    render(<App />);

    expect(screen.getByLabelText("Server URL")).toHaveProperty("value", "http://localhost:19879");
    expect(screen.getByLabelText("App ID")).toHaveProperty("value", "local-app-id");
    expect(screen.getByLabelText("Admin secret")).toHaveProperty("value", "");
    expect(createJazzClientMock).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(await screen.findByRole("heading", { name: "Connections" })).not.toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Add connection" }));

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
      connections?: Array<Record<string, unknown>>;
    };
    expect(stored.connections).toHaveLength(2);
    const preview = stored.connections?.find((connection) => connection["name"] === "Preview");
    expect(preview).toEqual(
      expect.objectContaining({
        name: "Preview",
        serverUrl: "https://preview.example.com",
        appId: "preview-app-id",
        env: "dev",
        schemaHash: "hash-b",
      }),
    );
    expect(preview).not.toHaveProperty("adminSecret");
    expect(localStorage.getItem(STORAGE_KEY)).not.toContain("preview-admin-secret");
    expect(stored.activeConnectionId).toBe(preview?.["id"]);
  });

  it("keeps connections open when an edit submission is cancelled while schema hashes load", async () => {
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
            env: "dev",
            schemaHash: "hash-a",
          },
        ],
      }),
    );

    render(<App />);

    expect(screen.getByLabelText("Server URL")).toHaveProperty("value", "http://localhost:19879");
    expect(screen.getByLabelText("Admin secret")).toHaveProperty("value", "");
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(await screen.findByRole("heading", { name: "Connections" })).not.toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Edit Local dev" }));
    expect(await screen.findByRole("heading", { name: "Edit connection" })).not.toBeNull();
    expect(screen.getByLabelText("Server URL")).toHaveProperty("value", "http://localhost:19879");
    expect(screen.getByLabelText("Admin secret")).toHaveProperty("value", "");
    expect(createJazzClientMock).not.toHaveBeenCalled();

    let resolveSchemaHashes!: (result: { hashes: string[] }) => void;
    const pendingSchemaHashes = new Promise<{ hashes: string[] }>((resolve) => {
      resolveSchemaHashes = resolve;
    });
    fetchSchemaHashesMock.mockImplementationOnce(() => pendingSchemaHashes);

    fireEvent.change(screen.getByLabelText("Admin secret"), {
      target: { value: "edit-admin-secret" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(await screen.findByRole("heading", { name: "Connections" })).not.toBeNull();

    await act(async () => {
      resolveSchemaHashes({ hashes: ["hash-c"] });
      await pendingSchemaHashes;
    });

    expect(screen.getByRole("heading", { name: "Connections" })).not.toBeNull();
  });

  it("prefills the connection form from partial hash params and scrubs legacy storage", async () => {
    const adminSecret = "stored-admin-secret";
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        serverUrl: "http://localhost:19879",
        appId: "stored-app-id",
        adminSecret,
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

    const migrated = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}") as {
      version?: number;
      connections?: Array<Record<string, unknown>>;
    };
    expect(migrated.version).toBe(2);
    expect(migrated.connections?.[0]).toEqual(
      expect.objectContaining({
        serverUrl: "http://localhost:19879",
        appId: "stored-app-id",
        env: "dev",
        schemaHash: "hash-b",
      }),
    );
    expect(migrated.connections?.[0]).not.toHaveProperty("adminSecret");
    expect(localStorage.getItem(STORAGE_KEY)).not.toContain(adminSecret);
  });

  it("scrubs a legacy admin secret from the visible URL hash", async () => {
    const adminSecret = "legacy-fragment-admin-secret";
    window.location.hash = `#serverUrl=https%3A%2F%2Fstaging.v2.aws.cloud.jazz.tools&appId=preview-app-id&adminSecret=${encodeURIComponent(adminSecret)}`;

    render(<App />);

    expect(window.location.hash).toBe("");
    expect(window.location.href).not.toContain(adminSecret);
    expect(await screen.findByRole("heading", { name: "Connect to Jazz server" })).not.toBeNull();
    expect(screen.getByLabelText("Server URL")).toHaveProperty(
      "value",
      "https://staging.v2.aws.cloud.jazz.tools",
    );
    expect(screen.getByLabelText("App ID")).toHaveProperty("value", "preview-app-id");
    expect(screen.getByLabelText("Admin secret")).toHaveProperty("value", "");
    expect(createJazzClientMock).not.toHaveBeenCalled();
  });
  it("a fragment connection preserves the existing saved connection", async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        version: 2,
        activeConnectionId: "saved",
        connections: [
          {
            id: "saved",
            name: "Saved",
            serverUrl: "https://saved.example.com",
            appId: "saved-app",
            adminSecret: "old-secret",
            env: "dev",
            schemaHash: "hash-a",
          },
        ],
      }),
    );
    window.location.hash = "#serverUrl=https%3A%2F%2Fnew.example.com&appId=new-app";
    render(<App />);
    fireEvent.change(screen.getByLabelText("Admin secret"), { target: { value: "new-secret" } });
    fireEvent.click(screen.getByRole("button", { name: "Connect" }));
    await screen.findByRole("heading", { name: "Select schema" });
    fireEvent.change(screen.getByLabelText("Schema hash"), { target: { value: "hash-b" } });
    fireEvent.click(screen.getByRole("button", { name: "Use schema" }));
    await screen.findByText("Inspector ready");
    const store = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
    expect(store.connections).toHaveLength(2);
    expect(store.connections.find((c: { id: string }) => c.id === "saved").appId).toBe("saved-app");
  });

  it("scrubs secrets even when one stored connection is invalid", () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        version: 2,
        activeConnectionId: "saved",
        connections: [
          {
            id: "saved",
            name: "Saved",
            serverUrl: "https://saved.example.com",
            appId: "saved-app",
            adminSecret: "old-secret",
            env: "dev",
            schemaHash: "hash-a",
          },
          {},
        ],
      }),
    );
    render(<App />);
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  });
});
